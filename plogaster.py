#!/usr/bin/env python
#
import errno
import feedparser
import hashlib
import itertools
import logging
import optparse
import os
import re
import string
import sys
import threading
import time
import urllib
import xml.dom.minidom

logging.basicConfig(level=logging.WARNING,
                    format='[%(levelname)s:%(threadName)s] %(message)s')

tfmt = '%Y-%m-%dT%H:%M:%S'

cfg_base = os.path.join(os.path.expanduser('~'), '.plogaster')
cfg_file = cfg_base + '.xml' # default, could be changed by --config option
cfg_temp = cfg_base + '.tmp'
cfg_lock = threading.RLock()

class ExceptionKillThread(StandardError):
    pass

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

def hashfile(filename):
    with open(filename, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()

def get_mp3_link(links):
    for l in links:
        url = l.get('href')
        type = l.get('type')
        if type == 'audio/mpeg':
            return url
    return None

def cfg_get_node(node, tag):
    return node.getElementsByTagName(tag)[0].firstChild

def cfg_get_data(node, tag):
    return cfg_get_node(node, tag).data

def safe_name(name):
    ok_char = string.letters + string.digits
    return ''.join(c if c in ok_char else '_' for c in name)

def isactive(t):
    t.join(0.5)
    return t.isAlive()

def wait_for_threads(threads, event_quit):
    try:
        while len(threads) > 0:
            threads[:] = itertools.ifilter(isactive, threads)
    except KeyboardInterrupt:
        logging.info('aborting')
        event_quit.set()

def get_feed_links_to_download(feed, update_after, dirs, default_dir):
    link_info = []
    for entry in feed.entries:
        updated_parsed = time.strptime(time.strftime(tfmt,
                            entry.updated_parsed), tfmt)
        if updated_parsed > update_after:
            outdir = None
            for d in dirs:
                if re.search(d.getAttribute('match'), entry.title):
                    outdir = d.firstChild.data
                    break
            if not outdir:
                outdir = default_dir
            mp3_link = get_mp3_link(entry.links)
            if mp3_link:
                link_info.append((updated_parsed,
                                  entry.title,
                                  get_mp3_link(entry.links),
                                  outdir))
        else:
            break
    link_info.reverse()
    return link_info

def get_all_links_to_download(cfg, event_quit):
    link_info = []
    default_dir = cfg_get_data(cfg, 'default_dir')

    event_done = threading.Event()
    bytes_received = {}

    def progress_thread():
        while True:
            msg = '\rDownloading feed XML: %d KB' % (
                    sum(bytes_received.values()) / 1024)
            sys.stdout.write(msg)
            sys.stdout.flush()
            if event_done.wait(0.25):
                sys.stdout.write(msg + '\n')
                sys.stdout.flush()
                break

    def worker(cast, nick):

        def url_progress(block_count, block_size, total_size):
            if event_quit.isSet():
                # kill the thread
                raise ExceptionKillThread()
            bytes_received[nick] = block_count * block_size

        with cfg_lock:
            url = cfg_get_data(cast, 'url')
            name = cfg_get_data(cast, 'name')

        try:
            logging.debug('%-8s getting links' % nick)

            # get feed XML and parse it
            feed_xml = nick + '.xml'
            urllib.urlretrieve(url, feed_xml, url_progress)
            feed = feedparser.parse(feed_xml)

            with cfg_lock:
                # determine time of last successful download from feed
                update_after_node = cfg_get_node(cast, 'update_after')
                update_after = time.strptime(update_after_node.data, tfmt)

                # get new links in feed
                links = get_feed_links_to_download(feed, update_after,
                        cast.getElementsByTagName('dir'), default_dir)

                if len(links) > 0:
                    link_info.append((links, name, nick,
                                      update_after_node, cast))

            logging.info('%-8s got %d links' % (nick, len(links)))

        except ExceptionKillThread:
            logging.info('aborted %s' % feed_xml)

        os.unlink(feed_xml)

    threads = []
    for cast in cfg.getElementsByTagName('cast'):
        with cfg_lock:
            nick = (cfg_get_data(cast, 'nick') + 8 * '_')[:8]
        t = threading.Thread(name=nick, target=worker, args=(cast, nick))
        threads.append(t)
        t.start()
    p = threading.Thread(name='progress', target=progress_thread)
    p.start()

    wait_for_threads(threads, event_quit)
    event_done.set()

    return link_info

def next_counter(cfg):
    with cfg_lock:
        node = cfg_get_node(cfg, 'counter')
        value = int(node.data)
        node.data = str(value+1)
        with open(cfg_temp, 'w') as f:
            f.write(cfg.toxml())
        os.rename(cfg_temp, cfg_file)
    return value

def download_links(cfg, link_info, event_quit):

    base_dir = cfg_get_data(cfg, 'base_dir')

    # default maximum number of checksums to keep
    max_history = 25
    max_history_nodes = cfg.getElementsByTagName('max_history')
    if max_history_nodes:
        max_history_text = max_history_nodes[0].firstChild
        if max_history_text:
            max_history = int(max_history_text.data)

    event_done = threading.Event()
    console_lock = threading.RLock()
    progress_data = {}

    def progress_thread():
        while True:
            # single-line output
            msg = ' '.join('%d/%d:%s' % (d['link'], d['links'],
                '?' if d['total'] in (0,-1) else
                '%02d' % min(99, (100 * d['data'] / d['total'])))
                for n, d in progress_data.iteritems() if not d['done'])
            with console_lock:
                sys.stdout.write((msg + 78 * ' ')[:78] + '\r')
                sys.stdout.flush()
            if event_done.wait(0.25):
                with console_lock:
                    print
                break

    def worker(cast):
        links, name, nick, update_after_node, cast_node = cast
        dups = 0

        try:
            for i, link in enumerate(links):

                def url_progress(block_count, block_size, total_size):
                    if event_quit.isSet():
                        # kill the thread
                        raise ExceptionKillThread()
                    progress_data[nick] = {
                            'data'  : block_count * block_size,
                            'total' : total_size,
                            'link'  : i + 1,
                            'links' : len(links),
                            'done'  : False,
                            }

                # get feed timestamp for this link
                update_after = time.strftime(tfmt, link[0])

                # create a unique filename for the file to download
                safe_title = '%05d_%s_%s.mp3' % (next_counter(cfg), nick,
                                                 safe_name(link[1]))
                outdir = os.path.join(base_dir, link[3])
                outfile = os.path.join(outdir, safe_title)
                mkdir_p(outdir)

                # download the file
                url = link[2]
                logging.debug('GET %s' % url)
                urllib.urlretrieve(url, outfile, url_progress)

                # handle zero-length files
                if os.path.getsize(outfile) == 0:
                    # try again on zero-size file
                    logging.warning('0 KB file, retrying download')
                    urllib.urlretrieve(url, outfile, url_progress)
                    if os.path.getsize(outfile) == 0:
                        # after second try, bail out
                        logging.error('download failed')
                        i -= 1
                        break

                # set filesystem timestamp on downloaded file
                timestamp = time.mktime(link[0])
                os.utime(outfile, (timestamp, timestamp))

                # determine checksum of downloaded file
                file_checksum = hashfile(outfile)

                # update config data for this feed
                with cfg_lock:

                    # set time of last successful download
                    update_after_node.data = update_after

                    # get checksums of previous downloads
                    history_nodes = cast_node.getElementsByTagName('history')
                    if history_nodes:
                        history_node = history_nodes[0]
                    else:
                        history_node = cast_node.appendChild(
                                cfg.createElement('history'))
                    checksums = history_node.getElementsByTagName('checksum')

                    # remove the downloaded file if it's a duplicate, otherwise
                    # record the checksum of the new file
                    for checksum in checksums:
                        if checksum.firstChild.data == file_checksum:
                            # duplicate
                            logging.info('DUP %s %s' % (file_checksum, link[1]))
                            os.unlink(outfile)
                            dups += 1
                            break
                    else:
                        # new file
                        logging.info('GOT %s %s' % (file_checksum, link[1]))

                        # add its checksum to the history
                        checksum_node = history_node.appendChild(
                                cfg.createElement('checksum'))
                        checksum_node.appendChild(
                                cfg.createTextNode(file_checksum))

                        # purge older checksums if history list is too long
                        while len(history_node.childNodes) > max_history:
                            logging.info('PURGE %s' % cfg_get_data(
                                history_node, 'checksum'))
                            history_node.removeChild(
                                    history_node.firstChild).unlink()

                    # Write config to temp file, then commit change to original
                    with open(cfg_temp, 'w') as f:
                        f.write(cfg.toxml())
                    os.rename(cfg_temp, cfg_file)

            # update progress meter
            progress_data[nick]['done'] = True
            msg = '%s got %d' % (nick, i + 1 - dups)
            if dups:
                msg += ', %d dups' % dups
            with console_lock:
                print (msg + 78 * ' ')[:78]

        except ExceptionKillThread:
            os.unlink(outfile)
            logging.info('aborted %s' % outfile)

    # launch a worker thread for each feed that has updates and another thread
    # to monitor progress
    threads = []
    for cast in link_info:
        t = threading.Thread(name=cast[2], target=worker, args=(cast,))
        threads.append(t)
        t.start()
    p = threading.Thread(name='progress', target=progress_thread)
    p.start()

    wait_for_threads(threads, event_quit)
    event_done.set()

def main():

    # process command line
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config")
    options, args = parser.parse_args()

    # parse config XML
    global cfg_file
    if options.config:
        cfg_file = options.config
    try:
        cfg = xml.dom.minidom.parse(cfg_file)
    except IOError, e:
        parser.error('Could not open configuration file: %s' % cfg_file)

    event_quit = threading.Event()
    link_info = get_all_links_to_download(cfg, event_quit)
    if not event_quit.isSet():
        download_links(cfg, link_info, event_quit)
    #print cfg.toxml()

if __name__ == '__main__':
    main()
