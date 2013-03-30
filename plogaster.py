#!/usr/bin/env python
#
import errno
import feedparser
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

def get_mp3_link(links):
    for l in links:
        url = l.get('href')
        type = l.get('type')
        if type == 'audio/mpeg':
            return url
    return None

def cfg_get_node(node, tag):
    return node.getElementsByTagName(tag)[0].childNodes[0]

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
                    outdir = d.childNodes[0].data
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

            feed_xml = nick + '.xml'
            urllib.urlretrieve(url, feed_xml, url_progress)
            feed = feedparser.parse(feed_xml)

            with cfg_lock:
                update_after_node = cfg_get_node(cast, 'update_after')
                update_after = time.strptime(update_after_node.data, tfmt)

                links = get_feed_links_to_download(feed, update_after,
                        cast.getElementsByTagName('dir'), default_dir)

                if len(links) > 0:
                    link_info.append((links, name, nick, update_after_node))

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

    event_done = threading.Event()
    console_lock = threading.RLock()
    progress_data = {}

    def progress_thread():
        while True:
            # multi-line output
            # for nick, data in progress_data.iteritems():
            #     print '%s %d/%d' % (nick, data['link'], data['links']),
            #     if data['done']:
            #         print 'DONE'
            #     else:
            #         if data['total'] != -1:
            #             print '%3d%%' % (100 * data['data'] / data['total']),
            #         print '%d KB' % (data['data'] / 1024)

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
        links, name, nick, update_after_node = cast

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

                update_after = time.strftime(tfmt, link[0])
                safe_title = '%05d_%s_%s.mp3' % (next_counter(cfg), nick,
                                                 safe_name(link[1]))
                url = link[2]
                outdir = os.path.join(base_dir, link[3])
                outfile = os.path.join(outdir, safe_title)
                mkdir_p(outdir)
                logging.debug('GET %s' % url)
                urllib.urlretrieve(url, outfile, url_progress)
                if os.path.getsize(outfile) == 0:
                    # try again on zero-size file
                    logging.warning('0 KB file, retrying download')
                    urllib.urlretrieve(url, outfile, url_progress)
                    if os.path.getsize(outfile) == 0:
                        # after second try, bail out
                        logging.error('download failed')
                        i -= 1
                        break
                timestamp = time.mktime(link[0])
                os.utime(outfile, (timestamp, timestamp))
                with cfg_lock:
                    update_after_node.data = update_after
                    with open(cfg_temp, 'w') as f:
                        f.write(cfg.toxml())
                    os.rename(cfg_temp, cfg_file)
                logging.info('GOT %s' % link[1])
            progress_data[nick]['done'] = True
            msg = '%s got %d' % (nick, i + 1)
            with console_lock:
                print (msg + 78 * ' ')[:78]

        except ExceptionKillThread:
            os.unlink(outfile)
            logging.info('aborted %s' % outfile)

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
