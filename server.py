#!/usr/bin/env python3
from influxdb_udp_inserter.server_async import Server

import argparse
import glob
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', nargs=1, metavar='URL', help='Influxdb URL for Line Protocol',
                        required=True)
    parser.add_argument('--formats', nargs=1, action='append', metavar='PATTERN',
                        help='Glob pattern to look for message formats')
    parser.add_argument('--port', nargs=1, metavar='PORT', help='List on UDP port number', default=9999)
    args = parser.parse_args()

    s = Server(args.url[0], local_addr=('0.0.0.0', args.port))

    for pattern in args.formats:
        for filepath in glob.glob(pattern[0]):
            logging.info("Add message format from file: %s", filepath)
            s.read_message_format_file(filepath)

    s.run()


if __name__ == '__main__':
    main()
