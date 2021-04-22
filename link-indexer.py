#!/usr/bin/env python3

# Copyright (C) 2020-2021 Bibliotheca Alexandrina <https://www.bibalex.org/>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.

# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import re
import argparse
import requests
import traceback
from retry.api import retry
from datetime import datetime

from iformats import wat
from iformats import csv

path = ''
record_count = 1
node_id = 1
edge_id = 1
body = []

batch = 0

# cumulative total counts
files = 0
records = 0
nodes = 0

# accept multiple WAT files as command-line arguments
my_parser = argparse.ArgumentParser()
my_parser.add_argument('files', metavar='files', nargs='+', help='list of ARC, WARC, WAT, or CSV files')
my_parser.add_argument('--host', action='store', default='localhost')
my_parser.add_argument('--port', action='store', type=int, default=80)
my_parser.add_argument('--batch_size', action='store', type=int, default=100)
my_parser.add_argument('--retries', action='store', type=int, default=3)
my_parser.add_argument('--timeout_network', action='store', type=int, default=90)
my_parser.add_argument('--timeout_process', action='store', type=int, default=60)
my_parser.add_argument('--max_identifier_length', action='store', type=int, default=2000)
my_parser.add_argument('--dt14', action='store_true')
my_parser.add_argument('--ignore_errors', action='store_true')
my_parser.add_argument('--print_only', action='store_true')
my_parser.add_argument('--keep', action='store_true')

args = my_parser.parse_args()


@retry(tries=args.retries)
def update_graph(url, body):
    if args.print_only:
        print(body, end='')
    else:
        response = requests.post(url, data=body, timeout=args.timeout_network)

        print("%s %s: files=%d batch=%d records=%d nodes=%d status=%d" % (
              datetime.now().strftime("%b %d %H:%M:%S"),
              os.path.basename(path),
              files, batch, records, nodes, response.status_code), file=sys.stderr, flush=True)

        if not response.ok:
            print("ERROR: %s" % response.content, file=sys.stderr, flush=True)


def check_batch_size():
    if record_count > args.batch_size:
        globals()['nodes'] += node_id
        globals()['batch'] += 1
        update()
        reset()
        return True

    return False


def update():
    global request_body
    request_body = ''.join(globals()['body'])

    try:
        update_graph("http://%s:%s/?operation=updateGraph" % (args.host, args.port), request_body)
    except Exception as exc:
        traceback.print_exc()

        if not args.ignore_errors:
            sys.exit(1)  # TODO: test this

    globals()['body'] = []


def reset():
    globals()['node_id'] = globals()['edge_id'] = globals()['record_count'] = 1
    globals()['batch'] += 1


# callback function
def process_record(record_json, node_id, edge_id):
    globals()['node_id'] = node_id
    globals()['edge_id'] = edge_id
    globals()['record_count'] += 1
    globals()['records'] += 1
    globals()['body'] += record_json

    if check_batch_size():
        return False

    return True


for i in range(0, len(args.files)):
    files += 1

    for ifmt in (wat, csv):
        path = ifmt.check_path(str(args.files[i]), args.timeout_process)

        if path:
            ifmt.parse_record(path, node_id, edge_id, process_record, args.max_identifier_length, args.dt14)

            # file was generated on the fly
            if path != str(args.files[i]):
                if not args.keep:
                    os.remove(path)

            break

    update()
