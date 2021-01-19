#!/usr/bin/env python3

# Copyright (C) 2020 Bibliotheca Alexandrina <https://www.bibalex.org/>

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
import re
import sys
import json
import urlcanon
import argparse
import requests
import dateutil.parser as dp
import traceback
from retry.api import retry
from datetime import datetime
from urllib.parse import urljoin
from warcio.archiveiterator import ArchiveIterator

record_count = 1
node_id = 1
edge_id = 1
body = []

batch = 0

# Cumulative total counts
wats = 0
records = 0
nodes = 0

# accept multiple WAT files as command-line arguments
my_parser = argparse.ArgumentParser()
my_parser.add_argument('wats', metavar='wats', nargs='+', help='list of WAT files')
my_parser.add_argument('--host', action='store', default='localhost')
my_parser.add_argument('--port', action='store', type=int, default=8080)
my_parser.add_argument('--batch_size', action='store', type=int, default=1000)
my_parser.add_argument('--retries', action='store', type=int, default=3)
my_parser.add_argument('--timeout', action='store', type=int, default=90)
my_parser.add_argument('--max_url_length', action='store', type=int, default=2000)
my_parser.add_argument('--dt14', action='store_true')
my_parser.add_argument('--ignore_errors', action='store_true')

args = my_parser.parse_args()


@retry(tries=args.retries)
def update_graph(url, body):
    response = requests.post(url, data=body, timeout=args.timeout)

    print("%s %s: wats=%d batch=%d records=%d nodes=%d status=%d" % (
        datetime.now().strftime("%b %d %H:%M:%S"),
        os.path.basename(wat_file),
        wats, batch, records, nodes, response.status_code), file=sys.stderr, flush=True)

    if not response.ok:
        print("ERROR: %s" % response.content, file=sys.stderr, flush=True)


for i in range(0, len(args.wats)):
    wat_file = str(args.wats[i])
    wats += 1

    with open(wat_file, 'rb') as stream:
        # loop on every record in WAT
        for record in ArchiveIterator(stream):
            if record_count > args.batch_size:
                node_id = edge_id = record_count = 1
                batch += 1
                request_body = ''.join(body)

                try:
                    update_graph("http://%s:%s/?operation=updateGraph" % (args.host, args.port), request_body)
                except Exception as exc:
                    traceback.print_exc()

                    if args.ignore_errors:
                        continue
                    else:
                        sys.exit(1)

                body = []

            if record.rec_type != 'metadata':
                continue

            warc_target_uri = urlcanon.parse_url(record.rec_headers.get_header('WARC-Target-URI'))
            urlcanon.whatwg(warc_target_uri)  # canonicalization

            # select only members whose WARC-Target-URI begins with "https?://"
            if not re.search("^https?://", str(warc_target_uri)) or len(str(warc_target_uri)) > args.max_url_length:
                continue

            datetime = record.rec_headers.get_header('WARC-Date')

            if args.dt14:
                datetime = dp.parse(datetime).strftime('%Y%m%d%H%M%S')

            # construct node with timestamp (VersionNode)
            version_node = {
                "an": {
                    node_id:
                    {
                        "url": str(warc_target_uri.ssurt(), encoding='utf-8'),
                        "timestamp": datetime,
                        "TYPE": "VersionNode"
                    }
                }
            }

            # \r is required as separator in the Gephi streaming format

            # https://github.com/gephi/gephi/wiki/GraphStreaming#Supported_formats

            body.append(json.dumps(version_node))
            body.append('\r\n')

            source_id = node_id
            node_id += 1
            record_count += 1
            records += 1

            content = json.loads(record.raw_stream.read().decode('utf-8'))

            try:
                links = content["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
            except:
                links = ''

            # loop on links if not empty and get all urls
            if links == '':
                continue

            for link in links:
                # this is for empty outlink elements, maybe a bug in webarchive-commons used to generate WAT
                try:
                    # convert relative outlink to absolute one
                    url = urljoin(str(warc_target_uri), link["url"])
                    urlcanon.whatwg(url)  # canonicalization

                    # match only urls that begin with "https?://"
                    if not re.search("^https?://", url) or len(str(url)) > args.max_url_length:
                        continue

                    # construct node and edge
                    node = {
                        "an": {
                            node_id:
                            {
                                "url": str(urlcanon.parse_url(url).ssurt(), encoding="utf-8"),
                                "TYPE": "Node"
                            }
                        }
                    }

                    edge = {
                        "ae": {
                            edge_id:
                            {
                                "directed": "true",
                                "source": str(source_id),
                                "target": str(node_id)
                            }
                        }
                    }

                    body.append(json.dumps(node))
                    body.append('\r\n')

                    body.append(json.dumps(edge))
                    body.append('\r\n')

                    node_id += 1
                    edge_id += 1
                    nodes += 1
                except:
                    continue

update_graph("http://%s:%s/?operation=updateGraph" % (args.host, args.port), request_body)
