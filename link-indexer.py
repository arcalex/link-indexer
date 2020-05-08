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

import sys
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urljoin
import json
import re
import urlcanon

batch_size = 1000  # TODO: should not be hard coded
record_count = 1
node_id = 1
edge_id = 1

# accept multiple WAT files as command-line arguments
for i in range(1, len(sys.argv)):
    wat_file = str(sys.argv[i])

    with open(wat_file, 'rb') as stream:
        # loop on every record in WAT
        for record in ArchiveIterator(stream):
            if record_count > batch_size:
                node_id = edge_id = record_count = 1

                # TODO: send to link-serv
                print("...SEND TO LINK-SERV...")

            if record.rec_type != 'metadata':
                continue

            warc_target_uri = urlcanon.parse_url(record.rec_headers.get_header('WARC-Target-URI'))
            urlcanon.whatwg(warc_target_uri)  # canonicalization

            # select only members whose WARC-Target-URI begins with "https?://"
            if not re.search("^https?://", str(warc_target_uri)):
                continue

            warc_date = record.rec_headers.get_header('WARC-Date')

            # construct node with timestamp (VersionNode)
            version_node = json.dumps({
                "an": {
                    node_id:
                    {
                        "url": str(warc_target_uri.ssurt(), encoding='utf-8'),
                        "timestamp": warc_date,
                        "TYPE": "VersionNode"
                    }
                }
            })
            print(version_node)

            source_id = node_id
            node_id += 1

            content = json.loads(record.raw_stream.read())

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
                    if not re.search("^https?://", url):
                        continue

                    # construct node and edge

                    node = json.dumps({
                        "an": {
                            node_id:
                            {
                                "url": str(urlcanon.parse_url(url).ssurt(), encoding="utf-8"),
                                "TYPE": "Node"
                            }
                        }
                    })
                    print(node)

                    edge = json.dumps({
                        "ae": {
                            edge_id:
                            {
                                "directed": "true",
                                "source": source_id,
                                "target": node_id
                            }
                        }
                    })
                    print(edge)

                    node_id += 1
                    edge_id += 1
                except:
                    continue

            record_count += 1
