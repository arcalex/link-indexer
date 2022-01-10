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
import urlcanon
import re
import dateutil.parser as dp
import json
from subprocess import run, TimeoutExpired
from urllib.parse import urljoin
from warcio.archiveiterator import ArchiveIterator

wat_jar = './webarchive-commons-jar-with-dependencies.jar'

previous_uri = ''
previous_dt = ''


def check_path(path, timeout_process):
    if path.endswith(".wat.gz"):
        return path

    x = re.sub("\.w?arc\.gz$", '', path)

    # path is neither .warc.gz nor .arc.gz
    if x == path:
        return ''

    x = x + ".wat.gz"

    with open(x, "wb") as outfile:
        try:
            rc = run(("java", "-cp", wat_jar,
                      "org.archive.extract.ResourceExtractor", "-wat", path), stdout=outfile, timeout=timeout_process)

            if rc.returncode != 0:
                return ''
        except TimeoutExpired as exc:
            os.remove(x)
            return exc

    return x


def parse_record(path, node_id, edge_id, process_record, max_identifier_length, dt14):
    with open(path, "rb") as infile:
        # loop on every record in WAT
        for record in ArchiveIterator(infile):
            record_array = []

            if record.rec_type != 'metadata':
                continue

            warc_target_uri = urlcanon.parse_url(record.rec_headers.get_header('WARC-Target-URI'))
            urlcanon.whatwg(warc_target_uri)  # canonicalization

            # select only members whose WARC-Target-URI begins with "https?://"
            if not re.search("^https?://", str(warc_target_uri)) or len(str(warc_target_uri)) > max_identifier_length:
                continue

            content = json.loads(record.raw_stream.read().decode('utf-8'))

            try:
                links = content["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
            except:
                links = ''

            # WARC-Date from Envelope, not WAT record header (#17)
            try:
                dt = content["Envelope"]["WARC-Header-Metadata"]["WARC-Date"]
            except:
                dt = ''

            if dt14:
                dt = dp.parse(dt).strftime('%Y%m%d%H%M%S')
                
            # exclude consecutive records with same identifier and timestamp
            if str(warc_target_uri) == previous_uri and dt == previous_dt:
                continue
            globals()['previous_uri'] = str(warc_target_uri)
            globals()['previous_dt'] = dt

            # construct node with timestamp (VersionNode)
            version_node = {
                "an": {
                    node_id:
                    {
                        "identifier": str(warc_target_uri.ssurt(), encoding='utf-8'),
                        "timestamp": dt,
                        "TYPE": "VersionNode"
                    }
                }
            }

            record_array.append(json.dumps(version_node))
            record_array.append('\r\n')

            source_id = node_id
            node_id += 1

            # loop on links if not empty and get all urls
            if links != '':
                for link in links:
                    # this is for empty outlink elements, maybe a bug in webarchive-commons used to generate WAT
                    try:
                        # convert relative outlink to absolute one
                        url = urljoin(str(warc_target_uri), link["url"])
                        urlcanon.whatwg(url)  # canonicalization

                        # match only urls that begin with "https?://"
                        if not re.search("^https?://", url) or len(str(url)) > max_identifier_length:
                            continue

                        # construct node and edge
                        node = {
                            "an": {
                                node_id:
                                {
                                    "identifier": str(urlcanon.parse_url(url).ssurt(), encoding="utf-8"),
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

                        record_array.append(json.dumps(node))
                        record_array.append('\r\n')
                        record_array.append(json.dumps(edge))
                        record_array.append('\r\n')

                        node_id += 1
                        edge_id += 1
                    except:
                        continue

            same_batch = process_record(record_array, node_id, edge_id)

            if not same_batch:
                node_id = edge_id = 1
