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

import dateutil.parser as dp
import urlcanon
import json


def check_path(path, timeout_process):
    if path.endswith(".csv"):
        return path

    return ''


def parse_record(path, node_id, edge_id, process_record, max_identifier_length, dt14):
    # reference identifier and timestamp
    identifier0 = ""
    dt0 = ""

    with open(path) as infile:
        for line in infile:
            fields = line.split(',')
            identifier = fields[0]
            dt = fields[1]
            outlink = urlcanon.parse_url(fields[2].rstrip())
            urlcanon.whatwg(outlink)  # canonicalization

            if dt14:
                dt = dp.parse(dt).strftime('%Y%m%d%H%M%S')

            if (identifier, dt) != (identifier0, dt0):
                if identifier0 != "" and dt0 != "":
                    # this is a new record, invoke the callback function
                    same_batch = process_record(record_array, node_id, edge_id)

                    if not same_batch:
                        node_id = edge_id = 1

                record_array = []
                identifier0 = identifier
                dt0 = dt

                version_node = {
                    "an": {
                        node_id:
                            {
                                "identifier": str(urlcanon.whatwg(urlcanon.parse_url(fields[0])).ssurt(), encoding='utf-8'),
                                "timestamp": dt,
                                "TYPE": "VersionNode"
                            }
                    }
                }

                record_array.append(json.dumps(version_node))
                record_array.append('\r\n')

                source_id = node_id
                node_id += 1

            # definitely becomes true if previous 'if' block was entered
            if (identifier, dt) == (identifier0, dt0):
                # construct node and edge
                node = {
                    "an": {
                        node_id:
                            {
                                "identifier": str(outlink.ssurt(), encoding='utf-8'),
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

        process_record(record_array, node_id, edge_id)
