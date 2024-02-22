# link-indexer
data collection tool for web archive graph visualization (LinkGate)

link-indexer is the tool that runs on web archive storage where WARC/ARC
files are kept and collects outlinks data to feed to link-serv to load
into the graph data store.  In a subsequent phase of the project,
collected data may include details besides outlinks to enrich the
visualization.

Input to the tool is WAT files, which are generated from WARC/ARC files.

archive-metadata-extractor was once used to generate WAT files.

That tool, however, seems to generate WAT files that are invalid.

“... the metadata records in this WAT are missing a \r\n at the end of the body -- there are supposed to be 2 pairs, and there's only 1. So yes, it's invalid.”

See:

https://github.com/webrecorder/warcio/issues/102

Use instead ia-web-commons and ia-hadoop-tools[]:

https://github.com/commoncrawl/ia-web-commons

Assuming Java and Maven are there, build webarchive-commons as follows:

```
git clone https://github.com/commoncrawl/ia-web-commons
cd ia-web-commons
mvn -f pom.xml install
cd -
git clone https://github.com/commoncrawl/ia-hadoop-tools
cd ia-hadoop-tools
mvn package
```
Note that the WARC file must be placed in a folder warc/

Run the following to generate wat.gz from some file.warc.gz or arc.gz:

```
java -jar ./target/ia-hadoop-tools-jar-with-dependencies.jar WEATGenerator \
name_of_archive .../warc/warcfile.warc.gz
```

Use a tool such as Ansible, dsh, or pdsh (among many others) for ad-hoc
execution on multiple hosts in a distributed environment.  You may also
use a configuration management tool (again, such as Ansible) to
configure a cronjob or similar on each host in the distributed
environment to periodically execute link-indexer.

## Prerequisites

Install the following prerequisites:

```
# pip3 install warcio urlcanon python-dateutil retry requests configargparse
```
