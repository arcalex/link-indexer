#!/bin/sh

# This is an example for how to extract WAT from WARC:
# - List of source directories containing .warc.gz as cmdline args
# - .wat.gz in dir with basename of source dir under ./wats/
# - Assume webarchive-commons/ in ./

# webarchive-commons: https://github.com/internetarchive/webarchive-commons

for srcdir in "$@"; do 
  find "$srcdir" -type f -name "*.warc.gz" | while read i; do 
    dstdir=wats/$(basename "$srcdir")
    mkdir -p "$dstdir"

    java -cp webarchive-commons/target/webarchive-commons-jar-with-dependencies.jar \
      org.archive.extract.ResourceExtractor \
      -wat "$i" > "$dstdir/$(basename "$i" .warc.gz).wat.gz"
  done
done
