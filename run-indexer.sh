#!/bin/bash
IN="webdb-0"
OUT="index-raw"
COPY="fresh-urls"
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
#CLASS="de.hd.cl.haas.distributedcrawl.map.IndexerMap"
CLASS="de.hd.cl.haas.distributedcrawl.Indexer.IndexerApp"
# clean up files to copy
for token in $COPY; do
	hadoop dfs -rmr "$token";
done
hadoop dfs -rmr "$IN"
hadoop dfs -copyFromLocal "$IN" . 
hadoop dfs -rmr "$OUT"
hadoop jar "$JAR" "$CLASS" "$IN" "$OUT"
rm -rf "$OUT"
hadoop dfs -copyToLocal "$OUT" .
for token in $COPY; do
	hadoop dfs -copyToLocal $token .
done
