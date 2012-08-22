#!/bin/bash
IN="index-raw"
OUT="index-sorted"
COPY=""
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
CLASS="de.hd.cl.haas.distributedcrawl.IndexMerger.IndexMergerApp"
hadoop dfs -rmr "$IN"
hadoop dfs -copyFromLocal "$IN" . 
hadoop dfs -rmr "$OUT"
hadoop jar "$JAR" "$CLASS" "$IN" "$OUT"
rm -rf "$OUT"
hadoop dfs -copyToLocal "$OUT" .
for token in $COPY; do
	hadoop dfs -copyToLocal "$token" .;
done
