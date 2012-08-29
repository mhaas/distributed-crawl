#!/bin/bash
IN1="webdb-0"
IN2="fresh-urls"
OUT="webdb-merged"
COPY=""
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
CLASS="de.hd.cl.haas.distributedcrawl.WebDBMerger.WebDBMergerApp"
#hadoop dfs -rmr "$IN"
#hadoop dfs -copyFromLocal "$IN" . 
hadoop dfs -rmr "$OUT"
hadoop jar "$JAR" "$CLASS" "$IN1" "$IN2" "$OUT"
rm -rf "$OUT"
hadoop dfs -copyToLocal "$OUT" .
for token in $COPY; do
	hadoop dfs -copyToLocal "$token" .;
done
