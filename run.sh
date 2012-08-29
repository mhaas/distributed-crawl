#!/bin/bash
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
#CLASS="de.hd.cl.haas.distributedcrawl.map.IndexerMap"
CLASS="de.hd.cl.haas.distributedcrawl.App"
WEBDB="webdb"
DIRS="$WEBDB webdb-merged fresh-urls index-raw index-sorted"
# clean up files to copy
for token in $DIRS; do
	hadoop dfs -rmr "$token";
done
hadoop dfs -copyFromLocal $WEBDB $WEBDB
# populate webdb
hadoop jar "$JAR" "$CLASS" $@
hadoop dfs -copyToLocal "$OUT" .
for token in $DIRS; do
	hadoop dfs -copyToLocal $token .
done
