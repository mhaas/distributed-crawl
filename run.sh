#!/bin/bash
IN="dcrawl"
OUT="dcrawl-out"
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
#CLASS="de.hd.cl.haas.distributedcrawl.map.IndexerMap"
CLASS="de.hd.cl.haas.distributedcrawl.App"
hadoop dfs -rmr "$IN"
hadoop dfs -copyFromLocal "$IN" . 
hadoop dfs -rmr "$OUT"
hadoop jar "$JAR" "$CLASS" "$IN" "$OUT"
rm -rf "$OUT"
hadoop dfs -copyToLocal "$OUT" .
