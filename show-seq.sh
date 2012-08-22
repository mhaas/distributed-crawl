#!/bin/bash
#CLASS="de.hd.cl.haas.distributedcrawl.map.IndexerMap"
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
CLASS="de.hd.cl.haas.distributedcrawl.util.SequenceFileDumper"
hadoop jar "$JAR" "$CLASS" "$1"
