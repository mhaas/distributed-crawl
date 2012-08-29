#!/bin/bash
#CLASS="de.hd.cl.haas.distributedcrawl.map.IndexerMap"
JAR="target/hadoop-deploy/DistributedCrawl-hdeploy.jar"
CLASS="de.hd.cl.haas.distributedcrawl.search.Searcher"
hadoop jar "$JAR" "$CLASS" "$@"
