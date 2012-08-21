/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.common.Posting;
import de.hd.cl.haas.distributedcrawl.common.PostingList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * This class (package) merges the output of multiple Indexer (reducer) instances.
 * 
 * It also merges output from previous run, thus updating the index.
 * 
 * It also sorts a posting list by frequency while eliminating duplicates.
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
// Input is SequenceFileFormat <Text,TextArrayList>
public class MergerReduce extends Reducer<Text,PostingList,Text,Posting>{
    
    
}
