/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.common.Term;
import de.hd.cl.haas.distributedcrawl.common.TermCount;
import de.hd.cl.haas.distributedcrawl.common.URLText;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * This is a special Partitioner for TermCount.
 * 
 * For a TermCount, it is important that all 
 * terms are sent to the same reducer, ordered by 
 * the frequency.
 * 
 * Also see @Comparable implementation in @TermCount.
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class MergerPartitioner extends Partitioner<TermCount, URLText> {

    private HashPartitioner<Term, URLText> hp = new HashPartitioner<Term, URLText>();
 
    @Override
    public int getPartition(TermCount key, URLText value, int numPartitions) {
        return hp.getPartition(key.getTerm(), value, numPartitions);

    }
    
}
