/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * Interface indicating whether a class has an associated MapReduce job.
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public interface HasJob {
    
    public Job getJob() throws IOException,InterruptedException;
    
}
