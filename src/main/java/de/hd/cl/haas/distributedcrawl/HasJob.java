/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author haas
 */
public interface HasJob {
    
    public Job getJob() throws IOException,InterruptedException;
    
}
