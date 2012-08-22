/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author haas
 */
public class PostingList extends ArrayWritable {

    public PostingList() {
        super(Posting.class);
    }
    @Override
    public Posting[] toArray() {
        return (Posting[]) super.toArray();
    }
}
