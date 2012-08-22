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
    
    /**
     * This is probably somewhat slow, so use with care.
     * @return 
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        Posting[] list = this.toArray();
        for (int ii = 0; ii < list.length; ii++) {
            buf.append(list[ii].toString());
            buf.append("\n");
        }
        return buf.toString();    
    }
}
