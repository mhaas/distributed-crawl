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
public class WebDBURLList extends ArrayWritable {
    
     public WebDBURLList() {
        super(WebDBURL.class);
    }
    @Override
    public WebDBURL[] toArray() {
        return (WebDBURL[]) super.toArray();
    }
    
}
