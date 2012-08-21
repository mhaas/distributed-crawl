/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author haas
 */
public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }
    
}
