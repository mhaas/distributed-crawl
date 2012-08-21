/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * Tuple class holding an Text and an Int.
 * 
 * Can be used to hold postings.
 * 
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 * 
 */
public class TextIntWritable implements Writable {

    private Text text = new Text();
    private IntWritable value = new IntWritable();
    
    
    public Text getText(){
        return this.text;
    }
    public IntWritable getValue() {
        return this.value;
    }
    
    // I assume this would be hidden anyways by ctors with arguments
    private TextIntWritable() {};
    
    /**
     * Constructor.
     * 
     * @param t Text instance
     * @param i IntWritable instance
     */
   
    public TextIntWritable(Text t, IntWritable i) {
        this.text = t;
        this.value = i;
    }
    
    /**
     * 
     * Constructor
     * 
     * @param s String instance
     * @param i Integer instance 
     */
    public TextIntWritable(String s, Integer i) {
        this.text = new Text(s);
        this.value = new IntWritable(i);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        this.text.write(out);
        this.value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.text.readFields(in);
        // this.text.set(Text.readString(in));
        this.value.readFields(in);
    }
}
