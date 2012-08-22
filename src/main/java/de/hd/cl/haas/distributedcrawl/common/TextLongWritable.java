/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class TextLongWritable implements Writable {

    private Text text = new Text();
    private LongWritable value = new LongWritable();
    
    
    public Text getText(){
        return this.text;
    }
    public LongWritable getValue() {
        return this.value;
    }
    
    // I assume this would be hidden anyways by ctors with arguments
    private TextLongWritable() {};
    
    /**
     * Constructor.
     * 
     * @param t Text instance
     * @param i IntWritable instance
     */
   
    public TextLongWritable(Text t, LongWritable i) {
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
    public TextLongWritable(String s, Integer i) {
        this.text = new Text(s);
        this.value = new LongWritable(i);
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
