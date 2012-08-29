package de.hd.cl.haas.distributedcrawl.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * Abstract tuple class holding a Text and a Long.
 *
 * This is sub-classed to implement different key and value classes which need
 * to hold two values.
 *
 *
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 *
 */
public abstract class TextLongWritable implements Writable {

    private Text text = new Text();
    private LongWritable value = new LongWritable();

    public Text getText() {
        return this.text;
    }

    public LongWritable getValue() {
        return this.value;
    }

    // Default constructor is needed by the Hadoop framework or
    // deserialization of objects will fail
    public TextLongWritable() {
    }

    ;
    
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

    @Override
    public String toString() {
        return this.text.toString() + ";" + this.value.toString();
    }
}
