/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.RawComparable;

/**
 * A posting is an URL combined with a frequency.
 *
 * Postings are comparable by frequency. Postings are typically associated with
 * terms. In information retrieval, we want to know which documents have the
 * highest frequency of a given term, thus we sort by frequency.
 *
 * @author haas
 */
public class Posting extends TextIntWritable implements WritableComparable<Posting> {
    // TODO: Implement RawComparable to make things a bit faster in the sorting phase

   /* public Posting(String s, Integer i) {
        super(s, i);
    } */

    public Posting(URLText t, IntWritable i) {
        super(t, i);
    }

    public URLText getURL() {
        return new URLText(super.getText());
    }
    
    @Override
    public int compareTo(Posting o) {
        // TODO: guard against null?
        return this.getValue().compareTo(o);
    }
}
