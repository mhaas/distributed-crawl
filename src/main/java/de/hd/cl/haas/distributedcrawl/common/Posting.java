
package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.RawComparable;

/**
 * A posting is an URL combined with a frequency as payload.
 *
 * Postings are comparable by frequency. Postings are typically associated with
 * terms. In information retrieval, we want to know which documents have the
 * highest frequency of a given term, thus we sort by frequency.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class Posting extends TextLongWritable implements WritableComparable<Posting> {
    // TODO: Implement RawComparable to make things a bit faster in the sorting phase
    
    public Posting() {};

    public Posting(URLText t, LongWritable i) {
        super(t, i);
    }

    public URLText getURL() {
        return new URLText(super.getText());
    }
    
    // Postings are compared by Frequency
    @Override
    public int compareTo(Posting o) {
        // TODO: guard against null?
        return this.getValue().compareTo(o);
    }
}
