package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * A PostingList is a list of Posting instances.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
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
     *
     * Returns a string representation of the whole list.
     *
     * This is probably somewhat slow, so use with care.
     *
     * @return String representing complete list
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        Posting[] list = this.toArray();
        buf.append("[");
        for (int ii = 0; ii < list.length; ii++) {
            buf.append(list[ii].toString());
            buf.append("||");
        }
        buf.append("]");
        return buf.toString();
    }
}
