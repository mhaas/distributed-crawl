/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.common.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is the Mapper for the IndexMerger job.
 *
 * Its input are terms as keys and posting lists as values. As output keys, it
 * emits TermCount instances, which is a tuple holding terms and posting
 * frequencies (key-value conversion pattern). The output value for each
 * TermCount will be the accompanying URL.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class IndexMergerMap extends Mapper<Term, PostingList, TermCount, URLText> {

    @Override
    protected void map(Term key, PostingList value, Context context) throws IOException, InterruptedException {
        Posting[] postings = value.toArray();
        for (int ii = 0; ii < postings.length; ii++) {
            Posting p = postings[ii];
            TermCount tc = new TermCount(key, p.getValue());
            context.write(tc, p.getURL());
        }
    }
}
