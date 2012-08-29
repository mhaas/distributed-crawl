package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.common.*;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * This is the Reducer for the IndexMerger.
 *
 * Its input keys are TermCount, input values are URLText. Terms are collected
 * and sorted by count.
 *
 *
 * Its output are again Terms and PostingsLists, which together form the new
 * index.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class MergerReduce extends Reducer<TermCount, URLText, Term, PostingList> {

    Term currentTerm;
    ArrayList<Posting> postings = new ArrayList<Posting>();

    @Override
    protected void reduce(TermCount key, Iterable<URLText> values, Context context) throws IOException, InterruptedException {

        if (this.currentTerm == null) {
            this.currentTerm = key.getTerm();

        }
        if (!this.currentTerm.equals(key.getTerm())) {
            this.yield(context);
            this.currentTerm = key.getTerm();
        }
        for (URLText u : values) {
            Posting p = new Posting(u, key.getValue());
            postings.add(p);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.yield(context);
        super.cleanup(context);
    }

    private void yield(Context context) throws IOException, InterruptedException {
        PostingList pl = new PostingList();
        pl.set(postings.toArray(new Posting[0]));
        // currentTerm is null sometimes for empty input files
        if (this.currentTerm != null) {
            context.write(this.currentTerm, pl);
        }
        postings.clear();
    }
}
