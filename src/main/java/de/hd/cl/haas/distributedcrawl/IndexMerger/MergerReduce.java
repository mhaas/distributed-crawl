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
public class MergerReduce extends Reducer<Term, Posting, Term, PostingList> {

    private Term currentTerm;
    private ArrayList<Posting> postings = new ArrayList<Posting>();

    @Override
    protected void reduce(Term key, Iterable<Posting> values, Context context) throws IOException, InterruptedException {

        if (this.currentTerm == null) {
            this.currentTerm = new Term();
            this.currentTerm.set(key.toString());

        }
        if (!this.currentTerm.equals(key)) {
            this.yield(context);
            this.currentTerm = new Term();
            this.currentTerm.set(key.toString());
            
        }
        for (Posting p : values) {
            Posting temp = new Posting(p.getURL().toString(), p.getValue().get());
            this.postings.add(temp);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.yield(context);
        super.cleanup(context);
    }

    private void yield(Context context) throws IOException, InterruptedException {
        PostingList pl = new PostingList();
        pl.set(this.postings.toArray(new Posting[0]));
        // currentTerm is null sometimes for empty input files
        if (this.currentTerm != null) {
            context.write(this.currentTerm, pl);
        }
        this.postings.clear();
    }
}
