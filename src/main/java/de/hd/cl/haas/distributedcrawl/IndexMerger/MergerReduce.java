/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.common.*;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * This class (package) merges the output of multiple Indexer (reducer)
 * instances.
 *
 * It also merges output from previous run, thus updating the index.
 *
 * It also sorts a posting list by frequency while eliminating duplicates.
 * 
 * The benefit of sorting the postings by frequency is not entirely clear to me anymore.
 * The only obvious benefit is the ease of duplicate elimination.
 * 
 * The MapReduce book by by Jimmy Lin and Chris Dyer describe that the postings
 * should be sorted by document id, not by frequency. Sorting by document ID
 * has the benefit of allowing quick access to specific IDs by doing a binary search.
 * 
 * TODO: re-read chapter 4 in MapReduce book.
 * 
 * As a stop-gap measure, we can implement in-memory sorting of the list by document ID.
 * Assuming the number of number of URLs per domain is tractable (similar assumption in
 * @see{WebDBMergerReducer})
 * 
 * In the end, we need to do two things:
 * - eliminate duplicate URLs (while retaining correct last-fetched date)
 * - sort URLs (document IDs)
 * 
 * We can do both by applying a key-value conversion pattern where we
 * emit (Term,URL) as key and (Count) as value.
 * This allows us to easily retain the correct last-fetched date.
 * 
 * TODO: this is very similar to what we do in @WebDBMergerApp.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
// Input should arrive sorted by key. We collect these and emit a sorted PostingList
// We should also eleminate duplicate URLTexts - TODO: how? Sort PostingLists by URL?
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
        context.write(this.currentTerm, pl);
        postings.clear();
    }
}
