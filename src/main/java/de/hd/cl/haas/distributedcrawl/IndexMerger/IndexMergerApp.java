package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.HasJob;
import de.hd.cl.haas.distributedcrawl.common.PostingList;
import de.hd.cl.haas.distributedcrawl.common.Term;
import de.hd.cl.haas.distributedcrawl.common.TermCount;
import de.hd.cl.haas.distributedcrawl.common.URLText;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * This package merges the output of multiple Indexer instances.
 *
 * It also merges output from previous run, thus updating the index.
 *
 * It also sorts a posting list by frequency while eliminating duplicates.
 *
 * The benefit of sorting the postings by frequency is not entirely clear to me
 * anymore. The only obvious benefit is the ease of duplicate elimination.
 *
 * The MapReduce book by by Jimmy Lin and Chris Dyer describe that the postings
 * should be sorted by document id, not by frequency. Sorting by document ID has
 * the benefit of allowing quick access to specific IDs by doing a binary
 * search.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */

// In the end, we need to do two things: - eliminate duplicate URLs (while
// retaining correct last-fetched date) - sort URLs (document IDs)
// We can do both by applying a key-value conversion pattern where we emit
// (Term,URL) as key and (Count) as value. This allows us to easily retain the
// correct last-fetched date.
public class IndexMergerApp implements HasJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new IndexMergerApp().getJob();

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);


    }

    @Override
    public Job getJob() throws IOException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "IndexMerger");

        job.setMapOutputKeyClass(TermCount.class);
        job.setMapOutputValueClass(URLText.class);

        job.setOutputKeyClass(Term.class);
        job.setOutputValueClass(PostingList.class);

        job.setJarByClass(IndexMergerApp.class);
        job.setMapperClass(IndexMergerMap.class);
        job.setPartitionerClass(IndexMergerPartitioner.class);
        job.setReducerClass(MergerReduce.class);


        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job;
    }
}
