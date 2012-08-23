package de.hd.cl.haas.distributedcrawl.Indexer;

import de.hd.cl.haas.distributedcrawl.HasJob;
import de.hd.cl.haas.distributedcrawl.common.Posting;
import de.hd.cl.haas.distributedcrawl.common.PostingList;
import de.hd.cl.haas.distributedcrawl.common.Term;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Hello world!
 *
 */
public class IndexerApp implements HasJob {

    public IndexerApp() {
    }

    @Override
    public Job getJob() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Indexer");

        job.setMapOutputKeyClass(Term.class);
        job.setMapOutputValueClass(Posting.class);

        job.setOutputKeyClass(Term.class);
        job.setOutputValueClass(PostingList.class);

        job.setJarByClass(IndexerApp.class);
        job.setMapperClass(IndexerMap.class);
        job.setReducerClass(IndexerReduce.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        HasJob h = new IndexerApp();
        Job job = h.getJob();
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
