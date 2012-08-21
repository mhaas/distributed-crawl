package de.hd.cl.haas.distributedcrawl.IndexMerger;

import de.hd.cl.haas.distributedcrawl.*;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerMap;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerReduce;
import de.hd.cl.haas.distributedcrawl.common.PostingList;
import de.hd.cl.haas.distributedcrawl.common.Term;
import de.hd.cl.haas.distributedcrawl.common.TextArrayWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class IndexMergerApp {

    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Indexer");

        job.setMapOutputKeyClass(Term.class);
        job.setMapOutputValueClass(PostingList.class);

        job.setOutputKeyClass(Term.class);
        job.setOutputValueClass(PostingList.class);

        job.setJarByClass(IndexMergerApp.class);
        job.setMapperClass(MergerMap.class);
        job.setPartitionerClass(MergerPartitioner.class);
        job.setReducerClass(MergerReduce.class);

       
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
