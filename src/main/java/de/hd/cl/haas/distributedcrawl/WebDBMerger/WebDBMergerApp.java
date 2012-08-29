package de.hd.cl.haas.distributedcrawl.WebDBMerger;

import de.hd.cl.haas.distributedcrawl.HasJob;
import de.hd.cl.haas.distributedcrawl.common.URLText;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * This package merges the existing WebDB with the URLs freshly discovered
 * during the crawl process in @IndexerApp.
 *
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class WebDBMergerApp implements HasJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = new WebDBMergerApp().getJob();
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }

    @Override
    public Job getJob() throws IOException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "WebDBMerger");

        job.setMapOutputKeyClass(URLText.class);
        job.setMapOutputValueClass(WebDBURL.class);

        job.setOutputKeyClass(URLText.class);
        job.setOutputValueClass(WebDBURLList.class);

        job.setJarByClass(WebDBMergerApp.class);
        job.setMapperClass(WebDBMergerMap.class);
        job.setReducerClass(WebDBMergerReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job;
    }
}
