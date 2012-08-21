package de.hd.cl.haas.distributedcrawl;

import de.hd.cl.haas.distributedcrawl.Indexer.IndexerMap;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerReduce;
import de.hd.cl.haas.distributedcrawl.common.TextArrayWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App {

    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Indexer");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        job.setJarByClass(IndexerMap.class);
        job.setMapperClass(IndexerMap.class);
        job.setReducerClass(IndexerReduce.class);

        // Enable compression for intermediate output
            // as described indasgerman 
        //conf.setCompressMapOutput(true);
        //job.setMapOutputCompressorClass(GzipCodec.class);


        // TextInputFormat: key is offset in file, value is line
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
