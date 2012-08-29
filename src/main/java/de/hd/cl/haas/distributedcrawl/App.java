package de.hd.cl.haas.distributedcrawl;

import de.hd.cl.haas.distributedcrawl.IndexMerger.IndexMergerApp;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerApp;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerMap;
import de.hd.cl.haas.distributedcrawl.Indexer.IndexerReduce;
import de.hd.cl.haas.distributedcrawl.WebDBMerger.WebDBMergerApp;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App {

    public static final String WEBDB_DIR = "webdb";
    public static final String WEBDB_MERGED_DIR = "webdb-merged";
    public static final String FRESHURLS_DIR = "fresh-urls";
    public static final String INDEX_RAW_DIR = "index-raw";
    public static final String INDEX_SORTED_DIR = "index-sorted";
    // index from previous run
    public static final String INDEX_OLD_DIR = "index-old";

    private static void handleStatus(String job, boolean success, int iteration) {
        if (!success) {
            System.err.println("Job " + job + " failed in iteration " + iteration + ".");
            System.exit(1);
        } else {
            System.err.println("Job " + job + " successful in iteration " + iteration + ".");
        }
    }

    private static void handleStatus(String job, boolean success) {
        handleStatus(job, success, -1);
    }

    private static void printUsage() {
        System.err.println("Error! Provide number of iterations as first argument!");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(WEBDB_DIR), conf);


        Path freshURLs = new Path(FRESHURLS_DIR);
        Path rawIndex = new Path(INDEX_RAW_DIR);

        Path webdb;
        Path webdbMerged;
        Path sortedIndex;
        Path oldIndex;

        if (args.length < 1) {
            printUsage();
        }

        Date start = new Date();
        int iterations = -1;
        try {
            iterations = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            printUsage();
        }
        System.err.println("User requested " + iterations + " iterations");
        for (int ii = 0; ii < iterations; ii++) {
            System.err.println("Now running iteration " + ii);
            Date iterStart = new Date();

            // Indexer:
            //  - reads from WEBDB_DIR
            //  - writes into INDEX_RAW_DIR and FRESH_URLS_DIR
            // IndexMerger:
            // - reads from INDEX_RAW_DIR and INDEX_OLD_DIR
            // - writes into INDEX_SORTED_DIR
            // WebDBMerger
            // - reads from WEBDB_DIR and FRESH_URLS_DIR
            // - writes into WEB_MERGED_DIR



            // flip-flop input and output directories on every run
            if (ii % 2 == 0) {
                System.err.println("Flipping directories in iteration " + ii);
                webdb = new Path(WEBDB_DIR);
                webdbMerged = new Path(WEBDB_MERGED_DIR);
                sortedIndex = new Path(INDEX_SORTED_DIR);
                oldIndex = new Path(INDEX_OLD_DIR);


            } else {
                System.err.println("Flipping directories in iteration " + ii);
                webdb = new Path(WEBDB_MERGED_DIR);
                webdbMerged = new Path(WEBDB_DIR);
                sortedIndex = new Path(INDEX_OLD_DIR);
                oldIndex = new Path(INDEX_SORTED_DIR);
            }
            // oldIndex always is a pointer to the input directory for IndexMerger
            // We need to make sure it exists in the first iteration or
            // Hadoop will error out
            if (!fs.exists(oldIndex)) {
                fs.mkdirs(oldIndex);
            }

            fs.delete(freshURLs, true);
            fs.mkdirs(freshURLs);
            fs.delete(rawIndex, true);
            //fs.mkdirs(rawIndex);

            // delete old stuff, we have data of previous run in oldIndex
            fs.delete(sortedIndex, true);
            // delete old stuff, we have data of previous run (webdbMerged + freshurls) in webdb
            fs.delete(webdbMerged, true);


            Job indexerJob = new IndexerApp().getJob();

            conf.setInt("mapred.map.tasks", 10);
            // TODO: for multiple mappers, it's probably inefficient to copy everything
            // to a single reducer (?), so having multiple reducers might speed things up
            // this doesn't break our data model as the next stop, the 
            // indexMerger, can work just fine with multiple input files
            // in fact, that might speed up the indexMerger a bit
            // indexerJob.setNumReduceTasks(30);



            //indexerJob.setMapperClass(MultithreadedMapper.class);
            //conf.set("mapred.map.multithreadedrunner.class", IndexerMap.class.getCanonicalName());
            //conf.set("mapred.map.multithreadedrunner.threads", "8");
            FileInputFormat.addInputPath(indexerJob, webdb);
            FileOutputFormat.setOutputPath(indexerJob, rawIndex);

            boolean success = indexerJob.waitForCompletion(true);
            handleStatus(indexerJob.getJobName(), success, ii);


            Job indexMergerJob = new IndexMergerApp().getJob();
            FileInputFormat.addInputPath(indexMergerJob, rawIndex);
            FileInputFormat.addInputPath(indexMergerJob, oldIndex);

            FileOutputFormat.setOutputPath(indexMergerJob, sortedIndex);

            success = indexMergerJob.waitForCompletion(true);
            handleStatus(indexMergerJob.getJobName(), success, ii);

            Job webDBMergerJob = new WebDBMergerApp().getJob();
            // specifies the number of reducers, thus the number
            // of output files, which should result in an increased
            // number of indexer mappers
            // so in pipelined job environments, configuring the number
            // of reducers may have a positive performance impact
            // for the following map tasks
            // especially in our case with small input files and a huge
            // processing cost for value.
            webDBMergerJob.setNumReduceTasks(40);
            FileInputFormat.addInputPath(webDBMergerJob, webdb);
            FileInputFormat.addInputPath(webDBMergerJob, freshURLs);
            FileOutputFormat.setOutputPath(webDBMergerJob, webdbMerged);
            success = webDBMergerJob.waitForCompletion(true);
            handleStatus(webDBMergerJob.getJobName(), success, ii);
            Date iterStop = new Date();
            System.err.println("Iteration took " + (iterStop.getTime() - iterStart.getTime()) / 1024);
        }

        Date end = new Date();
        System.err.println("Crawling " + iterations + " took " + ((end.getTime() - start.getTime()) / 1000) + "s");
    }
}
