package de.hd.cl.haas.distributedcrawl.util;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * Helper to dump SequenceFiles to stdout.
 *
 * Start from Shell and supply name of SequenceFile as first argument.
 *
 * Example 4-11 from the Book "Hadoop - The definitive Guide".
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class SequenceFileDumper {

    public static String handleRelativeFileURL(String file) {
        if (!(file.startsWith("hdfs:") || file.startsWith("file:"))) {
            String curDir = System.getProperty("user.dir");
            file = "file://" + curDir + "/" + file;
        }
        return file;
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Please supply name of SequenceFile as first argument");
            System.exit(1);
        }
        String file = args[0];
        file = handleRelativeFileURL(file);
        System.err.println("Printing URI: " + file);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
