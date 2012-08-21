/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.Indexer;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author haas
 */
public class IndexerReduce extends Reducer<Text, Text, Text, IndexerReduce.TextArrayWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        System.out.println("Reducer: Key is: " + key.toString());
        ArrayList<Text> memory = new ArrayList<Text>();
        for (Text composite : values) {
            // key is term, value is posting: url,termcount
            //String[] tokens = composite.toString().split(",");
            //String term = tokens[0];
            //String count = tokens[1];
            //context.write(key, composite);
            memory.add(composite);
        }
        TextArrayWritable result = new TextArrayWritable();
        // Ugly. to populate TextArrayWritable, we need to call set()
        // which expects an Array.
        // We need to pass in an array of type Text to get the type right
        result.set(memory.toArray(new Text[memory.size()]));
        context.write(key, result);

    }
    // from http://stackoverflow.com/a/8210025

    public static class TextArrayWritable extends ArrayWritable {

        public TextArrayWritable() {
            super(Text.class);
        }
    }
}
