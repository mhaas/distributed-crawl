/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.reduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author haas
 */
public class IndexerReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
        System.out.println("Reducer: Key is: " + key.toString());
        for (Text composite: values) {
            //String term = composite[0];
            //String count = composite[1];
            context.write(key, composite);
        }
            
        
    }
    
    
}
