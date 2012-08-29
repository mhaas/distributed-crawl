package de.hd.cl.haas.distributedcrawl.WebDBMerger;

import de.hd.cl.haas.distributedcrawl.common.URLText;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;


 /**
  * This class is the Mapper part of the WebDBMerger.
  * 
  * Its input are existing WebDBs, be it the hold WebDB from a previous
  * iteration of the pipeline or the new URLs discovered by the Indexer.
  * 
  * 
  * @author Michael Haas <haas@cl.uni-heidelberg.de>
  */
public class WebDBMergerMap extends Mapper<URLText, WebDBURLList, URLText, WebDBURL> {

    @Override
    protected void map(URLText key, WebDBURLList value, Context context) throws IOException, InterruptedException {
       WebDBURL[] urls = value.toArray();
       for (int ii = 0; ii < urls.length; ii++) {
           System.err.println("Write: " + key + "," + urls[ii]);
           context.write(key, urls[ii]);
       }
    }
    
}
