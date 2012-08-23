/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.WebDBMerger;

import de.hd.cl.haas.distributedcrawl.common.URLText;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * This package merges the existing WebDB with the URLs freshly
 * discovered during the crawl process in @IndexerApp.
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 * 
 */
public class WebDBMergerMapper extends Mapper<URLText, WebDBURLList, URLText, WebDBURL> {

    @Override
    protected void map(URLText key, WebDBURLList value, Context context) throws IOException, InterruptedException {
       WebDBURL[] urls = value.toArray();
       for (int ii = 0; ii < urls.length; ii++) {
           System.err.println("Write: " + key + "," + urls[ii]);
           context.write(key, urls[ii]);
       }
    }
    
}
