/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;


/**
 *
 * Holds URL and last-fetch date.
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class WebDBURL extends TextLongWritable {
    
    public WebDBURL() {
        super();
    }
    
    public WebDBURL(URLText url, long date) {
        super(url, new LongWritable(date));
    }
   
    public WebDBURL(URLText url, Date d) {
        this(url, d.getTime());
    }
    
    public URLText getURLText() {
        return new URLText(super.getText());
    }
    public URL getURL() throws MalformedURLException {
        return this.getURLText().getURL();
    }
    
    public Date getDate() {
        return new Date(super.getValue().get());
    } 
}
