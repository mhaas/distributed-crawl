/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.io.ArrayWritable;

/**
 * This class holds a list of WebDB entries.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 * 
 */
public class WebDBURLList extends ArrayWritable {

    public WebDBURLList() {
        super(WebDBURL.class);
    }

    @Override
    public WebDBURL[] toArray() {
        return (WebDBURL[]) super.toArray();
    }

    public void fromCollection(Collection<WebDBURL> urls) {
        super.set(urls.toArray(new WebDBURL[urls.size()]));
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        WebDBURL[] urls = this.toArray();
        for (int ii = 0; ii < urls.length; ii++) {
            buf.append(urls[ii]);
        }
        return buf.toString();
    }
}
