/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.util.List;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * @author haas
 */
public class WebDBURLList extends ArrayWritable {
    
     public WebDBURLList() {
        super(WebDBURL.class);
    }
    @Override
    public WebDBURL[] toArray() {
        return (WebDBURL[]) super.toArray();
    }
    
    public void fromList(List<WebDBURL> urls) {
        super.set(urls.toArray(new WebDBURL[urls.size()]));
    }
    
}
