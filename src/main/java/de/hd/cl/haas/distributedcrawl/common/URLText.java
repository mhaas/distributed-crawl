/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.io.Text;

/**
 *
 * @author haas
 */
public class URLText extends Text {

    public URLText(byte[] utf8) {
        super(utf8);
    }

    public URLText(Text utf8) {
        super(utf8);
    }

    public URLText(String string) {
        super(string);
    }

    public URLText() {
    }
    
    public URL getURL() throws MalformedURLException {
        return new URL(this.toString());
    }
    
    
}
