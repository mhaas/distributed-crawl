package de.hd.cl.haas.distributedcrawl.common;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.io.Text;

/**
 *
 * This class represents an URL.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 * 
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
