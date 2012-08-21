/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.Text;

/**
 *
 * Represents a term/token as found on a website.
 * 
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class Term extends Text{

    public Term(byte[] utf8) {
        super(utf8);
    }

    public Term(Text utf8) {
        super(utf8);
    }

    public Term(String string) {
        super(string);
    }

    public Term() {
    }
    
}
