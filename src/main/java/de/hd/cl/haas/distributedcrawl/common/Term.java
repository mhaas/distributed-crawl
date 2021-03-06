package de.hd.cl.haas.distributedcrawl.common;

import org.apache.hadoop.io.Text;

/**
 *
 * Represents a term/token as found in a document,
 * e.g. on a web site.
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
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    // TODO: implement hashcode?

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Term other = (Term) obj;
        return other.toString().equals(this.toString());
    }
    
}
