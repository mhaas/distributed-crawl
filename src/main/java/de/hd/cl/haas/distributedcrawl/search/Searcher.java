/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.search;

import de.hd.cl.haas.distributedcrawl.common.Term;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author haas
 */
public class Searcher {

    private SequenceFile.Reader reader;

    private SequenceFile.Reader getReader(String file) throws IOException {
        if (this.reader == null) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(file), conf);
            Path path = new Path(file);
            this.reader = new SequenceFile.Reader(fs, path, conf);
        }
        return this.reader;
    }

    private Set<WebDBURL> getDocumentsForTerm(String term) throws IOException {
        // TODO: use MapSequenceFile
        // Unfortunately, MapFileOutputFormat is not available in 0.20.2
        // http://hadoop.apache.org/mapreduce/docs/r0.21.0/api/org/apache/hadoop/mapreduce/lib/output/MapFileOutputFormat.html
        Term curTerm = new Term();
        WebDBURLList l = new WebDBURLList();
        boolean found = false;
        // This is slow, but Hadoop 0.20.2 does not support (a non-deprecated, usable)
        // MapFileOutputFormat
        while (this.reader.next(curTerm)) {
            if (curTerm.toString().equals(term)) {
                this.reader.getCurrentValue(l);
                found = true;
                break;
            }
        }
        // prevents obscure NPE in ArrayWritable on empty ArrayWritable
        if (found) {
        WebDBURL[] converted = l.toArray();
        HashSet<WebDBURL> s = new HashSet<WebDBURL>(Arrays.asList(converted));
        return s;
        } else {
            return new HashSet<WebDBURL>();
        }
    }

    /**
     * Given result sets for multiple terms, gets intersection of result sets.
     *
     * @param results
     * @return
     */
    private Set<WebDBURL> score(List<Set<WebDBURL>> results, boolean inclusive) {

        HashSet<WebDBURL> res = new HashSet<WebDBURL>();
        if (results.size() > 0) {
            res.addAll(results.get(0));
            results.remove(0);
        }

        for (Set<WebDBURL> posting : results) {
            // retainAll is intersection
            if (inclusive) {
                res.addAll(posting);
            } else {
                res.retainAll(posting);
            }
        }
        return res;
    }

    public static void main(String[] args) throws IOException {
        
        Date d1 = new Date();
        Searcher s = new Searcher();
        s.getReader(args[0]);
        
        List<Set<WebDBURL>> l = new ArrayList<Set<WebDBURL>>();
        for (int ii = 1; ii < args.length; ii++) {
            String term = args[ii];
            System.err.println("User specified term: " + term);
            Set<WebDBURL> docs = s.getDocumentsForTerm(term);
            l.add(docs);
        }
        boolean inclusive = true;
        Set<WebDBURL> rs = s.score(l, inclusive);
        System.err.println("Inclusiveness status: " + inclusive);
        System.err.println("Got " + rs.size() + " results");
        for (WebDBURL result : rs) {
            System.err.println("Got result: " + result);
        }
        Date d2 = new Date();
        System.err.println("Retrieval took " + (d2.getTime() - d1.getTime()) + "ms");
    }
}
