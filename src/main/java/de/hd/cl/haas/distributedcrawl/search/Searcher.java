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
 * Runs queries against the index.
 *
 * Start this in a shell. The first argument is the SequenceFile containing the
 * index. The second argument is either "OR" or "AND", indicating the mode in
 * which result sets for each term are merged. Following arguments are search
 * terms.
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
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

        Term curTerm = new Term();
        WebDBURLList l = new WebDBURLList();
        boolean found = false;
        // This is slow, but Hadoop 0.20.2 does not support (a non-deprecated, usable)
        // MapFileOutputFormat
        // Unfortunately, MapFileOutputFormat is not available in 0.20.2
        // http://hadoop.apache.org/mapreduce/docs/r0.21.0/api/org/apache/hadoop/mapreduce/lib/output/MapFileOutputFormat.html
        while (this.reader.next(curTerm)) {
            if (curTerm.toString().equals(term)) {
                this.reader.getCurrentValue(l);
                found = true;
                break;
            }
        }
        // seek back to beginning of file, otherwise we will miss query results
        this.reader.close();
        this.reader = null;
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
     * Merges result sets for multiple terms.
     *
     * If the inclusive parameter is true, the union of result sets is returned,
     * corresponding to the "OR" search mode. If the inclusive parameter is
     * false, the intersection of result sets is returned, corresponding to the
     * "AND" search mode.
     *
     * @param results List of result sets
     * @param inclusive If true, return union of result sets
     * @return union or intersection of result sets
     */
    private Set<WebDBURL> merge(List<Set<WebDBURL>> results, boolean inclusive) {

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

    private static void printUsage() {
        System.err.println("First argument: index file name");
        System.err.println("Second argument: search mode: 'OR', 'AND'");
        System.err.println("Following arguments: search terms");
    }

    public static void main(String[] args) throws IOException {

        Date d1 = new Date();
        Searcher s = new Searcher();
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }
        String mode = args[1];
        boolean inclusive = false;
        if (mode.equals("OR")) {
            inclusive = true;
        } else if (mode.equals("AND")) {
            inclusive = false;
        } else {
            printUsage();
            System.exit(1);
        }

        List<Set<WebDBURL>> l = new ArrayList<Set<WebDBURL>>();
        for (int ii = 2; ii < args.length; ii++) {
            String term = args[ii];
            System.err.println("User specified term: " + term);
            // get new reader for each term
            s.getReader(args[0]);
            Set<WebDBURL> docs = s.getDocumentsForTerm(term);
            System.err.println("Got " + docs.size() + " documents for term");

            l.add(docs);
        }

        Set<WebDBURL> rs = s.merge(l, inclusive);
        System.err.println("Merging result sets: " + inclusive);
        System.err.println("Got " + rs.size() + " results");
        for (WebDBURL result : rs) {
            System.err.println("Got result: " + result.getURLText());
        }
        Date d2 = new Date();
        System.err.println("Retrieval took " + (d2.getTime() - d1.getTime()) + "ms");
    }
}
