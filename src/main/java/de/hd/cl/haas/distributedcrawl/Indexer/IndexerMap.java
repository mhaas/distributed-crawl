/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.hd.cl.haas.distributedcrawl.Indexer;

import de.hd.cl.haas.distributedcrawl.common.Posting;
import de.hd.cl.haas.distributedcrawl.common.Term;
import de.hd.cl.haas.distributedcrawl.common.URLText;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 *
 * @author Michael Haas
 */
public class IndexerMap extends Mapper<LongWritable, Term, Term, Posting> {

    private SequenceFile.Writer writer;

    @Override
    protected void map(LongWritable key, Term value, Context context) throws IOException, InterruptedException {
        if (this.writer == null) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path p = new Path(context.getWorkingDirectory(), "foo");
            this.writer = SequenceFile.createWriter(fs, context.getConfiguration(), p, Text.class, Text.class);
            System.out.println("Writer initialized.");
        }
        System.out.println("key is " + key.toString());
        System.out.println("value is " + value.toString());
        URL url = new URL(value.toString());
        url.openConnection();
        InputStream stream = url.openStream();
        Source source = new Source(stream);
        source.fullSequentialParse();

        String completeContent = source.getTextExtractor().toString();
        System.out.println("CompleteContent: ");
        System.out.println(completeContent);
        // poor man's tokenizer
        String[] tokens = completeContent.split(" ");

        List<Element> anchorElements = source.getAllElements("a");
        for (Element anchorElement : anchorElements) {
            String target = anchorElement.getAttributeValue("href");
            System.out.println("Anchor target is: " + target);
            if (target == null)
                continue;
            if (target.startsWith("/")) {
                target = key + target;
            }
            if (!target.startsWith("http")) {
                System.out.println("URL with unsupported scheme.");
            }
            // we use the URL of the current document as key, but I don't plan
            // on using this in the DB
            this.writer.append(value, target);
        }

        // count absolute frequencies for terms
        HashMap<String, Integer> counts = new HashMap<String, Integer>();
        for (int ii = 0; ii < tokens.length; ii++) {
            String term = tokens[ii];
            // clean up string
            term = term.replace(",", "");
            term = term.replace(".", "");
            if (!counts.containsKey(term)) {
                counts.put(term, 0);
            }
            counts.put(term, counts.get(term) + 1);
        }
        Term tTerm = new Term();
        IntWritable freq = new IntWritable();
        URLText u = new URLText(value);
        for (String term : counts.keySet()) {
            // Emit(term t, posting <n,H{t}>)      
            freq.set(counts.get(term));
            Posting p = new Posting(u, freq);
            tTerm.set(term);
            context.write(tTerm, p);
        }
    }
}
