/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package WebDBMerger;

import de.hd.cl.haas.distributedcrawl.common.URLText;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * This package merges the existing WebDB with the URLs freshly discovered
 * during the crawl process in @IndexerApp.
 *
 *
 * @author Michael Haas <haas@cl.uni-heidelberg.de>
 */
public class WebDBMergerReducer extends Reducer<URLText, WebDBURL, URLText, WebDBURLList> {

    @Override
    protected void reduce(URLText key, Iterable<WebDBURL> values, Context context) throws IOException, InterruptedException {

        // TODO: ideally, we would again employ a composite key with a key-value conversion
        // pattern to be able to easily detect duplicates

        // For now, we assume that the list of URLs for a domain is tractable
        // and treat duplicates in memory
        HashMap<URLText, WebDBURL> memory = new HashMap<URLText, WebDBURL>();
        for (WebDBURL u : values) {
            if (memory.containsKey(u.getURLText())) {
                long currentDate = memory.get(u.getURLText()).getValue().get();
                long newDate = u.getValue().get();
                // if we have a newer last-fetch date, update it
                if (newDate > currentDate) {
                    memory.put(u.getURLText(), u);
                }
            } else {
                // first time we see this URL, so in it goes
                memory.put(u.getURLText(), u);
            }
        }
        WebDBURLList result = new WebDBURLList();
        result.fromCollection(memory.values());
        context.write(key, result);

    }
}
