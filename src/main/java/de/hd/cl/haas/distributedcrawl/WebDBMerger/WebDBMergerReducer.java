package de.hd.cl.haas.distributedcrawl.WebDBMerger;

import de.hd.cl.haas.distributedcrawl.common.URLText;
import de.hd.cl.haas.distributedcrawl.common.WebDBURL;
import de.hd.cl.haas.distributedcrawl.common.WebDBURLList;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * This class is the Reducer part of the WebDBMerger job.
 * 
 * Its input are host names and lists of WebDB entries. The reducer
 * removes duplicates from the URL lists for a given host name while
 * preserving the freshest last-fetched time stamp.
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

        HashMap<String, Long> memory = new HashMap<String, Long>();
        for (WebDBURL u : values) {
            String mapKey = u.getURLText().toString();
            long currentDate = u.getValue().get();
            if (memory.containsKey(mapKey)) {
                long oldDate = memory.get(mapKey);

                if (currentDate > oldDate) {
                    memory.put(mapKey, currentDate);
                }

            } else {
                memory.put(mapKey, currentDate);
            }
        }

        // re-construct urllist
        HashSet<WebDBURL> temp = new HashSet<WebDBURL>();
        for (String s : memory.keySet()) {
            temp.add(new WebDBURL(new URLText(s), memory.get(s)));
        }
        WebDBURLList result = new WebDBURLList();
        result.fromCollection(temp);
        context.write(key, result);
    }
}
