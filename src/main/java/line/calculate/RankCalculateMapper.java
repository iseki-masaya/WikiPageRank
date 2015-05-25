package line.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.commons.logging.*;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form
     *      `"[page]    [initialPagerank]    outLinkA,outLinkB,outLinkC..."`
     * and store a new key-value pair mapping linked pages to this page's name, rank and total number of links:
     *      `"[otherPage]   [thisPage]    [thisPagesRank]    [thisTotalNumberOfLinks]"
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param key the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer PageLinksParseReducer}
     * @param value the text value "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);

        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(page), new Text("!"));

        // Skip pages with no links.
        if(rankTabIndex == -1) return;

        String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
        String[] allOtherPages = links.split(",");
        int totalLinks = allOtherPages.length;

        for (String otherPage : allOtherPages){
            Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }

        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|" + links));
    }
}
