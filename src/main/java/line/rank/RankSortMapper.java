package line.rank;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class RankSortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form:
     *      `"[page]    [finalPagerank]    outLinkA,outLinkB,outLinkC..."`
     * discard the outgoing links, parse the pagerank to a float and map each page to its rank.
     *
     * Note: The output from this Mapper will be sorted by the order of its keys.
     *
     * @param key the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.calculate.RankCalculateReducer RankCalculateReducer}
     * @param value the text value "[page]  [finalPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pageAndRank = getPageAndRank(key, value);

        float parseFloat = Float.parseFloat(pageAndRank[1]);

        Text page = new Text(pageAndRank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);

        context.write(rank, page);
    }

    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageAndRank = new String[2];
        int tabPageIndex = value.find("\t");
        int tabRankIndex = value.find("\t", tabPageIndex + 1);

        // no tab after rank (when there are no links)
        int end;
        if (tabRankIndex == -1) {
            end = value.getLength() - (tabPageIndex + 1);
        } else {
            end = tabRankIndex - (tabPageIndex + 1);
        }

        pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
        pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

        return pageAndRank;
    }
}
