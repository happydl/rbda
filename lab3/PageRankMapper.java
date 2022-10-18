import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        List<String> list = List.of(line.split("\\s"));
        // println(list);
        // PR / num_outgoing_edges
        double score = Double.parseDouble(list.get(list.size() - 1)) / (list.size() - 2);
        // println("PR: " + score);
        for (int i=1; i<list.size() - 1; i++) {
            context.write(new Text(list.get(0)), new Text(list.get(i)));
            context.write(new Text(list.get(i)), new Text("_" + String.valueOf(score))); // encode double
        }
    }
}