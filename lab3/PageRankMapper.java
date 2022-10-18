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


        // String s = "A B C D E 0.5";
        // String[] list = s.split("\\s");
        // println(list);
        // // outgoing edges
        // double score = Double.parseDouble(list[list.length - 1]) / (list.length - 2);
        // println("PR: " + score);
        // for (int i=1; i<list.length - 1; i++) {
        //     println(list[0] + ", " + list[i]);
        //     println(list[i] + "_" + String.valueOf(score));
        // }


        String line = value.toString();
        String[] list = s.split("\\s");
        // println(list);
        // PR / num_outgoing_edges
        double score = Double.parseDouble(list[list.length - 1]) / (list.length - 2);
        // println("PR: " + score);
        for (int i=1; i<list.length - 1; i++) {
            context.write(new Text(list[0]), new Text(list[i]));
            context.write(new Text(list[i]), new Text("_" + String.valueOf(score))); // encode double
        }
    }
}