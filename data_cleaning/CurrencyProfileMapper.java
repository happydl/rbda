import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CurrencyProfileMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // skip header
        if (line.charAt(0) == 's')
            return;

        String[] list = line.split(",");

        String dateStr = list[1].substring(0, 4);

        context.write(new Text(dateStr), new IntWritable(1));

    }
}