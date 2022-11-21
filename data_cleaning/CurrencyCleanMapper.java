import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CurrencyCleanMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // skip header
        if (line.charAt(0) == 's')
            return;

        String[] list = line.split(",");

        StringBuilder sb = new StringBuilder;
        sb.append(list[0]); // slug
        sb.append(",")
        sb.append(list[1]); // date
        sb.append(",")
        sb.append(list[2]); // open
        sb.append(",")
        sb.append(list[5]); // close
        sb.append(",")
        sb.append(list[6]); // currency

        context.write(NullWritable.get(), new Text(sb.toString()));

    }
}