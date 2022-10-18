import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
        extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        Double pr = 0d;
        String edgeStr = "";

        for (Text value : values) {
            String strValue = value.toString();
            // pr
            if (strValue.indexOf("_") != -1) {
                pr += Double.parseDouble(strValue.substring(1));
            } else {
                if (edgeStr.length() > 0) {
                    edgeStr += " ";
                }
                edgeStr += strValue;
            }
        }
        if (edgeStr.length() > 0) {
            edgeStr += " ";
        }
        edgeStr += String.valueOf(pr);
        
        context.write(key, new Text(edgeStr));
    }
}