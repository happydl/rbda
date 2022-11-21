import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;

public class CurrencyClean {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CurrencyClean <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf);
        job.setJarByClass(CurrencyClean.class);

        job.setJobName("CurrencyClean");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(CurrencyCleanMapper.class);
        // job.setReducerClass(CurrencyCleanReducer.class);
        // job.setCombinerClass(PageRankReducer.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
