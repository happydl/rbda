import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;

public class PageRank {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("mapreduce.textoutputformat.separator", " ");
//        Job job = new Job();
        // Job job = new Job();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRank.class);
        job.setJobName("Max temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        // job.setCombinerClass(PageRankReducer.class);
        job.setNumReduceTasks(1); //
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
