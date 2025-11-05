// By WXD
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RankingDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: RankingDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Global Ranking");

        job.setJarByClass(RankingDriver.class);
        job.setMapperClass(RankingMapper.class);
        job.setReducerClass(RankingReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1); // ⚠️ 关键！必须设为1才能全局排序！

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("成功");
    }
}