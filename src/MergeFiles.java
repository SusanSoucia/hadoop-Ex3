// By ZZY
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MergeFiles {

    // Mapper 类
    public static class MergeFilesMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            // 跳过空行
            if (line.isEmpty()) {
                return;
            }
            
            // 按空格分割每行，第一个字段作为key，剩余部分作为value
            String[] parts = line.split("\\s+", 2);
            
            if (parts.length >= 2) {
                outputKey.set(parts[0]);  // 日期作为key
                outputValue.set(parts[1]); // 字母作为value
                context.write(outputKey, outputValue);
            }
        }
    }

    // Reducer 类
    public static class MergeFilesReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text outputValue = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            Set<String> uniqueValues = new HashSet<>();
            
            // 收集所有不重复的值
            for (Text value : values) {
                uniqueValues.add(value.toString());
            }
            
            // 为每个唯一值输出一行
            for (String uniqueValue : uniqueValues) {
                outputValue.set(uniqueValue);
                context.write(key, outputValue);
            }
        }
    }

    // Driver 主方法
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MergeFiles <input path A> <input path B> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Merge and Remove Duplicates");
        
        job.setJarByClass(MergeFiles.class);
        
        // 设置Mapper和Reducer类
        job.setMapperClass(MergeFilesMapper.class);
        job.setReducerClass(MergeFilesReducer.class);
        
        // 设置输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // 设置输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // 文件A路径
        FileInputFormat.addInputPath(job, new Path(args[1])); // 文件B路径
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // 输出路径C
        
        // 等待作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}