import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrandchildFinder {

    // -------- Mapper --------
    // 输入格式固定为：child parent
    public static class ChildParentMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.toLowerCase().startsWith("child")) return; // 忽略表头

            String[] parts = line.split("\\s+");
            if (parts.length != 2) return;

            String child = parts[0];
            String parent = parts[1];

            // child -> parent
            outKey.set(child);
            outVal.set("PARENT:" + parent);
            context.write(outKey, outVal);

            // parent -> child
            outKey.set(parent);
            outVal.set("CHILD:" + child);
            context.write(outKey, outVal);
        }
    }

    // -------- Reducer --------
    public static class GrandchildReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(new Text("grandchild"), new Text("grandparent"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> parents = new HashSet<>();
            Set<String> children = new HashSet<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("PARENT:"))
                    parents.add(s.substring(7));
                else if (s.startsWith("CHILD:"))
                    children.add(s.substring(6));
            }

            // 输出祖孙对
            for (String c : children) {
                for (String p : parents) {
                    outKey.set(c);
                    outVal.set(p);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    // -------- main --------
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GrandchildFinder <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        Job job = Job.getInstance(conf, "Grandchild Finder");
        job.setJarByClass(GrandchildFinder.class);

        job.setMapperClass(ChildParentMapper.class);
        job.setReducerClass(GrandchildReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
