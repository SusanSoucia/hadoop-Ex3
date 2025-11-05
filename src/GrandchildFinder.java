// By WCWZ
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
    // 支持两种常见输入行格式：
    //  A) child parent1 parent2 ...
    //  B) child1 parent1 child2 parent2 ... (成对序列)
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
            if (parts.length < 2) return;

            // Heuristic: 如果 parts 长度 >=4 并且为偶数长度，则可能是成对序列 child1 parent1 child2 parent2 ...
            boolean tryPairs = (parts.length >= 4) && (parts.length % 2 == 0);

            if (tryPairs) {
                // 进一步判断：如果把它当成成对序列更合理（例如 tokens[1] 与 tokens[3] 不相同），则按成对处理
                boolean looksLikePairs = true;
                for (int i = 0; i < parts.length; i += 2) {
                    if (i + 1 >= parts.length || parts[i].equals(parts[i+1])) {
                        looksLikePairs = false;
                        break;
                    }
                }
                if (looksLikePairs) {
                    // 按对解析
                    for (int i = 0; i < parts.length; i += 2) {
                        String child = parts[i];
                        String parent = parts[i+1];
                        // 输出 (child, PARENT:parent) 与 (parent, CHILD:child)
                        outKey.set(child);
                        outVal.set("PARENT:" + parent);
                        context.write(outKey, outVal);

                        outKey.set(parent);
                        outVal.set("CHILD:" + child);
                        context.write(outKey, outVal);
                    }
                    return;
                }
            }

            // 否则按 "第一个是 child，其余全是 parent" 处理
            String child = parts[0];
            for (int i = 1; i < parts.length; i++) {
                String parent = parts[i];
                outKey.set(child);
                outVal.set("PARENT:" + parent);
                context.write(outKey, outVal);

                outKey.set(parent);
                outVal.set("CHILD:" + child);
                context.write(outKey, outVal);
            }
        }
    }

    // -------- Reducer --------
    // 对每个 key（节点B），收集 parents (B 的父母) 与 children (B 的孩子)，然后输出 (child, grandparent)
    public static class GrandchildReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 在所有 reduce 调用之前写入表头（只有一个 reducer 时，表头只会出现一次且位于文件首行）
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

            // 产生祖孙 (child, grandparent)
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
        // 如需指定 NameNode，可解除下一行注释并设置为你的 hdfs 地址
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        Job job = Job.getInstance(conf, "Grandchild Finder");
        job.setNumReduceTasks(1);
        job.setJarByClass(GrandchildFinder.class);
        job.setMapperClass(ChildParentMapper.class);
        job.setReducerClass(GrandchildReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
