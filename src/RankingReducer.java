// By WXD
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankingReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private int rank = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        rank++;
        context.write(new IntWritable(rank), key);
    }
}