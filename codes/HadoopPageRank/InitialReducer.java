import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitialReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
     
    /**
     * 
     * @param key the node's ID
     * @param values the node's initial pagerank value and a list of all of its neighbors
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     * @writes simply its ID and its value
     */
    @Override
    public void reduce (LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}