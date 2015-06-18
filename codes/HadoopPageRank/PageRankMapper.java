import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, LongWritable, Text> {
        
    /**
     * 
     * @param key the node's ID
     * @param value the node's pagerank value followed by a list of all of its neighbors
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @writes passes its current pagerank value to all of its neighbors
     */
    @Override
    public void map (Text key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] aux = value.toString().split(" ");
        int length = aux.length;
        double currentPageRank = Double.parseDouble(aux[0]);
        StringBuilder outerEdges = new StringBuilder();
        
        for (int i = 1; i < length; i++) {
            context.write(
                    new LongWritable(Long.parseLong(aux[i])),
                    new Text(String.valueOf(currentPageRank / (double)(length - 1)))
            );
            
            outerEdges.append(aux[i]).append(' ');
        }
        
        context.write(
                new LongWritable(Long.parseLong(key.toString())),
                new Text(outerEdges.toString().trim())
        );
    }
}