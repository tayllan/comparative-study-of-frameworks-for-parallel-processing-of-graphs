import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitialMapper extends Mapper<Text, Text, LongWritable, Text> {

	/**
	 * @param key the node's ID
	 * @param value all the node's neighbors' IDs, separated by spaces
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @writes the node's ID, the node's initial pagerank value and all of its neighbors
	 */
	@Override
	public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
		int amountOfVertices = Integer.parseInt(context.getConfiguration().get("AMOUNT_OF_VERTICES"));

		context.write(
			new LongWritable(Long.parseLong(key.toString())),
			new Text((1.0 / (double)amountOfVertices) + " " + value.toString())
		);
	}

}