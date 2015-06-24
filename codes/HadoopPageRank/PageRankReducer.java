
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	/**
	 * @param key the node's ID
	 * @param values one of the values is the list of all the node's neighbors, the others are the pagerank values passed by its neighbors
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 * @writes the node's ID, the node's new pagerank value and a list with its neighbors
	 */
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double sum = 0.0;
		String[] outerEdges = new String[0];

		for (Text value : values) {
			String v = value.toString().trim();
			String[] temp = v.split(" ");

			if (temp.length == 0 || temp.length > 1) {
				outerEdges = temp;
			}
			else if (v.contains(".")) {
				sum += Double.parseDouble(v);
			}
			else {
				outerEdges = temp;
			}
		}

		int amountOfVertices = Integer.parseInt(context.getConfiguration().get("AMOUNT_OF_VERTICES"));
		double vertexValue = (0.15 / amountOfVertices) + (0.85 * sum);
		StringBuilder resultingValue = new StringBuilder();

		resultingValue.append(vertexValue);

		for (String outerEdge : outerEdges) {
			resultingValue.append(' ').append(outerEdge);
		}

		context.write(key, new Text(resultingValue.toString().trim()));
	}

}
