import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double nodeValue = Double.MAX_VALUE;
		String outerEdges = "";

		for (Text value : values) {
			String v = value.toString();

			if (v.charAt(0) == '#') {
				outerEdges = v.substring(1);
				nodeValue = Math.min(
					nodeValue,
					Double.parseDouble(outerEdges.split(" ")[0])
				);
			}
			else {
				nodeValue = Math.min(
					nodeValue,
					Double.parseDouble(v)
				);
			}
		}

		context.write(
			key,
			new Text(
				String.valueOf(nodeValue) + " " + outerEdges.substring(
					outerEdges.indexOf(' ') + 1
				)
			)
		);
	}

}
