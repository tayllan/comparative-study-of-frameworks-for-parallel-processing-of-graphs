import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitialMapper extends Mapper<Text, Text, LongWritable, Text> {

	@Override
	public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
		String sourceNode = context.getConfiguration().get("SOURCE_NODE");

		if (key.toString().equals(sourceNode)) {
			context.write(
				new LongWritable(Long.parseLong(key.toString())),
				new Text("0.0 " + value.toString())
			);
		}
		else {
			context.write(
				new LongWritable(Long.parseLong(key.toString())),
				new Text(String.valueOf((double)Integer.MAX_VALUE) + " " + value.toString())
			);
		}
	}

}