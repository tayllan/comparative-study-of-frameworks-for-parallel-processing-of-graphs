import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		long start = System.nanoTime();
		System.out.println("Beginning SSSP Hadoop job");
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		System.out.println(dateFormat.format(new Date()));

		if (args.length < 2) {
			System.out.println("HadoopSSSP Usage: <input_path> <source_node> [output_path]");
			System.exit(0);
		}

		String inputPath = args[0];
		String sourceNode = args[1];
		String outputPath = "output_hadoop";

		if (args.length == 3) {
			outputPath = args[2];
		}

		System.out.println("Input Path: " + inputPath);
		System.out.println("Source Node: " + sourceNode);
		System.out.println("Initial Output Path: " + outputPath);

		System.out.println("Initial Job");
		boolean hasEnded = true;
		int i = 0;
		Driver.startInitialJob(inputPath, outputPath + i, sourceNode);
		HashMap<Long, Double> values = new HashMap<>();

		do {
			System.out.println("SSSP Job #" + (i + 1));
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(
				new InputStreamReader(
					fs.open(
						new Path(outputPath + i + "/part-r-00000")
					)
				)
			);

			hasEnded = true;
			String line;
			while ((line = br.readLine()) != null) {
				String[] temp = line.split("\t");
				long key = Long.parseLong(temp[0]);
				double value = Double.parseDouble(temp[1].split(" ")[0]);

				if (!(values.containsKey(key) && values.get(key) == value)) {
					hasEnded = false;
				}

				values.put(key, value);
			}

			Driver.startSSSPJob(
				outputPath + i + "/part-r-*",
				outputPath + (i + 1) + "/"
			);

			fs.delete(new Path(outputPath + i), true);
			i += 1;
		} while(!hasEnded);

		double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
		System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
	}

	private static void startInitialJob(String inputPath, String outputPath, String sourceNode) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		conf.set("SOURCE_NODE", sourceNode);

		Job job = new Job(conf, "Initial Job");

		job.setJarByClass(Driver.class);
		job.setMapperClass(InitialMapper.class);
		job.setReducerClass(InitialReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	private static void startSSSPJob(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SSSP Job");

		job.setJarByClass(Driver.class);
		job.setMapperClass(SSSPMapper.class);
		job.setReducerClass(SSSPReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

}
