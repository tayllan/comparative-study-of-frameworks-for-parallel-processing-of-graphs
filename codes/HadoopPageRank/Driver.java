import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
		System.out.println("Beginning PageRank Hadoop job");
		long start = System.nanoTime();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		System.out.println(dateFormat.format(new Date()));

		if (args.length < 3) {
			System.out.println("HadoopPageRank Usage: <number_of_vertices> <number_of_iterations> <input_path> [output_path]");
			System.exit(0);
		}

			int amountOfVertices = Integer.parseInt(args[0]);
			int numberOfIterations = Integer.parseInt(args[1]);
			String inputPath = args[2];
			String outputPath = "output_hadoop";

		if (args.length == 4) {
			outputPath = args[3];
		}

		System.out.println("Amount of Vertices: " + amountOfVertices);
		System.out.println("Number of Iterations: " + numberOfIterations);
		System.out.println("Input Path: " + inputPath);
		System.out.println("Initial Output Path: " + outputPath);

		System.out.println("Initial Job");
		Driver.startInitialJob(inputPath, outputPath + "0", amountOfVertices);

		for (int i = 0; i < numberOfIterations; i++) {
			System.out.println("PageRank Job #" + (i + 1));

			Driver.startPageRankJob(
					outputPath + i + "/part-r-*",
					outputPath + (i + 1) + "/",
					amountOfVertices
			);

			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(outputPath + i), true);
		}

		double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
		System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
	}

	/**
	 * Reads the initial Adjacency List and sets the initial PageRank values, i.e. (1 / amountOfOuterEdges).
	 *
	 * @param inputPath
	 * @param outputPath
	 * @param amountOfVertices
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void startInitialJob(String inputPath, String outputPath, int amountOfVertices) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		conf.set("AMOUNT_OF_VERTICES", String.valueOf(amountOfVertices));

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

	/**
	 * Reads the Adjacency List and the PageRank value of each Vertex and updates it.
	 *
	 * @param inputPath
	 * @param outputPath
	 * @param amountOfVertices
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void startPageRankJob(String inputPath, String outputPath, int amountOfVertices) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		conf.set("AMOUNT_OF_VERTICES", String.valueOf(amountOfVertices));

		Job job = new Job(conf, "PageRank Job");

		job.setJarByClass(Driver.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

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
