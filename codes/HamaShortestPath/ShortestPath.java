import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class ShortestPath {

	public static final String START_VERTEX = "shortest.paths.start.vertex.name";

	public static class ShortestPathVertex extends Vertex<Text, IntWritable, IntWritable> {
		public boolean isStartVertex() {
			Text startVertex = new Text(getConf().get(START_VERTEX));
			return (this.getVertexID().equals(startVertex));
		}

		@Override
		public void compute(Iterable<IntWritable> messages) throws IOException {
			int minDist = isStartVertex() ? 0 : Integer.MAX_VALUE;
			for (IntWritable msg : messages) {
				if (msg.get() < minDist) {
					minDist = msg.get();
				}
			}
			if (minDist < this.getValue().get()) {
				this.setValue(new IntWritable(minDist));
				for (Edge<Text, IntWritable> e : this.getEdges()) {
					sendMessage(e, new IntWritable(minDist + e.getValue().get()));
				}
			}
			voteToHalt();
		}
	}

	public static class ShortestPathTextReader extends VertexInputReader<LongWritable, Text, Text, IntWritable, IntWritable> {
		@Override
		public boolean parseVertex(LongWritable key, Text value, Vertex<Text, IntWritable, IntWritable> vertex) throws Exception {
			String[] aux = value.toString().split("\t");
			String[] split = aux[1].split(" ");

			vertex.setVertexID(new Text(aux[0]));
			vertex.setValue(new IntWritable(Integer.MAX_VALUE));

			for (String split1 : split) {
				String[] split2 = split1.split(":");
				vertex.addEdge(
					new Edge<>(
						new Text(split2[0]),
						new IntWritable(Integer.parseInt(split2[1]))
					)
				);
			}
			return true;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length < 3) {
			System.out.println("HamaSSSP Usage: <start_node> <input_path> <output_path> [numberOfThreads (probably)]");
			System.exit(-1);
		}

		System.out.println("Beginning SSSP Hama job");
		long start = System.nanoTime();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		System.out.println(dateFormat.format(new Date()));

		HamaConfiguration conf = new HamaConfiguration();
		GraphJob ssspJob = new GraphJob(conf, ShortestPath.class);
		ssspJob.setJobName("Single Source Shortest Path");
		conf.set(START_VERTEX, args[0]);
		ssspJob.setInputPath(new Path(args[1]));
		ssspJob.setOutputPath(new Path(args[2]));

		if (args.length == 4) {
			ssspJob.setNumBspTask(Integer.parseInt(args[3]));
		}

		ssspJob.setVertexClass(ShortestPathVertex.class);
		ssspJob.setInputFormat(TextInputFormat.class);
		ssspJob.setInputKeyClass(LongWritable.class);
		ssspJob.setInputValueClass(Text.class);
		ssspJob.setPartitioner(HashPartitioner.class);
		ssspJob.setOutputFormat(TextOutputFormat.class);
		ssspJob.setVertexInputReaderClass(ShortestPathTextReader.class);
		ssspJob.setOutputKeyClass(Text.class);
		ssspJob.setOutputValueClass(IntWritable.class);

		// Iterate until all the nodes have been reached.
		ssspJob.setMaxIteration(Integer.MAX_VALUE);
		ssspJob.setVertexIDClass(Text.class);
		ssspJob.setVertexValueClass(IntWritable.class);
		ssspJob.setEdgeValueClass(IntWritable.class);

		if (ssspJob.waitForCompletion(true)) {
			double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
			System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
		}
	}

}
