
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class PageRank {

    public static class PageRankVertex extends Vertex<DoubleWritable, NullWritable, DoubleWritable> {

	private static final double DAMPING_FACTOR = 0.85;
	
	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
	    if (this.getSuperstepCount() == 0) {
		this.setValue(new DoubleWritable(1.0 / this.getNumVertices()));
	    }
	    else {
		double pageRankSum = 0;

		for (DoubleWritable message : messages) {
		    pageRankSum += message.get();
		}

		double alpha = (1.0 - DAMPING_FACTOR) / this.getNumVertices();
		setValue(new DoubleWritable(alpha + (pageRankSum * DAMPING_FACTOR)));
		// O que é isso: aggregate(0, this.getValue());
	    }

	    long edges = this.getEdges().size();

	    this.sendMessageToNeighbors(new DoubleWritable(this.getValue().get() / edges));
	}
    }

    public static class PagerankSeqReader
	extends VertexInputReader<LongWritable, Text, IntWritable, NullWritable, DoubleWritable> {

	@Override
	public boolean parseVertex(
		LongWritable key, Text value, Vertex<IntWritable, NullWritable, DoubleWritable> vertex) {
	    String[] split = value.toString().split("\t");

	    vertex.setVertexID(new IntWritable(Integer.parseInt((split[0]))));

	    String[] aux = split[1].split(" ");

	    for (String aux1 : aux) {
		vertex.addEdge(
		    new Edge<IntWritable, NullWritable>(
			new IntWritable(Integer.parseInt((aux1))),
			null
		    )
		);
	    }
	    return true;
	}
    }

    public static GraphJob createJob(String[] args, HamaConfiguration conf) throws IOException {
	GraphJob pageJob = new GraphJob(conf, PageRank.class);
	pageJob.setJobName("Pagerank");
	pageJob.setVertexClass(PageRankVertex.class);
	pageJob.setInputPath(new Path(args[0]));
	pageJob.setOutputPath(new Path(args[1]));

	pageJob.setMaxIteration(Integer.parseInt(args[2]));

	if (args.length == 4) {
	    pageJob.setNumBspTask(Integer.parseInt(args[3]));
	}

	pageJob.setVertexIDClass(IntWritable.class);
	pageJob.setVertexValueClass(DoubleWritable.class);
	pageJob.setEdgeValueClass(NullWritable.class);
	pageJob.setInputKeyClass(LongWritable.class);
	pageJob.setInputValueClass(Text.class);
	pageJob.setInputFormat(TextInputFormat.class);
	pageJob.setVertexInputReaderClass(PagerankSeqReader.class);
	pageJob.setPartitioner(HashPartitioner.class);
	pageJob.setOutputFormat(TextOutputFormat.class);
	pageJob.setOutputKeyClass(Text.class);
	pageJob.setOutputValueClass(Text.class);

	return pageJob;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	if (args.length < 3) {
	    System.out.println("HamaPageRank Usage: <input_path> <output_path> <number_of_iterations> [numberOfThreads (probably)]");
	    System.exit(-1);
	}

	System.out.println("Beginning PageRank Hama job");
	long start = System.nanoTime();
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	System.out.println(dateFormat.format(new Date()));
	
	HamaConfiguration conf = new HamaConfiguration();
	GraphJob pageJob = createJob(args, conf);

	if (pageJob.waitForCompletion(true)) {
	    double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
	    System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
	}
    }
}
