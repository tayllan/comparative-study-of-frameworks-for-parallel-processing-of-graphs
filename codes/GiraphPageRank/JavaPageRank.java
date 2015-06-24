import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;

public class JavaPageRank extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

	private static final double DAMPING_FACTOR = 0.85;

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			vertex.setValue(new DoubleWritable(1.0d / getTotalNumVertices()));
		}
		else {
			double pageRankSum = 0;

			for (DoubleWritable message : messages) {
				pageRankSum += message.get();
			}

			double alpha = (1.0 - DAMPING_FACTOR) / this.getTotalNumVertices();
			vertex.setValue(new DoubleWritable(alpha + (pageRankSum * DAMPING_FACTOR)));
		}

		long edges = vertex.getNumEdges();
		this.sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() / edges));
	}

}
