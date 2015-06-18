
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class ShortestPath extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    public static final long SOURCE_ID = 1l;

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
	if (getSuperstep() == 0) {
	    vertex.setValue(new DoubleWritable(Integer.MAX_VALUE));
	}
	double minDist = (vertex.getId().get() == 1) ? 0d : Integer.MAX_VALUE;
	for (DoubleWritable message : messages) {
	    if (message.get() < minDist) {
		minDist = message.get();
	    }
	}
	if ((int)minDist < (int)vertex.getValue().get()) {
	    vertex.setValue(new DoubleWritable(minDist));
	    
	    for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
		double distance = minDist + edge.getValue().get();

		this.sendMessage(
		    edge.getTargetVertexId(),
		    new DoubleWritable(distance)
		);
	    }
	}
	vertex.voteToHalt();
    }
}
