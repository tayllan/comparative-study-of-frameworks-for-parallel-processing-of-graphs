
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public final class ShortestPath {

    public static String startNode = "";

    private static class MinValue implements Function2<String, String, String> {
	@Override
	public String call(String t1, String t2) throws Exception {
	    String[] temp1 = t1.split(" ");
	    String[] temp2 = t2.split(" ");
	    
	    if (temp1.length == 1 && temp2.length == 1) {
		return String.valueOf(
		    Math.min(
			Double.parseDouble(t1),
			Double.parseDouble(t2)
		    )
		);
	    }
	    else if (temp1.length == 1) {
		double oldValue = Double.parseDouble(temp2[0]);
		double newValue = Double.parseDouble(t1);
		
		if (oldValue < newValue) {
		    return t2;
		}
		else {
		    return t1 + t2.substring(t2.indexOf(' '));
		}
	    }
	    else {
		double oldValue = Double.parseDouble(temp1[0]);
		double newValue = Double.parseDouble(t2);
		
		if (oldValue < newValue) {
		    return t1;
		}
		else {
		    return t2 + t1.substring(t1.indexOf(' '));
		}
	    }
	}
    }
    
    private static class FoldFunction implements Function2<String, String, String> {
	@Override
	public String call(String t1, String t2) throws Exception {
	    t1 = t1.trim();
	    t2 = t2.trim();
	    try {
		double aux = Double.parseDouble(t2);
		
		return t2 + " " + t1;
	    }
	    catch (NumberFormatException ex) {
		return t1 + " " + t2;
	    }
	}
    }

    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    System.err.println("SparkPageRank Usage: <input_path> <start_node>");
	    System.exit(-1);
	}

	System.out.println("Beginning SSSP Spark job");
	long start = System.nanoTime();
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	System.out.println(dateFormat.format(new Date()));

	SparkConf sparkConf = new SparkConf().setAppName("SparkSSSP").set("spark.executor.memory", "6g");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	//JavaRDD<String> lines = ctx.textFile("hdfs://localhost:54310/user/hduser/" + args[0], 1);
	JavaRDD<String> lines = ctx.textFile("hdfs://brutus00:54310/user/hduser/" + args[0], 1);
	ShortestPath.startNode = args[1];

	JavaPairRDD<String, String> nodes = lines.flatMapToPair(
	    new PairFlatMapFunction<String, String, String>() {
		@Override
		public Iterable<Tuple2<String, String>> call(String s) {
		    ArrayList<Tuple2<String, String>> pairs = new ArrayList<>();
		    String[] aux = s.split("\t");
		    String fromNode = aux[0];
		    String[] edges = aux[1].split(" ");

		    if (fromNode.equals(ShortestPath.startNode)) {
			pairs.add(new Tuple2<>(fromNode, "0.0"));
		    }
		    else {
			pairs.add(new Tuple2<>(
			    fromNode,
			    String.valueOf((double) Integer.MAX_VALUE)
			));
		    }

		    for (String edge : edges) {
			pairs.add(new Tuple2<>(fromNode, edge));
		    }

		    return pairs;
		}
	    }
	).foldByKey("", new FoldFunction()).persist(StorageLevel.DISK_ONLY());

	HashMap<String, Double> values = new HashMap<>();
	boolean hasEnded = true;
	int i = 0;

	do {
	    System.out.println("SSSP Iteration #" + (++i));
	    nodes = nodes.flatMapToPair(
		new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
		    @Override
		    public Iterable<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
			String[] temp = t._2.split(" ");
			int tempLength = temp.length;
			double nodeLocalValue = Double.parseDouble(temp[0]);
			ArrayList<Tuple2<String, String>> pairs = new ArrayList<>();
			
			for (int i = 1; i < tempLength; i++) {
			    String[] temp2 = temp[i].split(":");
			    String toNode = temp2[0];
			    double toValue = Double.parseDouble(temp2[1]);
			    
			    if (!t._1.equals(toNode)) {
				pairs.add(new Tuple2<>(
				    toNode,
				    String.valueOf(nodeLocalValue + toValue)
				));
			    }
			}
			
			pairs.add(new Tuple2<>(
			    t._1,
			    t._2
			));
			
			return pairs;
		    }
		}
	    ).reduceByKey(new MinValue()).persist(StorageLevel.DISK_ONLY());
	    
	    hasEnded = true;
	    for (Tuple2<String, String> tuple : nodes.collect()) {
		String key = tuple._1;
		double value = Double.parseDouble(tuple._2.split(" ")[0]);
		
		if (!(values.containsKey(key) && values.get(key) == value)) {
		    hasEnded = false;
		}
		values.put(key, value);
	    }
	} while (!hasEnded);

	System.out.println("THE SPARK HAS ENDED");
	System.out.println("EXIBINDO NODES");
	for (Tuple2<String, String> tuple : nodes.collect()) {
	    System.out.println(tuple._1 + "\t" + tuple._2.split(" ")[0]);
	}
	System.out.println("FIM EXIBINDO NODES");
	ctx.stop();

	double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
	System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
    }

}
