
import com.google.common.collect.Iterables;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

public final class JavaPageRank {

    private static double NUMBER_OF_VERTICES = 0.0;

    private static class Sum implements Function2<Double, Double, Double> {

	@Override
	public Double call(Double a, Double b) {
	    return a + b;
	}
    }

    public static void main(String[] args) throws Exception {
	if (args.length < 3) {
	    System.err.println("SparkPageRank Usage: <input_path> <number_of_iterations> <number_of_vertices> [true for cache]");
	    System.exit(-1);
	}

	System.out.println("Beginning PageRank Spark job");
	long start = System.nanoTime();
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	System.out.println(dateFormat.format(new Date()));

	NUMBER_OF_VERTICES = Double.parseDouble(args[2]);

	SparkConf sparkConf = new SparkConf().setAppName("HamaPageRank");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	JavaRDD<String> lines = ctx.textFile("hdfs://localhost:54310/user/hduser/" + args[0], 1);

	JavaPairRDD<String, Iterable<String>> links = lines.flatMapToPair(
	    new PairFlatMapFunction<String, String, String>() {
		@Override
		public Iterable<Tuple2<String, String>> call(String s) {
		    List<Tuple2<String, String>> pairs = new LinkedList<>();
		    String[] aux = s.split("\t");

		    for (String neighboor : aux[1].split(" ")) {
			pairs.add(new Tuple2<>(aux[0], neighboor));
		    }

		    return pairs;
		}
	    }
	).distinct().groupByKey();

	if (args.length >= 4 && args[3].equals("true")) {
	    links = links.cache();
	}

	JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
	    @Override
	    public Double call(Iterable<String> rs) {
		return 1.0 / NUMBER_OF_VERTICES;
	    }
	});

	for (int current = 0; current < Integer.parseInt(args[1]); current++) {
	    JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(
		new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
		    @Override
		    public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
			int urlCount = Iterables.size(s._1);
			List<Tuple2<String, Double>> results = new ArrayList<>();
			for (String n : s._1) {
			    results.add(new Tuple2<>(n, s._2() / urlCount));
			}
			return results;
		    }
		}
	    );

	    ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
		@Override
		public Double call(Double sum) {
		    return (0.15 / JavaPageRank.NUMBER_OF_VERTICES) + (sum * 0.85);
		}
	    });
	}

	List<Tuple2<String, Double>> result = ranks.collect();

	Collections.sort(result, new Comparator<Tuple2<String, Double>>() {
	    @Override
	    public int compare(Tuple2<String, Double> s1, Tuple2<String, Double> s2) {
		int key1 = Integer.parseInt(s1._1);
		int key2 = Integer.parseInt(s2._1);

		return Integer.compare(key1, key2);
	    }
	});

	for (Tuple2<String, Double> tuple : result) {
	    System.out.println(tuple._1() + "\t" + tuple._2());
	}

	ctx.stop();

	double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
        System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
    }

}
