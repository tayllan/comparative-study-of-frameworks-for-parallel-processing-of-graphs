import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MeuGiraphRunner implements Tool {

	private static int MAX_NUMBER_SUPERSTEPS;

	static {
		Configuration.addDefaultResource("giraph-site.xml");
	}

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (null == getConf()) {
			conf = new Configuration();
		}
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);

		if (null == cmd) {
			return 0;
		}
		final String vertexClassName = args[0];
		final String jobName = "Giraph: " + vertexClassName;

		giraphConf.setMaxNumberOfSupersteps(MAX_NUMBER_SUPERSTEPS);

		GiraphJob job = new GiraphJob(giraphConf, jobName);
		prepareHadoopMRJob(job, cmd);
		boolean verbose = !cmd.hasOption('q');

		return job.run(verbose) ? 0 : -1;
	}

	private void prepareHadoopMRJob(final GiraphJob job, final CommandLine cmd) throws Exception {
		if (cmd.hasOption("op")) {
			FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue("op")));
		}
		if (cmd.hasOption("cf")) {
			DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")), job.getConfiguration());
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("GiraphPageRank Usage: <input_path> <output_path> <number_of_iterations> [number_of_workers (default 1)]");
			System.exit(-1);
		}

		MAX_NUMBER_SUPERSTEPS = Integer.parseInt(args[2]);

		String numberOfWorkers = "1";

		if (args.length == 4) {
			numberOfWorkers = args[3];
		}

		String[] newArgs = {
			"-libjars",
			"~/GiraphPageRank/dist/GiraphPageRank.jar",
			"JavaPageRank",
			"-vif",
			"org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat",
			"-vip",
			args[0],
			"-vof",
			"org.apache.giraph.io.formats.IdWithValueTextOutputFormat",
			"-op",
			args[1],
			"-w",
			numberOfWorkers
		};

		System.out.println("Beginning PageRank Giraph job");
		long start = System.nanoTime();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		System.out.println(dateFormat.format(new Date()));

		ToolRunner.run(new MeuGiraphRunner(), newArgs);

		double elapsedTimeInSec = (System.nanoTime() - start) * 1.0e-9;
		System.out.println("<" + elapsedTimeInSec + "s> " + dateFormat.format(new Date()));
	}

}
