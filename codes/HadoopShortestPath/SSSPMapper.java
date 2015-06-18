import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SSSPMapper extends Mapper<Text, Text, LongWritable, Text> {

    @Override
    public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] aux = value.toString().split(" ");
        int length = aux.length;
	double nodeValue = Double.parseDouble(aux[0]);
        
        for (int i = 1; i < length; i++) {
	    String[] edge = aux[i].split(":");
	    String edgeToNode = edge[0];
	    double edgeValue = Double.parseDouble(edge[1]);
	    
            context.write(
                    new LongWritable(Long.parseLong(edgeToNode)),
                    new Text(String.valueOf(nodeValue + edgeValue))
            );
        }
        
        context.write(
                new LongWritable(Long.parseLong(key.toString())),
                new Text("#" + value.toString())
        );
    }
}