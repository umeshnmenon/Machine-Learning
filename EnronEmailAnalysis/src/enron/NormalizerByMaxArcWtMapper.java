package enron;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * Description: NormalizerMapper reads the file produced by the Question 2, Part 2 and
 * emits (key, value) as (from_to, normalized_weight) of an email header
 *
 */
public class NormalizerByMaxArcWtMapper extends Mapper<Object, Text, Text, FloatWritable>{

	private Text from_to=new Text();
	private FloatWritable weight=new FloatWritable();

	// The map method
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get a line of input
		if (value==null){
			return;
		}else{
			String[] from_to_wt=value.toString().split(context.getConfiguration().get("mapreduce.output.textoutputformat.separator"));//split("\t");
			if (from_to_wt.length==3){
				from_to.set(from_to_wt[0] +context.getConfiguration().get("mapreduce.output.textoutputformat.separator")+from_to_wt[1]);
				weight.set(Float.parseFloat(from_to_wt[2]));
				context.write(from_to, weight);
			}else{
				return;
			}
		}
	}

}
