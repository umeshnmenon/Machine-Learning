package enron;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * Description: EnronEmailFilterMapper just emits the filtered from, to, and timestamp.
 * Filter: We will only pick the enron emails.
 *
 */
public class EnronEmailFilterMapper extends Mapper<Object, Text, Text, NullWritable>{

	private Text filtered_from_to_wt=new Text();

	// The map method
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get a line of input
		if (value==null || value.toString()==null){
			return;
		}else{
			String[] from_to_wt=value.toString().split("\t");//split(context.getConfiguration().get("mapreduce.output.textoutputformat.separator"));
			if (from_to_wt.length==3){
				//Let's filter only enron emails
				//if (from_to_wt[0].toLowerCase().contains("@enron.com") && from_to_wt[1].toLowerCase().contains("@enron.com")){
				if (from_to_wt[0].toLowerCase().contains("@enron.com")){
					//filtered_from_to_wt.set(from_to_wt[0] +context.getConfiguration().get("mapreduce.output.textoutputformat.separator")+
					//		from_to_wt[1] + context.getConfiguration().get("mapreduce.output.textoutputformat.separator")+from_to_wt[2]);
					filtered_from_to_wt.set(from_to_wt[0] +"\t"+
							from_to_wt[1] + "\t" +from_to_wt[2]);
					context.write(filtered_from_to_wt, NullWritable.get());
				}
			}else{
				return;
			}
		}
	}

}
