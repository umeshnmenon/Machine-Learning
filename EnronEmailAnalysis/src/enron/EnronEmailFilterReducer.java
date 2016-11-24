package enron;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Description: EnronEmailFilterReducer just emits the filtered from, to, and timestamp from the mapper.
 * Filter: We will only pick the enron emails.
 *
 */
public class EnronEmailFilterReducer extends Reducer<Text,NullWritable,Text, NullWritable>{
	
	private Text from_to_wt=new Text();
	
	public void reduce(Text key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		//We are not worried about the values, because they will always be null and will always be 1
		context.write(key,NullWritable.get());
	}
}

