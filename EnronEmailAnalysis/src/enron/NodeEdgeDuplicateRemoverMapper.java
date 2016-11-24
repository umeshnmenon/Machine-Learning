package enron;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * Description: NodeEdgeDuplicateRemoverMapper reads the input file (output of Question 1) line by line
 * and emits a tab seperated value of from, to, and timestamp of an email message as key and null a value.
 * This way in reducer we can eliminate the dupicate from,to, and timestamp fields by just picking the
 * key only.
 *
 */

public class NodeEdgeDuplicateRemoverMapper extends Mapper<Object, Text, Text, NullWritable>{

	private Text from_to_ts=new Text();
		
	// The map method
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get a line of input
		if (value==null){
			return;
		}else{
			from_to_ts.set(value.toString());
			context.write(from_to_ts, NullWritable.get());
		}
	}
}
