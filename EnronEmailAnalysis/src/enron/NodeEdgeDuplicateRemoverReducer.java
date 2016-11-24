package enron;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import enron.SocialNetworkExtractor.NODE_ARC_TABLE;
/**
 * 
 * Description: NodeEdgeDuplicateRemoverReducer gets the input; a tab separated string of from, to, 
 * and timestamp from Mapper and in the reduce() it just picks the key, thus avoiding duplicates
 *
 */
public class NodeEdgeDuplicateRemoverReducer extends Reducer<Text, NullWritable, NullWritable, Text>{

	public void reduce(Text key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException{
		//REMOVING DUPLICATES
		//Here key will be the tab separated values of from, to, and timestamp of an email
		//and values is an array of null. So we take only the key to avoid duplicates
		if (key==null){
			return;
		}else{
			if (key.toString()!="") 	context.write(NullWritable.get(), key);
		}
	}

	// The cleanup method. Anything in here will be run exactly once after the
	// end of the map task.
	public void cleanup(Context context) throws IOException,
	InterruptedException {
		// Note: you can use Context methods to emit records in the setup and cleanup as well.
		//context.getCounter(NODE_ARC_TABLE.TOTAL_NO_OF_ARCS).setValue(context.getCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue());
	}
}
