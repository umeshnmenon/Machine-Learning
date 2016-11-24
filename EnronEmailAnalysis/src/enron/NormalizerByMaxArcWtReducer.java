package enron;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import enron.SocialNetworkExtractor.NODE_ARC_TABLE;

/**
 * 
 * Description: EdgeWeightAssignerReducer assigns the weight to each node based on the number of 
 * connections from "from" to each "to". The weight is normalized by dividing the total no of connections
 * and then rescaled between 0 and 100
 *
 */
public class NormalizerByMaxArcWtReducer extends Reducer<Text,FloatWritable,Text, Text>{

	
	private Text weight=new Text();
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		float total_arcs=Float.parseFloat(context.getConfiguration().get(NODE_ARC_TABLE.TOTAL_NO_OF_ARCS.toString()));
		float max_wt=Float.parseFloat(context.getConfiguration().get(NODE_ARC_TABLE.MAX_ARC_WEIGHT.toString()));
		
		DecimalFormat df = new DecimalFormat("###.#####");
		for (FloatWritable wt: values){
			float wght=(wt.get() *total_arcs)/max_wt;
			weight.set(df.format(wght));
			context.write(key,weight);
		}

	}

}

