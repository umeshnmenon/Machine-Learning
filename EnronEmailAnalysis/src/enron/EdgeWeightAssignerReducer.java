package enron;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import enron.SocialNetworkExtractor.NODE_ARC_TABLE;

/**
 * 
 * Description: EdgeWeightAssignerReducer assigns the weight to each node based on the number of 
 * connections from "from" to each "to". The weight is normalized by dividing the total no of connections
 * and then rescaled between 0 and 100
 *
 */
public class EdgeWeightAssignerReducer extends Reducer<Text,Text,Text, Text>{

	private Text from_to=new Text();
	private Text weight=new Text();
	
	
	//private float max_wt=0;
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		
		HashMap<String, Integer> weights=new HashMap<String, Integer>();
		float total=0; //making this only float for weight calculation
		float total_arcs=Float.parseFloat(context.getConfiguration().get(NODE_ARC_TABLE.TOTAL_NO_OF_ARCS.toString()));
		for(Text to:values){
			Integer cnt=weights.get(to.toString());
			weights.put(to.toString(), cnt==null?1:cnt+1);
			total++;
		}
		
		
		//to get the max weight. I know this is ugly and counter is mainly meant for incremented value but then we
		//have to refer to distributed cache for global sharing
		if (total>context.getCounter(NODE_ARC_TABLE.MAX_ARC_WEIGHT).getValue()) 
			context.getCounter(NODE_ARC_TABLE.MAX_ARC_WEIGHT).setValue((long) total);
		//if (total>max_wt) max_wt=total;
		DecimalFormat df = new DecimalFormat("###.#####");

		for(Map.Entry entry: weights.entrySet()){
			//from_to.set(key.toString() +"\t"+entry.getKey().toString());
			from_to.set(key.toString() +context.getConfiguration().get("mapreduce.output.textoutputformat.separator")+entry.getKey().toString());
			float wt=(float) ((int)entry.getValue()/total_arcs);
			wt=wt*100; //rescaling 
			//weight.set(Float.toString(wt));
			weight.set(df.format(wt));
			context.write(from_to,weight);
		}
	}
	
	// The cleanup method. Anything in here will be run exactly once after the
	// end of the map task.
	public void cleanup(Context context) throws IOException,
	InterruptedException {
		// Note: you can use Context methods to emit records in the setup and cleanup as well.
		//DistributedCache.addCacheArchive(new URI("maxwt"), context.getConfiguration());
	
		
	}

	
}
