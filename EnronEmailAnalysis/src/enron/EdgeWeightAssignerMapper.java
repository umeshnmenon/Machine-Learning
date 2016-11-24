package enron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import enron.SocialNetworkExtractor.NODE_ARC_TABLE;
/**
 * 
 * Description: EdgeWeightAssignerMapper reads the file produced by the Question 1 (or the output of our
 * job 1), line by line and 
 * emits (key, value) as (from, to) of an email header
 *
 */
public class EdgeWeightAssignerMapper extends Mapper<Object, Text, Text, Text>{

	private Text from=new Text();
	private Text to=new Text();

	// The map method
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Get a line of input
		if (value==null){
			return;
		}else{
			//Here the key will be "from" field and value will be "to" field of an email message
			
			String[] from_to_ts=value.toString().split("\t");//(context.getConfiguration().get("mapreduce.output.textoutputformat.separator"));
			if (from_to_ts.length==3){
				from.set(from_to_ts[0]);
				to.set(from_to_ts[1]);
				context.write(from, to);
			}else{
				return;
			}
			
			/*
			String pat = "([\\w\\-]([\\.\\w])+[\\w]+@([\\w\\-]+\\.)+[A-Za-z]{2,4})";
			Pattern p = Pattern.compile(pat);
            Matcher m = p.matcher(value.toString());
            ArrayList<String> fromToList=new ArrayList<String>();
            while(m.find()) {
       			fromToList.add(m.group(1));
            }
            if (fromToList.size()==3){
            	from.set(fromToList.get(0));
                to.set(fromToList.get(1));
                context.write(from,to);
            }else{
            	return;
            }
            */
		}
	}

}
