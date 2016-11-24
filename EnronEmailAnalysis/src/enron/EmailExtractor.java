package enron;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * Description: EmailExtractor extracts the from, to and timestamp of an email message.  
 *
 */
public class EmailExtractor {

	/*
	 * Description: EmailExtractorMapper reads the file line by line and emits key as a tab separated
	 * string of from, to, and timestamp and null as value
	 */
	public static class EmailExtractorMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		// Map code goes here.
			Text emailkey=new Text();
			NullWritable val = NullWritable.get();
			String from = null;
			ArrayList<String> recipients = new ArrayList<String>();
			String timestamp = null;
			boolean skip = true;

			// You can put instance variables here to store state between iterations of
			// the map task.


			// The setup method. Anything in here will be run exactly once before the
			// beginning of the map task.
			public void setup(Context context) throws IOException, InterruptedException {
			
			}

			// The map method
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				// Get a line of input
				String line = value.toString();
				String pat = "([\\w\\-]([\\.\\w])+[\\w]+@([\\w\\-]+\\.)+[A-Za-z]{2,4})";
				Pattern p;
				if (line.startsWith("Message-ID:") && skip) {
					skip = false;
				}else if (line.startsWith("From:") && !skip){
					// Extract the sender's EMail address from the portion of
					// line following "From:"
		            p = Pattern.compile(pat);
		            Matcher m = p.matcher(line);
		            
		            	if(m.find()){
		            		from=m.group(1);
		            	}
			    
					
				}else if (line.startsWith("To:") && !skip){
					// Add the EMails on the portion of 
					// the line following "To:" to the recipients list
					p = Pattern.compile(pat);
		            Matcher m = p.matcher(line);
		            while(m.find()) {
		            	if(!recipients.contains(m.group(1))){
		            		recipients.add(m.group(1));
		            	}
		            }
				}
				else if (line.startsWith("Cc:") && !skip){
					// Add the EMails on the portion of
					// the line following "Cc:" to the recipients list
			        p = Pattern.compile(pat);
			        Matcher m = p.matcher(line);
			        while(m.find()) {
				    	if(!recipients.contains(m.group(1))){
				    		recipients.add(m.group(1));
				    	}
				    }	
				}
				else if (line.startsWith("Bcc:") && !skip){
					// Add the EMails on the portion of
					// the line following "Bcc:" to the recipients list
			        p = Pattern.compile(pat);
			        Matcher m = p.matcher(line);
			        while(m.find()) {
				    	if(!recipients.contains(m.group(1))){
				    		recipients.add(m.group(1));
				    	}
				    }	
				
				}else if (line.startsWith("Date:") && !skip) {
					// Extract the timestamp as a string from the portion of
					// line following "Date:"
					String dim  = ": " ;
					String[] datee= line.split(dim);
					timestamp=datee[1].trim();
				}else if (line.startsWith("\t") && !skip){
					// Add the EMails on the line to the recipients list
					p = Pattern.compile(pat);
		            Matcher m = p.matcher(line);
		            while(m.find()) {
		            	if(!recipients.contains(m.group(1))){
		            		recipients.add(m.group(1));
		            	}
		            }
				}else if ((line.equals("")) && !skip) {
					if (from != null && !recipients.isEmpty() && timestamp != null) {
						// Mapper emits "from	+to	+timestamp" as key and null value 
						//NullWritable val = NullWritable.get();
						String str =null;
						for (String to : recipients) {
							// Emit tab-separated tripes of (from, to, timestamp) as
							// reduce values. You can use key="" if you do not wish
							// to eliminate duplicates at this stage.
							str= from +"\t"+to+"\t"+timestamp;
							emailkey.set(str);
							context.write(emailkey,NullWritable.get());
						}
					}
					from = null;
					timestamp = null;
					recipients.clear();
					skip = true;
				}
						
			}

			// The cleanup method. Anything in here will be run exactly once after the
			// end of the map task.
			public void cleanup(Context context) throws IOException,
			InterruptedException {
				// Note: you can use Context methods to emit records in the setup and cleanup as well.

			}
	}
	
	/*
	 *  Reducer class for EmailExtractor. The key here is a tab separated string of from, to, and 
	 *  timestamp from Mapper and in the reduce() it just picks the key, thus avoiding duplicates
	 */
	public static class EmailExtractorReducer extends
			Reducer<Text, NullWritable, NullWritable,Text> {
		// Reduce code goes here.
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException
		{
			context.write(NullWritable.get(),key);
		}
		
	}

	/*
	 * Driver for the mapper/reducer
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: EmailExtractor <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(EmailExtractor.class);
		job.setMapperClass(EmailExtractorMapper.class);
		job.setReducerClass(EmailExtractorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}