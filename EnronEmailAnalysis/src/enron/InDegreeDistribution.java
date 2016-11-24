package enron;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
//import org.apache.hadoop.mapred.Task;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * Description: InDegreeDistribution calculates the degree distribution of "to", that is the in degree
 * distribution of a person
 *
 */
public class InDegreeDistribution {
	
	private static long total_no_nodes;
	/**************************************************************************************
     ********************************DEGREE MAPPER/REDUCER*********************************
     **************************************************************************************/
	/*
	 * Mapper for degree calculation
	 */
	public static class InDegreeMapper extends
							Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one=new IntWritable(1);
		private Text node=new Text();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			String delimit="\t";
			String[] des= line.split(delimit);
			if (des.length==3){
				if (des[1]!=null){
					node.set(des[1]);
					context.write(node, one);
				}
			}
		}
	}
	/*
	 * Reducer for Degree calculation
	 */
    public static class InDegreeReducer extends
						Reducer <Text,IntWritable, Text ,IntWritable> {

    	private IntWritable degree=new IntWritable();

    	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    			throws IOException, InterruptedException{
    		int sum=0;
    		for(IntWritable val:values){
    			sum+=val.get();
    		}
    		degree.set(sum);
    		context.write(key, degree);
    	}
    }
    
    /**************************************************************************************
     ***************************DISTRIBUTION MAPPER/REDUCER********************************
     **************************************************************************************/
    /*
     * Mapper for distribution
     */
    public static class InDistributionMapper extends
			Mapper<Object, Text, Text, FloatWritable> {
		private final static FloatWritable one=new FloatWritable(1);
		private Text degree=new Text();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			String delimit="\t";
			String[] des= line.split(delimit);
			if (des.length==2){
				if (des[1]!=null){
					degree.set(des[1]);
					context.write(degree, one);
				}
			}
		}
    }
    /*
     * Reducer for distribution
     */
    public static class InDistributionReducer extends
    		Reducer <Text,FloatWritable, Text ,FloatWritable> {

    	private FloatWritable distbn=new FloatWritable();
    	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
    			throws IOException, InterruptedException{
    		float sum=0;
    		for(FloatWritable val:values){
    			sum+=val.get();
    		}
    		sum=sum/(Float.parseFloat(context.getConfiguration().get("TOTAL_NO_NODES")));
    		distbn.set(sum);
 		
    		context.write(key, distbn);
    	}
    }

    /*
     * Main(). Driver is called here
     */
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: InDegreeDistribution <in> <out>");
			System.exit(2);
		}
		String outputPathDegreeReducer=otherArgs[1]+"/degree";
		String outputPathDistributionReducer=otherArgs[1]+"/distrbn";
		//Let's run the degree finder job for each node
		Job degreeJob=getDegreeJob();
		FileInputFormat.addInputPath(degreeJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(degreeJob, new Path(outputPathDegreeReducer));
		if (!degreeJob.waitForCompletion(true)) System.exit(1); //don't proceed if this is not succeeded
		total_no_nodes= degreeJob.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue();
		//Now with the above result, let's run the distribution for each degree
		Job distributionJob = getDistributionJob();
		FileInputFormat.addInputPath(distributionJob, new Path(outputPathDegreeReducer));
		FileOutputFormat.setOutputPath(distributionJob, new Path(outputPathDistributionReducer));
		if (!distributionJob.waitForCompletion(true)) System.exit(1);
		System.exit(0);
	}
    
    /*
     * Gets the first job which is to get the degrees of each node
     */
	private static Job getDegreeJob() throws IOException{
    	Job job = Job.getInstance(new Configuration());
		job.setJarByClass(InDegreeDistribution.class);
    	job.setMapperClass(InDegreeMapper.class);
    	job.setCombinerClass(InDegreeReducer.class);
    	job.setReducerClass(InDegreeReducer.class);
    	job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
    	
		return job;
    }
    
	/*
	 * Gets the second job which is to calculate the distribution (no of nodes) of each degree
	 */
	private static Job getDistributionJob() throws IOException{
		Configuration conf=new Configuration();
		conf.set("TOTAL_NO_NODES", String.valueOf(total_no_nodes));
    	Job job = Job.getInstance(conf);
		job.setJarByClass(InDegreeDistribution.class);
    	job.setMapperClass(InDistributionMapper.class);
    	//job.setCombinerClass(InDistributionReducer.class);
    	job.setReducerClass(InDistributionReducer.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(FloatWritable.class);
    	job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
    	//job.setOutputFormatClass(TextOutputFormat.class);
		return job;
    }
  }