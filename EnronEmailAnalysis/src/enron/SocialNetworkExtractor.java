package enron;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * 
 * Description: Driver class to run duplicate Node-Edge mapper and reducer, and EdgeWeightAssigner 
 * mapper and reducer
 */
public class SocialNetworkExtractor {
	
	public static enum NODE_ARC_TABLE{
		TOTAL_NO_OF_ARCS{
			public String toString(){
				return "TOTAL_NO_OF_ARCS";
			}
		},
		MAX_ARC_WEIGHT{
			public String toString(){
				return "MAX_ARC_WEIGHT";
			}
		}
	}
	
	private enum NORMALIZATION_METHOD{
		TOTAL_NO_OF_ARCS{
			public String toString(){
				return "TOTAL_NO_OF_ARCS";
			}
		},
		MAX_ARC_WEIGHT{
			public String toString(){
				return "MAX_ARC_WEIGHT";
			}
		}
	}
	
	private long total_no_of_arcs;
	private float max_arc_wt;
	/**
	 * Description: The mapper class to remove duplicate Node-Edge pairs
	 */
	public static class DuplicateRemoverMapper extends NodeEdgeDuplicateRemoverMapper{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//calls NodeEdgeDuplicateRemoverMapper map()
			super.map(key, value, context);
		}
	}
	
	/**
	 * Description: The reducer class to remove the duplicate Node-Edge pairs
	 */
	public static class DuplicateRemoverReducer extends NodeEdgeDuplicateRemoverReducer{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException{
			super.reduce(key, values, context);	
		}
	}

	/**
	 * Description: The mapper class to assign weights to the edges
	 */
	public static class WeightAssignerMapper extends EdgeWeightAssignerMapper{
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//calls EdgeWeightAssignerMapper map()
			super.map(key, value, context);
		}
	}
	
	/**
	 * Description: The reducer class to assign weights to the edges
	 */
	public static class WeightAssignerReducer extends EdgeWeightAssignerReducer{
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			super.reduce(key, values, context);	
		}
	}
	
	/*
	 * Description: The mapper class to normalize the weights using Max Arc Weight
	 */
	public static class NormalizerMapper extends NormalizerByMaxArcWtMapper {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
		}
	}
	
	/*
	 * Description: The reducer class to normalize the weights using MaxArcWt
	 */
	public static class NormalizerReducer extends NormalizerByMaxArcWtReducer{
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
				throws IOException, InterruptedException{
			super.reduce(key, values, context);
		}
	}
	
	/*
	 * Description: Mapper class for filtering the enron emails 
	 */
	public static class EmailFilterMapper extends EnronEmailFilterMapper{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
		}
	}
	
	/*
	 * Description: Reducer class for filtering the enron emails
	 */
	public static class EmailFilterReducer extends EnronEmailFilterReducer{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException{
			super.reduce(key, values, context);
		}
	}
	/*
	 * Main function
	 */
	public static void main(String[] args) throws Exception {
		SocialNetworkExtractor driver=new SocialNetworkExtractor();
		int res=driver.run(args);
		System.exit(res);
	}
	
	/*
	 * The driver to execute the two jobs and corresponding mapper and reducers 
	 */
	private int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: SocialNetworkExtractor <in> <out>");
			System.exit(2);
		}
		
		//Since there are many map/reduce jobs, we are setting our paths now
		String inputPath =otherArgs[0];
		String intermediatePath=otherArgs[1]+"/temp";
		String outputPath=otherArgs[1]+"/final";
		String outputPathNoofArcs=outputPath + "/by_total_arcs";
		String outputPathMaxWt=outputPath + "/by_max_wt";
		String outputPathFilterArc=outputPath + "/by_total_arcs_filtered";
		String outputPathFilterMaxWt=outputPath + "/by_max_wt_filtered";
		
		System.out.println("=============================================================");
		System.out.println("JOB 1 DUPLICATE REMOVAL");
		System.out.println("=============================================================");
		//We have already removed the duplicate, still doing it again
		Job duplicateRemoverJob= getDuplicateRemoverJob();
		//set the input and output folders
		FileInputFormat.addInputPath(duplicateRemoverJob, new Path(inputPath));
		//Let's create and intermediate output folder
		FileOutputFormat.setOutputPath(duplicateRemoverJob, new Path(intermediatePath));
		//execute it
		if (!duplicateRemoverJob.waitForCompletion(true)) return 1;
		//this would have created files in our output folder. These are inputs to our second job
		//setting total no of arcs from job 2
		total_no_of_arcs= duplicateRemoverJob.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue();
		System.out.println("=============================================================");
		System.out.println("TOTAL NO OF ARCS AFTER JOB 1 IS "+total_no_of_arcs);
		System.out.println("=============================================================");
		
		
		System.out.println("=============================================================");
		System.out.println("JOB 2 FILTERING OUT NON ENRON EMAIL FROM 'FROM' ");
		System.out.println("=============================================================");
		//we will filter out onn enron emails to have only enron emails for our analysis
		Job filterJob=getFilterJob();
		FileInputFormat.addInputPath(filterJob, new Path(intermediatePath));
		FileOutputFormat.setOutputPath(filterJob, new Path(outputPath));
		//execute it
		if (!filterJob.waitForCompletion(true)) return 1;

		total_no_of_arcs= filterJob.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue();
		System.out.println("=============================================================");
		System.out.println("TOTAL NO OF ARCS AFTER JOB 2 IS "+total_no_of_arcs);
		System.out.println("=============================================================");
		
		System.out.println("=============================================================");
		System.out.println("JOB 3 ASSIGNING WEIGHTS AND NORMALIZATING BY TOTAL NO OF ARCS");
		System.out.println("=============================================================");
		//get the second job
		Job edgeWeightAssignerJob=getEdgeWeightAssignerJob();
		//set the input and output folders
		FileInputFormat.addInputPath(edgeWeightAssignerJob, new Path(outputPath));//FileInputFormat.addInputPath(edgeWeightAssignerJob, new Path(intermediatePath));
		FileOutputFormat.setOutputPath(edgeWeightAssignerJob, new Path(outputPathNoofArcs));
		//execute it
		if (!edgeWeightAssignerJob.waitForCompletion(true)) return 1;
		
		//setting the max arc weight 
		max_arc_wt=edgeWeightAssignerJob.getCounters().findCounter(NODE_ARC_TABLE.MAX_ARC_WEIGHT).getValue();
		System.out.println("=============================================================");
		System.out.println("MAX ARC WEIGHT IS "+max_arc_wt);
		System.out.println("=============================================================");
		
		System.out.println("=============================================================");
		System.out.println("JOB 4 ASSIGNING WEIGHTS AND NORMALIZATING BY MAX ARC WEIGHT");
		System.out.println("=============================================================");
		//Do the last job of normalization by max weight, if the NORMALIZATION_METHOD is set to MAX_WT
		//This is because only after the second job, we will come to know the max weight
		//if (edgeWeightAssignerJob.getConfiguration().get("NORMALIZATION_METHOD")=="MAX_WT"){
			//get the third and last job
			Job normalizerJob=getNormalizationJob();
			//set the input and output folders
			FileInputFormat.addInputPath(normalizerJob, new Path(outputPathNoofArcs));
			FileOutputFormat.setOutputPath(normalizerJob, new Path(outputPathMaxWt));
			//execute it
			if (!normalizerJob.waitForCompletion(true)) return 1;
		//}

		
		
		//convertAllToCSV(outputPathNoofArcs)	;
		//convertAllToCSV(outputPathMaxWt);
		return 0;
	}
	
	/*
	 * Gets the duplicate remover job with its configurations set
	 */
	private  Job getDuplicateRemoverJob() throws IOException{
		Configuration conf=new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SocialNetworkExtractor.class);
		job.setMapperClass(DuplicateRemoverMapper.class);
		job.setReducerClass(DuplicateRemoverReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}
	
	/*
	 * Gets the edge weight assigner job with its configurations set
	 */
	private Job getEdgeWeightAssignerJob() throws IOException{
		Configuration conf=new Configuration();
		
		//Custom configuration settings for the second job
		//Setting the previous reducers output counter. This will be our total no of arcs.
		//Configuration is the only persistence data which is shared
		//across all mappers/reducers. Also we cannot set the counters for the next job until unless it is started. 
		conf.set("TOTAL_NO_OF_ARCS", String.valueOf(total_no_of_arcs));
		//We are not using the below setting, rather we do bot the normalization
		//setNormalizationMethod(conf, NORMALIZATION_METHOD.TOTAL_NO_OF_ARCS.toString());//TOTAL_NO_OF_ARCS OR MAX_WT
		conf.set(NODE_ARC_TABLE.MAX_ARC_WEIGHT.toString(), "0");
		//conf.set("mapreduce.output.textoutputformat.separator", ",");
		conf.set("mapreduce.output.textoutputformat.separator", "\t");
		Job job = Job.getInstance(conf);
		job.setJarByClass(SocialNetworkExtractor.class);
		job.setMapperClass(WeightAssignerMapper.class);
		job.setReducerClass(WeightAssignerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}
	
	
	/*
	 * Gets the job for normalization, if we want to normalize our results by max arc weight
	 */
	private Job getNormalizationJob() throws IOException{
		Configuration conf=new Configuration();
		
		//Custom configuration settings for the second job
		//Configuration is the only persistence data which is shared
		//across all mappers/reducers. Also we cannot set the counters for the next job until unless it is started.
		conf.set("TOTAL_NO_OF_ARCS", String.valueOf(total_no_of_arcs));
		//We are not using the below setting, rather we do bot the normalization
		//setNormalizationMethod(conf, NORMALIZATION_METHOD.MAX_ARC_WEIGHT.toString());
		conf.set(NODE_ARC_TABLE.MAX_ARC_WEIGHT.toString(),String.valueOf(max_arc_wt));
		//conf.set("mapreduce.output.textoutputformat.separator", ",");//mapred.textoutputformat.separator
		conf.set("mapreduce.output.textoutputformat.separator", "\t");
		Job job = Job.getInstance(conf);
		job.setJarByClass(SocialNetworkExtractor.class);
		job.setMapperClass(NormalizerMapper.class);
		job.setReducerClass(NormalizerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(CSVOutputFormat.class);
		return job;
	}
	
	/*
	 * Gets the job for filtering
	 */
	private  Job getFilterJob() throws IOException{
		Configuration conf=new Configuration();
		//conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(SocialNetworkExtractor.class);
		job.setMapperClass(EmailFilterMapper.class);
		job.setReducerClass(EmailFilterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job;
	}
	
	/*
	 * Sets the Normalization method and the textoutputformat separator
	 */
	private void setNormalizationMethod(Configuration conf, String method){
		conf.set("NORMALIZATION_METHOD",method); //TOTAL_NO_OF_ARCS OR MAX_WT
		if (method==NORMALIZATION_METHOD.TOTAL_NO_OF_ARCS.toString()) conf.set("mapred.textoutputformat.separator", ","); //Set this only if it is TOTAL_NO_OF_ARCS
	}
	

	/*
	 * @Deprecated: Converts all files in a folder to csv. THIS WILL NOT WORK FOR FILES WITH SPACES IN IT.
	 * Also assuming defualt hadoop file names, i.e. files start with part-r-
	 */
	private void convertAllToCSV(String folder){
		String command="for f in `find "+folder+"/part-r-* -depth`;do cat $f > `echo "+folder+"/$f.csv`;done";
		System.out.println(executeShellCommand(command));
	}
	/*
	 * Execute shell commands from java
	 */
	private String executeShellCommand(String command){
		StringBuffer output =new StringBuffer();
		Process p;
		try{
			System.out.println(command);
			p=Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line="";
			while ((line=reader.readLine())!=null){
				output.append(line+"\n");
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		return output.toString();
	}
}
