package enron;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * Description: CSVOutputFormat is a custom Fileoutputformat class for CSV converion
 *
 */
public class CSVOutputFormat extends FileOutputFormat<Text, Text>{

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String extension=".csv";
		//get the output path
		Path path=FileOutputFormat.getOutputPath(job);
		//get the full path with the file name
		Path fullPath=new Path(path, getUniqueFile(job, 
			      getOutputName(job), extension));
		//create the file
		FileSystem fs=path.getFileSystem(job.getConfiguration());
		FSDataOutputStream fileOut=fs.create(fullPath,job);
		return new CSVRecordWriter(fileOut);
	}
}

