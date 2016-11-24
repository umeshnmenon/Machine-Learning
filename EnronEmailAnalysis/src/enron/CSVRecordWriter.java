package enron;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 * 
 * RecordWriter class for the CSVOutputFormat
 *
 */
public class CSVRecordWriter extends RecordWriter<Text, Text>{
	private DataOutputStream out;

    public CSVRecordWriter(DataOutputStream stream) {
        out = stream;
        try {
        	out.writeBytes("Source,Target,Weight");
            out.writeBytes("\r\n");
        }
        catch (Exception ex) {
        }  
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        //close our file
        out.close();
    }

    @Override
    public void write(Text arg0, Text arg1) throws IOException, InterruptedException {
        //write out our key
        out.writeBytes(arg0.toString() + ",");
        out.writeBytes(arg1.toString());
        out.writeBytes("\r\n");  
    }
}
