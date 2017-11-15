package twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
/**
 * 
 * Usage: This class accepts a file name and writes to the file
 *
 */
public class SimpleWriter{

	private String fileName="";
	SimpleWriter(String fileName){
		if (fileName==""){
		    System.err.println("Function expects a valid file name");
		    System.exit(0);
		}
		this.fileName=fileName;
	}

	/*
	 * writeArrayList() writes each entry in the array list as a line in the file
	 */
	public void writeArrayList(ArrayList<String>lst){
		FileWriter fstream;
		try {
			fstream = new FileWriter(this.fileName);
			final BufferedWriter out = new BufferedWriter(fstream);
			for(String item:lst){
				out.write(item);
				System.out.println(item);
				out.newLine();
			}
			out.close();
			fstream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * writeText() writes a text as line entry in the file
	 */
	public void writeText(String text){
		FileWriter fstream;
		try {
			fstream = new FileWriter(this.fileName);
			final BufferedWriter out = new BufferedWriter(fstream);
			out.write(text);
			System.out.println(text);
			out.close();
			fstream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
		
}
