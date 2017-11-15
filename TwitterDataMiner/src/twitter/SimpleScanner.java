package twitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
/**
 * 
 * Usage: This class accepts a file name and opens the file and reads the file contents
 *
 */
public class SimpleScanner {

	private String fileName="";
	SimpleScanner(String fileName){
		if (fileName==""){
		    System.err.println("Function expects a valid file name");
		    System.exit(0);
		}
		this.fileName=fileName;
	}

	public String[] readContentsToArray(int lines){
		String[] res=new String[lines];
		try{
			File file =new File(fileName);
			Scanner scanner=new Scanner(file);
			String line;
			int row=0;
			while (scanner.hasNextLine()){
				if (row==lines) break;
			    line=scanner.nextLine();
			    res[row]=line;
			    row++;
			}
			scanner.close();
		}catch (FileNotFoundException ex){
			System.err.println("Could not find file");
		    System.exit(0);
		}
		return res;
	}
	
	public String[] readContentsToArray(){
		ArrayList<String> resLst=new ArrayList<String>();
		try{
			File file =new File(fileName);
			Scanner scanner=new Scanner(file);
			String line;
			while (scanner.hasNextLine()){
			    line=scanner.nextLine();
			    resLst.add(line);
			}
			scanner.close();
		}catch (FileNotFoundException ex){
			System.err.println("Could not find file");
		    System.exit(0);
		}
		String[] res=new String[resLst.size()];
		resLst.toArray(res);
		return res;
	}
}
