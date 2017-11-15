package twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import twitter4j.Status;
import twitter4j.TwitterException;


import twitter4j.json.DataObjectFactory;

public class LocationTweetsSentimentAssigner {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws TwitterException 
	 */
	public static void main(String[] args) throws IOException, TwitterException {
		/* REMOVE THE COMMENT IF YOU WANT TO PARAMTERIZE THE COLLECTION NAME
		if (args.length==0){
			System.err.println("Please provide a tweets file name!");
			System.exit(0);
		}
		String tweetFile=args[0];
		*/
		//HARD CODING THE tweet file for the time being
		String tweetFile="location_based_tweets.json";
		String[] topics =getFilterTopics();
		String[] restaurants =getFilterRestaurants();
		String[] posWords=getWords("posWords.txt");
		String[] negWords=getWords("negWords.txt");
		File file=new File(tweetFile);
		Scanner scanner=new Scanner(file);
		String line;
		//Let's iterate through the output and write to a csv
		FileWriter csvWriter;
		csvWriter = new FileWriter("restaurant_sentiments_by_location.csv");
		csvWriter.append("restaurant");
		csvWriter.append(',');
		csvWriter.append("location");
		csvWriter.append(',');
		csvWriter.append("keyword");
		csvWriter.append(',');
		csvWriter.append("pos_sentiment");
		csvWriter.append(',');
		csvWriter.append("neg_sentiment");
		csvWriter.append(',');
		csvWriter.append("sentiment_score");
		csvWriter.append('\n');
		
		while (scanner.hasNext()){
			line=scanner.nextLine();
			Status tweet=DataObjectFactory.createStatus(line);
			String tweetText=tweet.getText().toLowerCase();
			for (String restaurant: restaurants){
				boolean bTopicFound=false;
				boolean bPosFound=false;
				boolean bNegFound=false;
				if (tweetText.contains(restaurant.toLowerCase())){
					csvWriter.append(restaurant);
					csvWriter.append(',');
					csvWriter.append("phoenix"); //we should get the location from bounding box, now hardcoidng as phoenix
					csvWriter.append(',');
					for (String topic: topics){
						if (tweetText.contains(topic.toLowerCase())){
							csvWriter.append(topic);
							csvWriter.append(',');
							bTopicFound=true;
							for(String word: posWords){
								if (tweetText.contains(word.toLowerCase())){
									csvWriter.append("1");
									csvWriter.append(',');
									bPosFound=true;
									break;
								}
							}
							if (!bPosFound){
								csvWriter.append("0");
								csvWriter.append(',');
							}
							for(String word: negWords){
								if (tweetText.contains(word.toLowerCase())){
									csvWriter.append("1");
									csvWriter.append(',');
									bNegFound=true;
									break;
								}
							}
							if (!bNegFound){
								csvWriter.append("0");
								csvWriter.append(',');
							}
							csvWriter.append('\n');
						}
					}
					if (!bTopicFound){
						csvWriter.append("");
						csvWriter.append(',');
						for(String word: posWords){
							if (tweetText.contains(word.toLowerCase())){
								csvWriter.append("1");
								csvWriter.append(',');
								break;
							}
						}
						if (!bPosFound){
							csvWriter.append("0");
							csvWriter.append(',');
						}
					
						for(String word: negWords){
							if (tweetText.contains(word.toLowerCase())){
								csvWriter.append("1");
								csvWriter.append(',');
								break;
							}
						}
						if (!bNegFound){
							csvWriter.append("0");
							csvWriter.append(',');
						}
						csvWriter.append('\n');
					}
					
				}
			}
			
		}
		
		csvWriter.flush();
		csvWriter.close();
		System.out.println("logged the results to csv");
	}

	/*
	 * getWords() gets the positive words from specified file (here posWords.txt and negWords.txt)
	 */
	private static String[] getWords(String file) throws IOException{
		ArrayList<String> lstWords=new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			lstWords.add(line);
		}
		br.close();
		//String[] words=new String[lstWords.size()];
		//lstWords.toArray(words);
		//return words;
		//there are duplicates so remove them, though we don't have to do it we will just clean it up and to improve the
		//performance
		//Set <String> setWords=new HashSet<>(Arrays.asList(words));
		Set <String> setWords=new HashSet<>(lstWords);
		String[] filteredWords=new String[setWords.size()];
		setWords.toArray(filteredWords);
		//System.out.println("count is " + filteredWords.length);
		return filteredWords;
		
	}
	
	/*
	 * getFilterTopics() gets the topics from specified (user_preferences.txt) file
	 */
	private static String[] getFilterTopics(){
		String[] topics;
		SimpleScanner scanner=new SimpleScanner("user_preferences.txt");
		topics=scanner.readContentsToArray();
		return topics;
	}
	
	/*
	 * getFilterRestaurants() gets the topics from specified (restaurants.txt) file
	 */
	private static String[] getFilterRestaurants(){
		String[] rests;
		SimpleScanner scanner=new SimpleScanner("restaurants.txt");
		rests=scanner.readContentsToArray();
		return rests;
	}
}
