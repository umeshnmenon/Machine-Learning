package twitter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStreamFactory;
import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;
import twitter4j.json.DataObjectFactory;

/**
 * 
 * Usage: This class collects the specified number of filtered twitter statuses using Twitter's streaming API
 * and writes to a file. Each line in the the file is a JSON entry. Three filters have been used here
 * 1. Location - is available in location.txt as lat and long
 * 2. topics - is available as user_preferences.txt
 * 3. Language - en (English)
 */
public class LocationBasedTweetsStorer {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws TwitterException,
	InterruptedException, IOException {

		
		//Eventually we want to limit this to a timestamp based say last 1 year or 2 years
		//for the time being declare an integer `TOTAL_TWEETS' representing the number of tweets
		// you wish to collect
		int TOTAL_TWEETS=1000;
		// Declare and set an array of strings called `topics' of topics you wish to
		// filter for >
		String[] topics =getFilterTopics();
		//Declare and set an two dimensional array for Location lat and long. Bounding Box
		double[][] bbox=getBoundingBox();
		
		// We use these to write to our file
		FileWriter fstream = new FileWriter("location_based_tweets.json");
		final BufferedWriter out = new BufferedWriter(fstream);
		// A queue to act as an intermediary between the asynchronous stream and
		// synchronous processing -- we set it to be rather large just so we
		// don't miss any tweets.
		final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(
				15000);
		
		// Our twitter stream
		twitter4j.TwitterStream twitterStream = new TwitterStreamFactory()
				.getInstance();
		
		// A listener which we use to process incoming tweets
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				// Convert the status object into a JSON string and put it into
				// the queue
				queue.offer(DataObjectFactory.getRawJSON(status));
				
				// Print out just the text of the status into the console so
				// that we can observe the running of our program >
				//System.out.println(status.toString());
			}
		
			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:"
						+ statusDeletionNotice.getStatusId());
			}
		
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:"
						+ numberOfLimitedStatuses);
			}
		
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId
						+ " upToStatusId:" + upToStatusId);
			}
		
			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}
		
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		
		// Create a filter query to filter for the topics we are interested in
		final FilterQuery query = new FilterQuery();
		//Lets first filter using Language
		String[] lang={"en"};
		query.language(lang);
		//Next filter using location
		query.locations(bbox);
		
		//Filtering with both location and topics is not allowed. So lets poll the tweets and check whether the
		//tweet contains any of the topic given in the topics array.
		//query.track(topics);
		
		// Add our listener to the stream
		twitterStream.addListener(listener);
		
		// Start the stream
		twitterStream.filter(query);
		Thread.sleep(20000);
		//Lets loop which extracts the JSON strings from the queue
		//then check whether the tweet contains the topics and if yes
		//write them into a file >
		Object oTweet;
		String tweet;
		int cnt=1;
		
		//while (!queue.isEmpty()){
			//if (cnt==TOTAL_TWEETS) break;
		while (cnt<=TOTAL_TWEETS){
			oTweet=queue.poll();
			if (oTweet!=null){
				tweet=oTweet.toString();
				for(String topic: topics){
					if (DataObjectFactory.createStatus(tweet).getText().toString().contains(topic.toLowerCase())){//NOTE: we can use regex for more exact matching
						out.write(tweet);
						out.newLine();
						cnt++;
						break;
					}
				}
			}
		}
		
		// Remember to shut things down properly
		twitterStream.cleanUp();
		out.close();
		fstream.close();
		
	}
	
	/*
	 * getFilterTopics() gets the topics from specified (topics.txt) file
	 */
	private static String[] getFilterTopics(){
		String[] topics;
		SimpleScanner scanner=new SimpleScanner("user_preferences.txt");
		topics=scanner.readContentsToArray();
		return topics;
	}
	
	/*
	 * getBoundingBox() gets the Bounding Box from the specified (location.txt) file
	 * The values are minLongitude, minLatitude, maxLongitude, maxLatitude
	 */
	private static double[][] getBoundingBox(){
		double[][] bbox=new double[2][2];
		String[] entries=new String[4];
		SimpleScanner scanner=new SimpleScanner("location.txt");
		entries=scanner.readContentsToArray(4);
		double val=0;
		for (int i=1;i<=4;i++){
			val=Double.parseDouble(entries[i-1]);
			switch(i){
			case 1: //min longitude
				bbox[0][0]=val;
				break;
			case 2: //min latitude
				bbox[0][1]=val;
				break;
			case 3: //max longitude
				bbox[1][0]=val;
				break;
			case 4://max latitude
				bbox[1][1]=val;
				break;
			}
		}
		/*
		for (int i=1;i<=4;i++){
			if (i % 2 ==0){
				bbox[1][(i/2)-1]=Double.parseDouble(entries[i-1]);
			}else{
				bbox[0][(int) ((i/2)-0.5)]=Double.parseDouble(entries[i-1]);
			}
		}
		*/
		/*
		for (int i=1;i<=4;i++){
			bbox[(int) (Math.ceil(i/2)-1)][]=Double.parseDouble(entries[i-1]);
		}
		*/
		
		return bbox;
	}
}
