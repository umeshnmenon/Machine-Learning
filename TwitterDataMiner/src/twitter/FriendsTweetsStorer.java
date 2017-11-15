
package twitter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import twitter4j.IDs;
import twitter4j.Relationship;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

public class FriendsTweetsStorer {

	/**
	 * @param args
	 * @throws TwitterException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws TwitterException, IOException {
		// TODO Auto-generated method stub
		long lCursor=-1;
		
		Twitter twitter=new TwitterFactory().getInstance();
		IDs friendIDs=twitter.getFriendsIDs(twitter.getId(), lCursor);
		FileWriter fstream = new FileWriter("friends_tweets.json");
		final BufferedWriter out = new BufferedWriter(fstream);
		do{
			for (long friendID: friendIDs.getIDs()){
				//System.out.println("friend is " + friendID);
				ResponseList<twitter4j.Status> tweets=twitter.getUserTimeline(friendID);
				for (Status tweet : tweets) {
					out.write(DataObjectFactory.getRawJSON(tweet));
					out.newLine();
		            //System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
		        }
			}
		}while (friendIDs.hasNext());
		out.close();
		fstream.close();
	}

}