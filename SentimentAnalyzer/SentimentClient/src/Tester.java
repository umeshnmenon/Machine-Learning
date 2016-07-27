import com.fun.sentiment.SentimentAnalyser;


public class Tester {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SentimentAnalyser client=new SentimentAnalyser();
		System.out.println(client.getSentiment("This is fantastic and superb"));
	}

}
