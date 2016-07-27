package com.fun.sentiment;

//import java.io.PrintWriter;
//import java.util.List;
import java.util.Properties;




import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


public class SentimentAnalyser {
	public static final String JAVABRIDGE_PORT="8087";
	static final php.java.bridge.JavaBridgeRunner runner =
	    php.java.bridge.JavaBridgeRunner.getInstance(JAVABRIDGE_PORT);
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println(getSentiment("Neither good and bad"));
		runner.waitFor();
		System.exit(0);
	}

	public String getSentiment(String comment){
		String sentiment="NEUTRAL";
		// Create a CoreNLP pipeline. 
		// Add in sentiment
	    Properties props = new Properties();
	    props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment");
	    String[] sentimentText = {"Very Negative", "Negative", 
	    	    "Neutral", "Positive", "Very Positive"};
	    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
	    //BELOW IS A SIMPLE USAGE
//	    //==================================================
//	    // Initialize an Annotation with some text to be annotated. The text is the argument to the constructor.
//	    Annotation annotation;
//	    annotation = new Annotation(comment);
//	    // run all the selected Annotators on this text
//	    pipeline.annotate(annotation);
//	    
//	    // this prints out the results of sentence analysis to file(s) in good formats
//	    PrintWriter out = new PrintWriter(System.out);
//	    pipeline.prettyPrint(annotation, out);
//	    
//	    // An Annotation is a Map with Class keys for the linguistic analysis types.
//	    // You can get and use the various analyses individually.
//	    List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
//	    if (sentences != null && ! sentences.isEmpty()) {
//	      CoreMap sentence = sentences.get(0);
//	      //System.out.println("The first sentence overall sentiment rating is " + sentence.get(SentimentCoreAnnotations.SentimentClass.class));
//	      sentiment=sentence.get(SentimentCoreAnnotations.SentimentClass.class);
//	    }
//	    //========================================================
	    Integer mainSentiment = 0;
	    if (comment != null && comment.length() > 0) {
	    	int longest = 0;
	    	Annotation annotation1 = pipeline.process(comment);
	    	for (CoreMap sentence : annotation1
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment1 = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment1;
                    longest = partText.length();
                }

            }
	    }
	    if (mainSentiment > 4 || mainSentiment < 0) {
	    	sentiment="";
	    }else{
	    	sentiment=sentimentText[mainSentiment];
	    }
		return(sentiment);
	}
}
