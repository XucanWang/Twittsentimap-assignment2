package tweet2;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
public class TwitterStreaming {
	static String ConsumerKey="OkdTvQevRCEpqK0imEwEbDHcm";
    static String ConsumerSecret="4SWX7uNl0l6pFGyHlkx8plAw2ahduPctUePi7tcZtjxdfNa40i";
    static String AccessToken="593902431-jp0f1Tp3XjBxMAY2gIQPbdnYY1NzUpl6XgHtNNzf";
    static String TokenSecret="UTjWeRlUgSO7teyqsPOuYztJ9y2QGUFAbmrbwc8SjW073";
    
	static SQS tweetSQS;
	static int tweetID=0;
   
    public static void main(String[] args) throws TwitterException,UnknownHostException, SQLException, JSONException, twitter4j.JSONException {
		tweetSQS = new SQS();
		tweetSQS.init();
		ConfigurationBuilder builder=new ConfigurationBuilder();
    	builder.setDebugEnabled(true).setOAuthConsumerKey(ConsumerKey)
    	.setOAuthConsumerSecret(ConsumerSecret)
    	.setOAuthAccessToken(AccessToken)
    	.setOAuthAccessTokenSecret(TokenSecret);
       TwitterStream twitterStream = new TwitterStreamFactory(builder.build()).getInstance();
       StatusListener listener = new StatusListener(){
            public void onStatus(Status st) {
                 if (st.getGeoLocation()!= null&&st.getUser()!=null&&st.getLang().matches("en")) {
                     try {
 						String latitude = st.getGeoLocation().getLatitude()+"";
 						String longitude = st.getGeoLocation().getLongitude()+"";
 						String usr = st.getUser().getScreenName();
 						String content = st.getText().replaceAll("\u005c\u0022", "");
 						content = content.replaceAll("(\\r|\\n|\\r\\n)+", "");
 						String time  = dateConvert( st.getCreatedAt().toString() );
 						tweetSQS.putInQueue(++tweetID, usr, content, time,latitude, longitude);
 						System.out.println("send to queue:"+content);
 					} catch (ParseException e) {
 						e.printStackTrace();
 					}
                     //importer.ImportTweet(st);
                 } 
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onScrubGeo(long arg0, long arg1) {}
			public void onStallWarning(StallWarning arg0) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
            }			
        };       
        twitterStream.addListener(listener);       
        twitterStream.sample(); 
        
       }

      public static String dateConvert(String date) throws ParseException{
		SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy");
		Date parsedDate = dateFormat.parse(date);
		Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
		return timestamp.toString();
	}

}
