package tweet2;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class ProcessQueue {
		static SQS sqs;  //The queue that we retrieve message from
		static List<Message> messages;
		public static void main(String[] args) throws JSONException{
			sqs = new SQS();
			sqs.init();
			SNSClient snsclient = new SNSClient();
			AmazonSNSClient amazonSNSClient =snsclient.ini();
			
			while(sqs.getFromQueue()!=null){
		        Message msg=(Message)(sqs.getFromQueue());
		        sqs.deleteMes(msg.getReceiptHandle()); 
		        System.out.println("delete successfully");
			    String sentiTweet= getFinalResult(msg);
			    System.out.println("After process with sentiment"+sentiTweet);
			    JSONObject jsonObj = new JSONObject(sentiTweet);
				String senti =jsonObj.get("Sentiment").toString();
				if (senti.equals("positive") ||senti.equals("negative")||senti.equals("neutral") ){	
					//System.out.println(temp);
					System.out.println("into the SNS publish process");
					snsclient.publish(amazonSNSClient, sentiTweet);
				}
				
		        }
			
			System.out.println("Out of while--------------------------------");
			sqs.countMessage();
		}
		    public static String getFinalResult(Message msg){
		    	String finalResult="";
		    	if(msg!=null){
					System.out.println("delete from queue："+msg.toString());
					sqs.deleteMes(msg.getReceiptHandle());
					String rsvTwt = msg.getBody().replaceAll("\\\\", "");
					//System.out.println("rsvTwt:" +rsvTwt);
					String content = null;
					String sentmt = "unknown";
					String tmpResult = null;
					if(rsvTwt!=null){		
						try {
							content = extractContent(rsvTwt);
						} catch (JSONException e) {
							e.printStackTrace();
						}
						System.out.println("content:"+content);
						sentmt = extractSentiment(content);
						System.out.println("sentiment:"+sentmt);
						rsvTwt = rsvTwt.substring(0, rsvTwt.length()-1);
						tmpResult = rsvTwt + ",\"Sentiment\":\""+sentmt+"\"}";
						//tmpResult = rsvTwt + ",\"Sentiment\": \"positive\" }";
						finalResult = tmpResult;
					}
				}
		    	return finalResult;
		    }
		//extract sentiment from raw sentiment result. ex: positive, negative, neutral
			public static String extractSentiment(String rTwt){
				String rawSenti = "not appliable";
				StringTokenizer st = null;
				String tmpStr = "unknown";
				try {
					rawSenti = httpGet(rTwt);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//extract<type>
				if(rawSenti != "not appliable"){
					st = new StringTokenizer(rawSenti,"\n ");
					while(st.hasMoreTokens()){
						tmpStr = st.nextToken();
						if(tmpStr.contains("<type>")){
							tmpStr = tmpStr.substring(6, tmpStr.length()-7);
							break;
						}
					}
				}
				return tmpStr;
			}
			
		//extract Content from JSON string
		public static String extractContent(String rTwt) throws JSONException{
			String content = null;
			JSONObject json = new JSONObject(rTwt);
			content = json.get("Content").toString();
				return content;
		}
		public static String httpGet(String text) throws IOException {
			String urlStr = "http://gateway-a.watsonplatform.net/calls/text/TextGetTextSentiment?apikey=ed5796e89bc63ae770d5fb8f9436a732b2b57b45&text="+URLEncoder.encode(text,"UTF-8");
			String rawSentiment = null;
			URL url = new URL(urlStr);
			URI uri = null;
			try {
				uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println("uri:"+uri);
			url = uri.toURL();
			//System.out.println("url:"+url);
			
			try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
			    // use httpClient (no need to close it explicitly)
				HttpGet request = new HttpGet(url.toString());
				HttpResponse response = client.execute(request);
				HttpEntity entity = response.getEntity();
				rawSentiment = EntityUtils.toString(entity);

			} catch (IOException e) {
             // handle
			}
			//HttpClient client = new DefaultHttpClient();
			//System.out.println(rawSentiment);
			return rawSentiment;
			}
			    
	}


