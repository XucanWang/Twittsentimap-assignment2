package tweet2;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
public class SQS {
	AmazonSQS sqs;
	public static String myQueueUrl;
	public SQS(){
		init();
	}
	
    public void init(){
    AWSCredentials credentials = null;
    try {
        credentials = new ProfileCredentialsProvider("default").getCredentials();
    } catch (Exception e) {
        throw new AmazonClientException(
                "Cannot load the credentials from the credential profiles file. " +
                "Please make sure that your credentials file is at the correct " +
                "location (~/.aws/credentials), and is in valid format.",
                e);
    }

    sqs = new AmazonSQSClient(credentials);
    Region usEast1 = Region.getRegion(Regions.US_EAST_1);
    sqs.setRegion(usEast1);
    //System.out.println("Creating a new SQS queue called tweets.\n");
    CreateQueueRequest createQueueRequest = new CreateQueueRequest("tweets");
    myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
    }
    
    public void putInQueue(int TID, String usr, String content, String time, String latitude, String longitude){
		String jsonString ="{ \"TID\": "+ TID +", " + "\"Usr\": "+ "\"" +usr+ "\", " +"\"Content\": " + "\""+ content + "\", "+"\"Time\": " + "\""+ time + "\", " + "\"Lat\": " + latitude + ", " + "\"Lon\": "+ longitude + "}";
		//System.out.println("-------"+jsonString);
		//System.out.println("-------"+myQueueUrl);
		sqs.sendMessage(new SendMessageRequest(myQueueUrl,jsonString));
	}
    
	public Message getFromQueue(){
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		for (Message message : messages) {
			return message;
		}
		return null;
	}
	public void deleteMes(String messageRecieptHandle){
		sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
		
	}
	public void deleteQueue(){
		sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
	}

	public void countMessage(){
		GetQueueAttributesRequest request = new GetQueueAttributesRequest(myQueueUrl);
		request.withAttributeNames("All");
		Map<String, String> rlt =  sqs.getQueueAttributes(request).getAttributes();            
		System.out.println("number of messages:" + rlt.get("ApproximateNumberOfMessages"));
	}

}
