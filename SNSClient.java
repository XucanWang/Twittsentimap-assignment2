package tweet2;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
public class SNSClient {
	public AmazonSNSClient ini() {
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
        AmazonSNSClient amazonSNSClient = new AmazonSNSClient(credentials);
		return amazonSNSClient;
	}

	public void publish(AmazonSNSClient amazonSNSClient, String temp) {
		// TODO Auto-generated method stub
		String topicArn="arn:aws:sns:us-east-1:139121763593:tweet";
		PublishRequest publishRequest = new PublishRequest(topicArn, temp);
		PublishResult publishResult = amazonSNSClient.publish(publishRequest);
		//print MessageId of message published to SNS topic
		System.out.println("publishResult - " + publishResult.toString());
		System.out.println("*************");
		//amazonSNSClient.publish("arn:aws:sns:us-east-1:139121763593:Tweet", temp);		
	}

}
