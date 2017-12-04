package com.example.mq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class CreateTopicExample {
	
	 public static void publishMsg() {
		    String projectId = ServiceOptions.getDefaultProjectId();

		 TopicName topicName = TopicName.of(projectId, "test-topic");
		 Publisher publisher = null;
		 List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

		 try {
		   // Create a publisher instance with default settings bound to the topic
		   publisher = Publisher.newBuilder(topicName).build();

		   List<String> messages = Arrays.asList("first message", "second message");

		   // schedule publishing one message at a time : messages get automatically batched
		   for (String message : messages) {
		     ByteString data = ByteString.copyFromUtf8(message);
		     PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

		     // Once published, returns a server-assigned message id (unique within the topic)
		     ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
		     messageIdFutures.add(messageIdFuture);
		   }
		   
		   System.out.println("finsish message");
		 } catch (Exception e) {
			e.printStackTrace(); 
		 } finally {
			 
			 try {
		   // wait on any pending publish requests.
		  System.out.println("pending any request");
		   List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

		   
		  
		   
		   for (String messageId : messageIds) {
		     System.out.println("published with message ID: " + messageId);
		   }

		   System.out.println("Goging to shutdown the message");
		   if (publisher != null) {
		     // When finished with the publisher, shutdown to free up resources.
		     publisher.shutdown();
		   }
			 } catch (Exception e) {
				 e.printStackTrace();;
			 }
		 }
		 
	 }
	 public static void main(String... args) throws Exception {
		 publishMsg();
		// createTopic();
		 
		   
     }
	 
	 public static void createTopic() throws Exception{
	
		 // Your Google Cloud Platform project ID
		    String projectId = ServiceOptions.getDefaultProjectId();

		    // Your topic ID, eg. "my-topic"
		    String topicId = "my-topic";
		     // Create a new topic
		    
	        TopicName topic = TopicName.of(projectId, topicId);
			
	        System.out.println("the topic of project name is "  + topic.getProject());
	        
	      //  TopicName topic = TopicName.of(projectId, topicId);
	        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
	          topicAdminClient.createTopic(topic);
		    	
		       	System.out.println("Created the toipic!!!");
		    } catch (ApiException e) {
		      e.printStackTrace();
		      // example : code = ALREADY_EXISTS(409) implies topic already exists
		      System.out.print(e.getStatusCode());
		      System.out.print(e.isRetryable());
		    }

		    System.out.printf("Topic %s:%s created.\n", topic.getProject(), topic.getTopic());
		  }
	 }
