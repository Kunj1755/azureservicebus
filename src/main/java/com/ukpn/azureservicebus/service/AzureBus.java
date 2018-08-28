package com.ukpn.azureservicebus.service;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

public class AzureBus {
	
	public static void sendMessagesToAzureQueue() {
		try
		{
			Configuration config =
				    ServiceBusConfiguration.configureWithSASAuthentication(
				            "kunj-servicebus",
				            "RootManageSharedAccessKey",
				            "SHUZjrSomj6uVlmHbvjt+klm4/B1AVtX1lLGwlrPpWc=",
				            ".servicebus.windows.net"
				            );

			ServiceBusContract service = ServiceBusService.create(config);
						
			for (int i=10; i<15; i++)
			{
			     // Create message, passing a string message for the body.
			     BrokeredMessage message = new BrokeredMessage("Test message " + i);
			     // Set an additional app-specific property.
			     message.setProperty("MyProperty", i);
			     // Send message to the queue
			     service.sendQueueMessage("kunjqueue1", message);
			}
			
			/*byte[] bytes = Encoding.UTF8.GetBytes(encryptData);
			MemoryStream stream = new MemoryStream(bytes, writable: false);
			BrokeredMessage message = new BrokeredMessage(stream);
			message.ContentType = "application/json";
			testQueueSender.Send(message);*/
		}
		catch (ServiceException e)
		{
		    System.out.print("ServiceException encountered: ");
		    System.out.println(e.getMessage());
		    System.exit(-1);
		}
	}
	
	public static void ReceiveMessagesToAzureQueue() {
		try
		{
			Configuration config =
				    ServiceBusConfiguration.configureWithSASAuthentication(
				            "kunj-servicebus",
				            "RootManageSharedAccessKey",
				            "SHUZjrSomj6uVlmHbvjt+klm4/B1AVtX1lLGwlrPpWc=",
				            ".servicebus.windows.net"
				            );

			ServiceBusContract service = ServiceBusService.create(config);
		    ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
		    opts.setReceiveMode(ReceiveMode.PEEK_LOCK);

		    while(true)  {
		         ReceiveQueueMessageResult resultQM =
		                 service.receiveQueueMessage("kunjqueue1", opts);
		        BrokeredMessage message = resultQM.getValue();
		        if (message != null && message.getMessageId() != null)
		        {
		            System.out.println("MessageID: " + message.getMessageId());
		            // Display the queue message.
		            System.out.print("From queue: ");
		            byte[] b = new byte[200];
		            String s = null;
		            int numRead = message.getBody().read(b);
		            while (-1 != numRead)
		            {
		                s = new String(b);
		                s = s.trim();
		                System.out.print(s);
		                numRead = message.getBody().read(b);
		            }
		            System.out.println();
		            System.out.println("Custom Property: " +
		                message.getProperty("MyProperty"));
		            // Remove message from queue.
		          
		            System.out.println("Deleting this message.");
		            //service.deleteMessage(message);
		        }  
		        else  
		        {
		            System.out.println("Finishing up - no more messages.");
		            break;
		            // Added to handle no more messages.
		            // Could instead wait for more messages to be added.
		        }
		    }
		}
		catch (ServiceException e) {
		    System.out.print("ServiceException encountered: ");
		    System.out.println(e.getMessage());
		    System.exit(-1);
		}
		catch (Exception e) {
		    System.out.print("Generic exception encountered: ");
		    System.out.println(e.getMessage());
		    System.exit(-1);
		}
	}
	
	public static void main(String... args) {
		//AzureBus.sendMessagesToAzureQueue();
		AzureBus.ReceiveMessagesToAzureQueue();
		
	}	

}
