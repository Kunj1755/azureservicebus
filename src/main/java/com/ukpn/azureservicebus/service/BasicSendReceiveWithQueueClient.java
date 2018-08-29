package com.ukpn.azureservicebus.service;

import java.time.Duration;

import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.IQueueClient;
import com.microsoft.azure.servicebus.Message;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.QueueClient;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

public class BasicSendReceiveWithQueueClient {
	// Connection String for the namespace can be obtained from the Azure portal
	// under the
	// 'Shared Access policies' section.
	private static final String connectionString = "Endpoint=sb://kunj-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SHUZjrSomj6uVlmHbvjt+klm4/B1AVtX1lLGwlrPpWc=";
	private static final String queueName = "kunjqueue1";
	private static QueueClient queueClient;
	private static int totalSend = 100;
	private static int totalReceived = 0;

	public static void main(String[] args) throws Exception {

		System.out.println("Starting BasicSendReceiveWithQueueClient sample");

		// create client
		System.out.println("Create queue client.");
		queueClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);

		// send and receive
		queueClient.registerMessageHandler(new MessageHandler(queueClient),
				new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));
		for (int i = 0; i < totalSend; i++) {
			int j = i;
			System.out.println("Sending message #%d.", j);
			queueClient.sendAsync(new Message("" + i)).thenRunAsync(() -> {
				System.out.println("Sent message #%d.", j);
			});
		}

		while (totalReceived != totalSend) {
			Thread.sleep(1000);
		}

		System.out.println("Received all messages, exiting the sample.");
		System.out.println("Closing queue client.");
		queueClient.close();
	}

	static class MessageHandler implements IMessageHandler {
		private IQueueClient client;

		public MessageHandler(IQueueClient client) {
			this.client = client;
		}

		@Override
		public CompletableFuture<Void> onMessageAsync(IMessage iMessage) {
			System.out.println("Received message with sq#: %d and lock token: %s.", iMessage.getSequenceNumber(),
					iMessage.getLockToken());
			return this.client.completeAsync(iMessage.getLockToken()).thenRunAsync(() -> {
				System.out.println("Completed message sq#: %d and locktoken: %s", iMessage.getSequenceNumber(),
						iMessage.getLockToken());
				totalReceived++;
			});
		}

		@Override
		public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
			System.out.println(exceptionPhase + "-" + throwable.getMessage());
		}
	}
}