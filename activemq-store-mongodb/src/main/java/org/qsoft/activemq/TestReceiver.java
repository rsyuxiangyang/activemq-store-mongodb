package org.qsoft.activemq;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class TestReceiver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory(
					"tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("queueTest");
			// create connection,session,consumer and receive message
			QueueConnection conn = factory.createQueueConnection();
			QueueSession session = conn.createQueueSession(false,
					Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiver = session.createReceiver(queue);

			TextMessage msg = (TextMessage) receiver.receive();

			System.out.println(msg.getText());

			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
