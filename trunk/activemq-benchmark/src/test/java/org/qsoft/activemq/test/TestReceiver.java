package org.qsoft.activemq.test;

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
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("queueTest");
			// create connection,session,consumer and receive message
			QueueConnection conn = factory.createQueueConnection();
			conn.start();
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiver = session.createReceiver(queue, "score=10");

			int index = 0;
			while (index++ < 1000) {
				TextMessage msg = (TextMessage) receiver.receive();
				System.out.println("*********");
				System.out.println(msg.getIntProperty("score"));
				System.out.println(msg.getText());
			}

			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
