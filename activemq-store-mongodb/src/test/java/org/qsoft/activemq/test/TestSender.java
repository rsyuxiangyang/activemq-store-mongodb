package org.qsoft.activemq.test;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;

public class TestSender {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616?wireFormat.maxInactivityDuration=300000");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,producer and deliver message
			QueueConnection conn = factory.createQueueConnection();
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueSender sender = session.createSender(queue);
			for (int i = 0; i < 100000; i++) {
				String msgText = "testMessage-"+i;
				TextMessage msg = session.createTextMessage(msgText);
				
				//if(msg.getClass().getName().indexOf("ActiveMQTextMessage") > -1)
				//if(i%2 == 1)
					msg.setIntProperty("score", 10);
				sender.send(msg);
			}
			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
