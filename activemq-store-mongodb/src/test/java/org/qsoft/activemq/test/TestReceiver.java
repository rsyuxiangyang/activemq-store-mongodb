package org.qsoft.activemq.test;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
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
		
		int a = 0;
		if(a== 1)
		listen();
		else
		receive();

	}

	private static void receive() {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,consumer and receive message
			QueueConnection conn = factory.createQueueConnection();
			conn.start();
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiver = session.createReceiver(queue);//, "score=10");

			int index = 0;
			while (index++ < 100000) {
				Message msg = receiver.receive();
				//System.out.println("*********");
				//System.out.println(msg.getIntProperty("score"));
				//System.out.println(msg.getText());
				if((index+1) % 100 == 0) 
					System.out.println((index+1)+ " - " + msg.getJMSMessageID());
			}

			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static void listen() {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,consumer and receive message
			QueueConnection conn = factory.createQueueConnection();
			conn.start();

			// first receiver on broker1
			QueueSession sessionA1 = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiverA1 = sessionA1.createReceiver(queue);
			final AtomicInteger aint1 = new AtomicInteger(0);
			MessageListener listenerA1 = new MessageListener(){
				public void onMessage(Message message) {
//					try {
//						System.out.println(aint1.incrementAndGet()+" => A1 receive from kk.mongo: " + ((TextMessage)message).getText());
//					} catch (JMSException e) {
//						e.printStackTrace();
//					}
				}};
			receiverA1.setMessageListener(listenerA1 );
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
