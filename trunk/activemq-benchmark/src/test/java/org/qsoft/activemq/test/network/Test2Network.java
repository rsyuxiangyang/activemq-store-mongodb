package org.qsoft.activemq.test.network;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class Test2Network {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factoryA = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queueB = new ActiveMQQueue("kk.b");
			// create connection,session,consumer and receive message
			QueueConnection connA = factoryA.createQueueConnection();
			connA.start();
			
			// first receiver on broker1
			QueueSession sessionA1 = connA.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiverA1 = sessionA1.createReceiver(queueB);
			final AtomicInteger aint1 = new AtomicInteger(0);
			MessageListener listenerA1 = new MessageListener(){
				public void onMessage(Message message) {
					try {
						System.out.println(aint1.incrementAndGet()+" => A1 receive from kk.b: " + ((TextMessage)message).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}};
			receiverA1.setMessageListener(listenerA1 );
			
			// second receiver on broker1
			QueueSession sessionA2 = connA.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiverA2 = sessionA2.createReceiver(queueB);
			final AtomicInteger aint2 = new AtomicInteger(0);
			MessageListener listenerA2 = new MessageListener(){
				public void onMessage(Message message) {
					try {
						System.out.println(aint2.incrementAndGet()+" => A2 receive from kk.b: " + ((TextMessage)message).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}};
			receiverA2.setMessageListener(listenerA2 );
			
			// a fake one on broker1
			QueueReceiver receiverA3 = sessionA2.createReceiver(queueB);
			final AtomicInteger aint3 = new AtomicInteger(0);
			MessageListener listenerA3 = new MessageListener(){
				public void onMessage(Message message) {
					try {
						System.out.println(aint3.incrementAndGet()+" => A3 receive from kk.b: " + ((TextMessage)message).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}};
			receiverA3.setMessageListener(listenerA3 );
			
			QueueConnectionFactory factoryB = new ActiveMQConnectionFactory("tcp://127.0.0.1:61618");
			Queue queueB1 = new ActiveMQQueue("kk.b");
			QueueConnection connB = factoryB.createQueueConnection();
			connB.start();
			
			// one receiver on broker2
			QueueSession sessionB1 = connB.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiverB1 = sessionB1.createReceiver(queueB);
			final AtomicInteger bint1 = new AtomicInteger(0);
			MessageListener listenerB1 = new MessageListener(){
				public void onMessage(Message message) {
					try {
						System.out.println(bint1.incrementAndGet()+" => B1 receive from kk.b: " + ((TextMessage)message).getText());
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}};
				receiverB1.setMessageListener(listenerB1 );
			
			// producer  on broker2
			QueueSession sessionBp = connB.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = sessionBp.createProducer(queueB1);
			int index = 0;
			while(index++<300){
				TextMessage message = sessionBp.createTextMessage(index + " from kk.b on broker2");
				producer.send(message);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
