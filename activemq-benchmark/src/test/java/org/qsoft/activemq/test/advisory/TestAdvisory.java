package org.qsoft.activemq.test.advisory;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

public class TestAdvisory {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// init connection factory with activemq
			ActiveMQConnectionFactory factoryA = new ActiveMQConnectionFactory(
					"tcp://127.0.0.1:61616");
			TopicConnection connA = factoryA.createTopicConnection();
			connA.start();
			TopicSession sessionA1 = connA.createTopicSession(false,
					Session.AUTO_ACKNOWLEDGE);
			
//			Topic topicQ = new ActiveMQTopic("ActiveMQ.Advisory.Topic");
//			TopicSubscriber subscriberQ = sessionA1.createSubscriber(topicQ);
//			final AtomicInteger aint1 = new AtomicInteger(0);
//			MessageListener listenerA1 = new MessageListener() {
//				public void onMessage(Message message) {
//					try {
//						System.out.println(aint1.incrementAndGet()
//								+ " => Advisory receive from ActiveMQ.Advisory.Topic: "
//								+   message );
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			};
//			subscriberQ.setMessageListener(listenerA1);
			
			Topic topicP = new ActiveMQTopic("ActiveMQ.Advisory.Producer.Topic..>");
			//Destination advisoryDestination = AdvisorySupport.getProducerAdvisoryTopic(topicP);
			MessageConsumer consumerP = sessionA1.createConsumer(topicP);
			final AtomicInteger aint2 = new AtomicInteger(0);
			MessageListener listenerA2 = new MessageListener() {
				public void onMessage(Message message) {
					try {
						System.out.println(aint2.incrementAndGet()
								+ " => Advisory receive from ActiveMQ.Advisory.Producer.Topic: "
								+   message );
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			consumerP.setMessageListener(listenerA2);
			
			Topic topicC = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic..>");
			//Destination advisoryDestinationC = AdvisorySupport.getProducerAdvisoryTopic(topicC);
			MessageConsumer consumerC = sessionA1.createConsumer(topicC);
			final AtomicInteger aintC = new AtomicInteger(0);
			MessageListener listenerAC = new MessageListener() {
				public void onMessage(Message message) {
					try {
						System.out.println(aintC.incrementAndGet()
								+ " => Advisory receive from ActiveMQ.Advisory.Consumer.Topic: "
								+   message );
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			consumerC.setMessageListener(listenerAC);
			
			Thread.sleep(1000);

			// 
			Topic destB = new ActiveMQTopic("kk.adt");
			TopicConnection connB = factoryA.createTopicConnection();
			connB.start();
			TopicSession sessionB = connB.createTopicSession(false,
					Session.AUTO_ACKNOWLEDGE);
			
			MessageConsumer consumer = sessionB.createConsumer(destB);
			final AtomicInteger aint3 = new AtomicInteger(0);
			MessageListener listenerA3 = new MessageListener() {
				public void onMessage(Message message) {
					try {
						System.out.println(aint3.incrementAndGet()
								+ " => receive from kk.ad: "
								+   message );
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			consumer.setMessageListener(listenerA3);
			
			MessageProducer producer = sessionB.createProducer(destB);
			int index = 0;
			while (index++ < 1) {
				TextMessage message = sessionB.createTextMessage(index
						+ " message.");
				producer.send(message);
			}
//			sessionB.close();
//			connB.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

}
