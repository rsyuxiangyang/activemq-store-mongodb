package org.qsoft.activemq.test.ha;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.transport.TransportListener;

public class TestReconnect {

	public static void main(String[] args) {
		try {

			BrokerService bs = new BrokerService();
			bs.addConnector("tcp://127.0.0.1:61616");
			bs.setStartAsync(false);
			bs.start();

			ActiveMQConnectionFactory factoryA = new ActiveMQConnectionFactory(
					"failover:(tcp://127.0.0.1:61616)");

			Topic destB = new ActiveMQTopic("kk.adt");
			ActiveMQConnection connB = (ActiveMQConnection) factoryA
					.createConnection();

			connB.addTransportListener(new TransportListener() {

				@Override
				public void onCommand(Object command) {
					System.out.println("===>> onCommand:" + command);
				}

				@Override
				public void onException(IOException error) {
					System.out.println("===>> onException:"
							+ error.getMessage());
				}

				@Override
				public void transportInterupted() {
					System.out.println("===>> transportInterupted");
				}

				@Override
				public void transportResumed() {
					System.out.println("===>> transportResumed");
				}
			});

			connB.start();
			TopicSession sessionB = connB.createTopicSession(false,
					Session.AUTO_ACKNOWLEDGE);

			MessageConsumer consumer = sessionB.createConsumer(destB);
			final AtomicInteger aint3 = new AtomicInteger(0);
			MessageListener listenerA3 = new MessageListener() {
				public void onMessage(Message message) {
					try {
						System.out.println(aint3.incrementAndGet()
								+ " => receive from kk.ad: " + message);
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

			bs = restart(bs);

			bs = restart(bs);
			
			bs = restart(bs);

			bs.stop();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static BrokerService restart(BrokerService bs) throws Exception,
			InterruptedException {
		bs.stop();

		Thread.sleep(1000);

		bs = new BrokerService();
		bs.addConnector("tcp://127.0.0.1:61616");
		bs.setStartAsync(false);
		bs.start();

		Thread.sleep(1000);
		return bs;
	}

}
