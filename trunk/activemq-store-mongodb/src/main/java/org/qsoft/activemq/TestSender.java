package org.qsoft.activemq;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class TestSender {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try
		   {
		    //init connection factory with activemq
		    QueueConnectionFactory factory=new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
		    //specify the destination
		    Queue queue=new ActiveMQQueue("queueTest");
		    //create connection,session,producer and deliver message
		    QueueConnection conn=factory.createQueueConnection();
		    QueueSession session=conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		    QueueSender sender=session.createSender(queue);
		    String msgText= "testMessage";
		   
		     TextMessage msg=session.createTextMessage(msgText);
		     sender.send(msg);
		    
		     session.close();
		    conn.close();
		   }
		   catch(Exception e)
		   {
		    e.printStackTrace();
		    System.exit(1);
		   }

	}

}
