package org.qsoft.activemq.store.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBHelper {

	private static final String MSGS = "activemq_msgs";

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoDBHelper.class);

	Mongo mongo;
	DB db;
	WireFormat wireFormat;

	public MongoDBHelper(String host, int port, String dbName,
			WireFormat wireFormat) {
		LOG
				.info("Connect to MongoDB[" + host + ":" + port + ":" + dbName
						+ "]");
		try {
			mongo = new Mongo(host, port);
			db = mongo.getDB(dbName);
		} catch (UnknownHostException e) {
			LOG.error("error host.", e);
			throw new RuntimeException(e);
		} catch (MongoException e) {
			LOG.error("MongoException.", e);
			throw new RuntimeException(e);
		}

		this.wireFormat = wireFormat;
	}

	public DBCollection getMsgsCollection() {
		return this.db.getCollection(MSGS);
	}

	public Boolean addMessage(Message message) throws IOException {
		BasicDBObject bo = new BasicDBObject();
		MessageId messageId = message.getMessageId();
		// Serialize the Message..
		byte data[];
		try {
			ByteSequence packet = wireFormat.marshal(message);
			data = ByteSequenceData.toByteArray(packet);
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message: "
					+ messageId + " in container: " + e, e);
		}
		bo.append("ID", messageId.getBrokerSequenceId());
		bo.append("CONTAINER", message.getDestination().getQualifiedName());
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getBrokerSequenceId());
		bo.append("EXPIRATION", message.getExpiration());
		bo.append("MSG", data);
		bo.append("PRIORITY", message.getPriority());
		getMsgsCollection().save(bo);

		return true;
	}

	public static void main(String[] args) {

	}

	public Message getMessage(MessageId messageId) throws IOException {

		BasicDBObject bo = new BasicDBObject();
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getBrokerSequenceId());
		DBObject o = getMsgsCollection().findOne(bo);
		if (o == null)
			return null;
		byte[] data = (byte[]) o.get("MSG");
		if (data == null)
			return null;

		Message answer = null;
		try {
			answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message: "
					+ messageId + " in container: " + e, e);
		}
		return answer;
	}

	public int count() {
		return (int) getMsgsCollection().count();
	}

	public void removeMessage(MessageAck ack) {
		MessageId messageId = ack.getLastMessageId();
		BasicDBObject bo = new BasicDBObject();
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getBrokerSequenceId());
		DBObject o = getMsgsCollection().findOne(bo);
		getMsgsCollection().remove(o);
	}

	public void removeAllMessages() {

	}

	public Message findOne() throws IOException {
		DBObject o = getMsgsCollection().findOne();
		if (o == null)
			return null;
		byte[] data = (byte[]) o.get("MSG");
		if (data == null)
			return null;

		Message answer = null;
		try {
			answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
		} catch (IOException e) {
			throw IOExceptionSupport.create(
					"Failed to broker message in container: " + e, e);
		}
		return answer;
	}

	public List<Message> find(int limit) throws IOException {
		List<Message> msgs = new ArrayList<Message>(limit);
		DBCursor c = getMsgsCollection().find().limit(limit);
		while (c.hasNext()) {
			DBObject o = c.next();
			if (o == null)
				return null;
			byte[] data = (byte[]) o.get("MSG");
			if (data == null)
				return null;
			Message answer = null;
			try {
				answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
				msgs.add(answer);
			} catch (IOException e) {
				throw IOExceptionSupport.create(
						"Failed to broker message in container: " + e, e);
			}
		}
		return msgs;
	}

}
