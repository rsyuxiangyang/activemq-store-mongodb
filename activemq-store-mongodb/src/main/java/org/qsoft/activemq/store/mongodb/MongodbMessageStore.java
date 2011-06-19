package org.qsoft.activemq.store.mongodb;

import java.io.IOException;
import java.util.List;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbMessageStore extends AbstractMessageStore {

	protected final WireFormat wireFormat;
	protected final MongoDBHelper helper;
	private static final Logger LOG = LoggerFactory
			.getLogger(MongodbMessageStore.class);

	public MongodbMessageStore(ActiveMQDestination destination,
			WireFormat wireFormat, MongoDBHelper helper) {
		super(destination);
		this.wireFormat = wireFormat;
		this.helper = helper;
	}

	@Override
	public void addMessage(ConnectionContext context, Message message)
			throws IOException {
		LOG.debug("MongodbMessageStore.addMessage: " + message);
		this.helper.addMessage(message);
	}

	@Override
	public Message getMessage(MessageId identity) throws IOException {
		LOG.debug("MongodbMessageStore.getMessage:{0}", identity);
		return this.helper.getMessage(identity);
	}

	@Override
	public void removeMessage(ConnectionContext context, MessageAck ack)
			throws IOException {
		LOG.debug("MongodbMessageStore.removeMessage: " + context + "," + ack);
		this.helper.removeMessage(ack);
	}

	@Override
	public void removeAllMessages(ConnectionContext context) throws IOException {
		LOG.debug("MongodbMessageStore.removeAllMessages");
		this.helper.removeAllMessages();
	}

	@Override
	public void recover(MessageRecoveryListener container) throws Exception {
		LOG.debug("MongodbMessageStore.recover: " + container);
	}

	@Override
	public int getMessageCount() throws IOException {
		LOG.debug("MongodbMessageStore.getMessageCount");
		return this.helper.count();
	}

	@Override
	public void resetBatching() {
		LOG.debug("MongodbMessageStore.resetBatching");
	}

	@Override
	public void recoverNextMessages(int maxReturned,
			MessageRecoveryListener listener) throws Exception {
		LOG.debug("MongodbMessageStore.recoverNextMessages: " + maxReturned
				+ "," + listener);
		// Message message = this.helper.findOne();
		List<Message> msgs = this.helper.find(maxReturned);
		for (Message message : msgs) {
			listener.recoverMessage(message);
		}
		LOG.debug("MongodbMessageStore.recoverNextMessages: " + msgs.size()
				+ " ~DONE!");

	}

}
