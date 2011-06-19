package org.qsoft.activemq.store.mongodb;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbPersistenceAdapter implements PersistenceAdapter,
		BrokerServiceAware {
	
	private static final Logger LOG = LoggerFactory
	.getLogger(MongodbPersistenceAdapter.class);
	
	protected MongoDBHelper helper;
	private WireFormat wireFormat = new OpenWireFormat();
	
	private String host;
	private int port;
	private String db;
	
	private BrokerService brokerService;
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	@Override
	public void start() throws Exception {
		helper = new MongoDBHelper(host,port,db,wireFormat);
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBrokerService(BrokerService brokerService) {
		this.brokerService = brokerService;		
	}

	@Override
	public Set<ActiveMQDestination> getDestinations() {
		// TODO Auto-generated method stub
		return new HashSet<ActiveMQDestination>();
	}

	@Override
	public MessageStore createQueueMessageStore(ActiveMQQueue destination)
			throws IOException {
		return new MongodbMessageStore(destination, wireFormat, helper);
	}

	@Override
	public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination)
			throws IOException {
		return new MongodbTopicMessageStore(destination, wireFormat, helper);
	}

	@Override
	public void removeQueueMessageStore(ActiveMQQueue destination) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeTopicMessageStore(ActiveMQTopic destination) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TransactionStore createTransactionStore() throws IOException {
		// TODO Auto-generated method stub
		return new MongodbTransactionStore();
	}

	@Override
	public void beginTransaction(ConnectionContext context) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitTransaction(ConnectionContext context) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackTransaction(ConnectionContext context)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getLastMessageBrokerSequenceId() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deleteAllMessages() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setUsageManager(SystemUsage usageManager) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setBrokerName(String brokerName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDirectory(File dir) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkpoint(boolean sync) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLastProducerSequenceId(ProducerId id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
