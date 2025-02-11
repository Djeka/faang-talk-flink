package com.emolokov.faang_talk_flink.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class MockKafkaBroker {
    private static final Logger log = LoggerFactory.getLogger(MockKafkaBroker.class);

    private static final String LOCALHOST = "127.0.0.1";

    private final String name;
    private String zkConnect;
    private String kafkaConnect;
    private EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;

    public static MockKafkaBroker startNewKafkaBroker(String name) throws IOException {
        return new MockKafkaBroker(name);
    }

    private MockKafkaBroker(String name) throws IOException {
        this.name = name;

        startZookeeper();
        startKafkaServer(name);
    }

    private void startZookeeper(){
        // setup Zookeeper
        System.setProperty("zookeeper.maxCnxns", "100");
        this.zkServer = new EmbeddedZookeeper();
        this.zkConnect = LOCALHOST + ":" + zkServer.port();
        log.info("Zookeeper started at: " + zkConnect);
    }

    private void startKafkaServer(String name) throws IOException {
        int kafkaBrokerPort = findFreePort();
        this.kafkaConnect =  LOCALHOST +":" + kafkaBrokerPort;


        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", this.zkConnect);
        brokerProps.setProperty("broker.id", Integer.toString(Math.abs(name.hashCode() % 1000)));
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + kafkaConnect);
        brokerProps.setProperty("offsets.topic.replication.factor" , "1");
        brokerProps.setProperty("transaction.state.log.replication.factor", "1");
        brokerProps.setProperty("transaction.state.log.min.isr", "1");
        brokerProps.setProperty("transaction.coordinator.timeout.ms", "60000");
        brokerProps.setProperty("auto.create.topics.enable", "true");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new SystemTime();
        this.kafkaServer = TestUtils.createServer(config, mock, Option.apply(name));
    }

    public void createTopic(String topicName) throws InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created: " + topicName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Integer findFreePort(){
        try{
            ServerSocket s = new ServerSocket(0);
            int port = s.getLocalPort();
            s.close();
            return port;
        } catch (Exception e){
            log.error("Mock Kafka Broker couldn't find a free port", e);
            throw new RuntimeException("Mock Kafka Broker couldn't find a free port");
        }
    }

    public void stop(){
        kafkaServer.shutdown();
        zkServer.shutdown();
    }

    public String getBootstrapServers(){
        return this.kafkaConnect;
    }
}
