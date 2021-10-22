package my.diploma.thesis.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import my.diploma.thesis.config.KafkaConfig;
import my.diploma.thesis.config.TwitterConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer extends Thread {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private Client client;
    private KafkaProducer<String, String> producer;
    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(30);
    private final List<String> trackTerms;

    public TwitterProducer(List<String> terms) {
        trackTerms = terms;
    }

    // Twitter Client
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Setting up a connection   */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
        // Term that I want to search on Twitter
        hbEndpoint.trackTerms(trackTerms);
        // Twitter API and tokens
        Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS, TwitterConfig.TOKEN, TwitterConfig.SECRET);

        /** Creating a client   */
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hbEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    //Kafka Producer
    private KafkaProducer<String, String> createKafkaProducer() {
        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConfig.MAX_IN_FLIGHT_CONN);

        // Additional settings for high throughput producer
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.BATCH_SIZE);

        // Create producer
        return new KafkaProducer<>(prop);
    }

    @Override
    public void run() {
        logger.info("Setting up");

        // 1. Call the Twitter Client
        client = createTwitterClient(msgQueue);
        client.connect();

        // 2. Create Kafka Producer
        producer = createKafkaProducer();

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Application is not stopping!");
            client.stop();
            logger.info("Closing Producer");
            producer.close();
            logger.info("Finished closing");
        }));

        // 3. Send Tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, null, msg),
                        (recordMetadata, e) -> {
                            if (e != null) {
                                logger.error("Some error OR something bad happened", e);
                            }
                        });
            }
        }
        logger.info("\n Application End");
    }

    public void terminate() {
        client.stop();
    }
}