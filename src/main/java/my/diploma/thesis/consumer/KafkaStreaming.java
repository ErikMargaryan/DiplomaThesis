package my.diploma.thesis.consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreaming extends Thread {
    private KafkaStreams streams;

    @Override
    public void run() {

        // Kafka Streams
        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-application");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks = builder.stream("tweets");
        KStream<String, String> mappedKs = ks.map(
                (key, tweetJson) -> KeyValue.pair(key, "{\"username\":\"" + JsonParser.parseString(tweetJson)
                        .getAsJsonObject()
                        .get("user")
                        .getAsJsonObject()
                        .get("screen_name")
                        .getAsString() + "\",\"tweet\":\"" + JsonParser.parseString(tweetJson)
                        .getAsJsonObject()
                        .get("text")
                        .getAsString() + "\",\"created_at\":\"" + JsonParser.parseString(tweetJson)
                        .getAsJsonObject()
                        .get("created_at")
                        .getAsString() + "\"}"));

        mappedKs.to("info");

        streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }

    public void terminate() {
        streams.close();
    }
}
