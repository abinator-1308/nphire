package com.noticeperiodreferrals.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafka {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "nphire";
        String topic = "nphire_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
       properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create Consumer
        KafkaConsumer <String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        ConsumerRecords<String,String> records  = consumer.poll(Duration.ofMillis(10));

        for(ConsumerRecord<String,String> record : records) {
            logger.info("New Record : value - " + record.value() + "key - " + record.key());
            logger.info("Partition" + record.partition() +" Timestamp" + record.timestamp() + "offset" + record.offset());
        }

    }
}
