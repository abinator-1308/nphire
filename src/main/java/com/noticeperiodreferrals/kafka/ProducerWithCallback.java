package com.noticeperiodreferrals.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
                , StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer =  new KafkaProducer<String, String>(properties);

        // adding key ensures that same key always goes to same partition (Important)
        //create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<>(
                "nphire_topic" , "id_"+ Integer.toString(1),"hello world"
        );

        //send data - async
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null)
                {

                    logger.info("Received new metadata. \n",
                            "Topic - "+metadata.topic() +"\n"+
                             "Partition -" + metadata.partition()+ "\n",
                            "Offset - " + metadata.offset() + "\n",
                            "TimeStamp - " + metadata.timestamp() + "\n");
                    //record successfully send
                }
                else
                    logger.error("Error while producing ",exception);
            }
        });

        //flush
        producer.flush();

        //close
        producer.close();

    }
}
