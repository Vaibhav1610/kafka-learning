package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
       log.info("produce code");

        //create producer properties
        Properties properties  = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++)
        {
            String topic = "demo_java";
            String value= "message: "+i;
            String key = "id_"+i;

            //create producer record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);

            //send data - asynchronous operation
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e==null)
                    {
                        log.info("received new metadata \n"+
                                "Topic: "+metadata.topic()+"\n"+
                                "Partition: "+metadata.partition()+"\n"+
                                "key: "+producerRecord.key()+"\n"+
                                "Offset: "+metadata.offset()+"\n"+
                                "TimeStamp: "+metadata.timestamp());
                    }
                    else
                    {
                        log.error("Error while producing",e);
                    }
                }
            });
        }


        //flush and close the producer
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
