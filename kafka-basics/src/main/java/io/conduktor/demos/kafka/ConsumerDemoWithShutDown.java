package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());

    public static void main(String[] args) {
       log.info("Consume code");

       String groupId = "my-second-application";
       String topic = "demo_java";
        //create producer properties
        Properties properties  = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //get areference to the current thread
        final Thread mainThread = Thread.currentThread();

        //add shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run()
            {
                log.info("Detected a shutdown,exit by calling consumer.wakeup");
                consumer.wakeup();

                //koin the main thread
                try
                {
                    mainThread.join();;
                }catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to our topic

            consumer.subscribe(Arrays.asList(topic));

            //poll for new data

            while(true)
            {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String,String> record : records){
                    log.info("Key: "+record.key()+" Value: "+record.value());
                    log.info("partition: "+record.partition() + "offset: "+record.offset());
                }
            }
        }catch (WakeupException e)
        {
            log.info("wake up exception");
            //we ignore this as this is a expected exception when closing a consumer
        }catch (Exception e)
        {
            log.error("unexpected exception");
        }finally {
            consumer.close(); // this will also commit the offst if needed
            log.info("The Consumer is now gracefully closed");
        }




    }
}
