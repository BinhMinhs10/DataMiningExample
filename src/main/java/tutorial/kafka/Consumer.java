package tutorial.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;



    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "user_registered";

        new Consumer(server, groupId, topic).run();
    }

    Consumer(String mBootstrapServer, String mGroupId, String mTopic){
        this.mBootstrapServer = mBootstrapServer;
        this.mGroupId = mGroupId;
        this.mTopic = mTopic;
    }
    private Properties consumerProps(String bootstrapServer, String groupId){
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        return properties;
    }



    void run(){
        mLogger.info("Creating consumer thread");

        /**
         * CountDownLatch a kind of barrier
         */
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnale consumerRunnale = new ConsumerRunnale(mBootstrapServer, mGroupId, mTopic, latch );
        Thread thread = new Thread(consumerRunnale);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mLogger.info("Catch shutdown hook");
            consumerRunnale.shutdown();
            await(latch);

            mLogger.info("Application has exited");
        }));

        await(latch);
    }

    void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            mLogger.error("Application got interrupted", e);
        } finally {
            mLogger.info("Application is closing");
        }
    }

    // Run consumer in separate thread
    private class ConsumerRunnale implements Runnable{
        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;

        ConsumerRunnale(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            mLatch = latch;
            Properties props = consumerProps(bootstrapServer, groupId);
            mConsumer = new KafkaConsumer<String, String>(props);
            mConsumer.subscribe(Collections.singletonList(topic));
        }

        /**
         * Here we define a polling mechanism to fetch data from a topic every 100 milliseconds
         */
        @Override
        public void run(){
            try{
                do{
                    ConsumerRecords<String, String> records = mConsumer.poll(100);

                    for(ConsumerRecord<String, String> record:  records){
                        mLogger.info("Key: " + record.key()+ ", value: "+ record.value());
                        mLogger.info("Partition: " + record.partition()+ ", Offset: "+ record.offset());
                    }
                }while (true);
            }catch (WakeupException e){
                mLogger.info("Received shutdown signal");
            }finally {
                mConsumer.close();
                mLatch.countDown();
            }
        }

        void shutdown(){
            mConsumer.close();
        }
    }


}
