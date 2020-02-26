package com.github.dhoard;

import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static String BOOTSTRAP_SERVERS = "cp-5-4-0.address.cx:9092,cp-5-4-0-2.address.cx:9092";

    private static final String TOPIC = "TEST";

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final int[] statusCodes = { 200, 400 };

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, String> kafkaProducer = null;
        ProducerRecord<String, String> producerRecord = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());

            kafkaProducer = new KafkaProducer<String, String>(properties);

            for (int index = 0; index < statusCodes.length; index++) {
                int statusCode = statusCodes[index];
                String uuid = UUID.randomUUID().toString();

                JSONObject jsonObject = new JSONObject();

                jsonObject.put("statusCode", statusCode);
                jsonObject.put("delay", 10);
                jsonObject.put("data", "test message " + uuid);

                String key = "key";
                String value = jsonObject.toString();

                logger.info("produce message = [" + value + "]");

                producerRecord = new ProducerRecord<String, String>(TOPIC, key, value);

                ExtendedCallback extendedCallback = new ExtendedCallback(producerRecord);
                Future<RecordMetadata> future = kafkaProducer.send(producerRecord, extendedCallback);

                future.get();

                //logger.info("isError = [" + extendedCallback.isError() + "]");
            }
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
            }
        }

        try {
            Thread.sleep(10000);
        } catch (Throwable t) {
            // DO NOTHING
        }
    }

    public class ExtendedCallback implements Callback {

        private ProducerRecord producerRecord;


        private RecordMetadata recordMetadata;
        private Exception exception;

        public ExtendedCallback(ProducerRecord<String, String> producerRecord) {
            this.producerRecord = producerRecord;
        }

        public boolean isError() {
            return (null != this.exception);
        }

        private RecordMetadata getRecordMetadata() {
            return this.recordMetadata;
        }

        public Exception getException() {
            return this.exception;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;

            if (null == exception) {
                //logger.info("Received, key = [" + producerRecord.key() + "] value = [" + producerRecord.value() + "] topic = [" + recordMetadata.topic() + "] partition = [" + recordMetadata.partition() + "] offset = [" + recordMetadata.offset() + "] timestamp = [" + toISOTimestamp(recordMetadata.timestamp(), "America/New_York") + "]");
            }

            this.exception = exception;
        }
    }

    private static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private static long randomLong(int min, int max) {
        return + (long) (Math.random() * (max - min));
    }
}
