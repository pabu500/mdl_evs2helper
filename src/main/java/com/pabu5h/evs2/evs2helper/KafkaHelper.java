package com.pabu5h.evs2.evs2helper;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
@Service
public class KafkaHelper {
    private final Logger logger = Logger.getLogger(KafkaHelper.class.getName());
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;
    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;
    @Value("${spring.kafka.properties.sasl.client.callback.handler.class}")
    private String saslClientCallbackHandlerClass;

    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put("security.protocol", securityProtocol);
        config.put("sasl.mechanism", saslMechanism);
        config.put("sasl.jaas.config", saslJaasConfig);
        config.put("sasl.client.callback.handler.class", saslClientCallbackHandlerClass);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<String, String>();

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "ktest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put("security.protocol", securityProtocol);
        config.put("sasl.mechanism", saslMechanism);
        config.put("sasl.jaas.config", saslJaasConfig);
        config.put("sasl.client.callback.handler.class", saslClientCallbackHandlerClass);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        return factory;
    }
    public void describeTopicConfig(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(resource));
            Map<ConfigResource, Config> configMap = describeConfigsResult.all().get();

            Config config = configMap.get(resource);

            config.entries().forEach(configEntry ->
                    logger.info("Config: " + configEntry.name() + " = " + configEntry.value())
            );
        }catch (ExecutionException | InterruptedException e) {
//            e.printStackTrace();
            logger.severe(e.getMessage());
        }
    }

    public void describeTopicOffsets(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<TopicPartition, OffsetSpec> query = new HashMap<>();
            for (TopicPartitionInfo info : adminClient.describeTopics(Collections.singletonList(topicName)).topicNameValues().get(topicName).get().partitions()) {
                query.put(new TopicPartition(topicName, info.partition()), OffsetSpec.earliest());
            }

            ListOffsetsResult result = adminClient.listOffsets(query);
            for (TopicPartition partition : query.keySet()) {
                ListOffsetsResult.ListOffsetsResultInfo info = result.all().get().get(partition);
                long firstOffset = info.offset();
                long lastOffset = adminClient.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest())).all().get().get(partition).offset();
                logger.info("Partition: " + partition.partition() + ", First Offset: " + firstOffset + ", Last Offset: " + lastOffset + ", Approximate Messages: " + (lastOffset - firstOffset));
            }
        }catch (ExecutionException | InterruptedException e) {
            logger.severe(e.getMessage());
        }
    }
    public void send(String topic, String key, String message, Integer logSetting) {

        CompletableFuture<SendResult<String, String>> future = null;
        if(key == null || key.isBlank()){
            future = kafkaTemplate.send(topic, message);
        }else {
            future = kafkaTemplate.send(topic, key, message);
        }
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.severe(ex.getMessage());
            } else {
                ProducerRecord<String, String> producerRecord = result.getProducerRecord();
                RecordMetadata recordMetadata = result.getRecordMetadata();
                //0: no log, 1: log meta, 2: log message, 3: log message and meta
                if(logSetting == 1) {
                    logger.info("t:" + producerRecord.topic() + " p:" + recordMetadata.partition() + ", o:" + recordMetadata.offset() + ", k:" + producerRecord.key());
                }else if(logSetting == 2) {
                    logger.info("v:" + producerRecord.value());
                }else if(logSetting == 3) {
                    logger.info("t:" + producerRecord.topic() + " p:" + recordMetadata.partition() + ", o:" + recordMetadata.offset() + ", k:" + producerRecord.key() + ", v:" + producerRecord.value());
                }
            }
        });
    }
    public void getBrokerStat() {
        logger.info("Broker Stat:");
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            listTopicsResult.names().get().forEach(topicName -> {
                logger.info("Topic: " + topicName);
                try {
                    TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topicName)).values().get(topicName).get();
                    Map<TopicPartition, OffsetSpec> queryLatest = new HashMap<>();
                    Map<TopicPartition, OffsetSpec> queryEarliest = new HashMap<>();

                    for (TopicPartitionInfo info : topicDescription.partitions()) {
                        queryLatest.put(new TopicPartition(topicName, info.partition()), OffsetSpec.latest());
                        queryEarliest.put(new TopicPartition(topicName, info.partition()), OffsetSpec.earliest());
                    }

                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(queryLatest).all().get();
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = adminClient.listOffsets(queryEarliest).all().get();

                    for (TopicPartitionInfo info : topicDescription.partitions()) {
                        TopicPartition tp = new TopicPartition(topicName, info.partition());
                        long latestOffset = latestOffsets.get(tp).offset();
                        long earliestOffset = earliestOffsets.get(tp).offset();
                        long numberOfMessages = latestOffset - earliestOffset;

                        logger.info("Partition: " + info.partition() + ", Leader: " + info.leader().id() + ", Replicas: " + info.replicas().size() + ", ISR: " + info.isr().size());
                        logger.info("Approximate number of messages: " + numberOfMessages);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.severe(e.getMessage());
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            logger.severe(e.getMessage());
        }
    }
    public void readAllMessagesFromPartition(String topic, int partition) {
        Map<String, Object> consumerProps = consumerFactory.getConfigurationProperties();
        Properties properties = new Properties();
        properties.putAll(consumerProps);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, String> consumer = consumerFactory.createConsumer("my-group", "client-id", null, properties)) {
            TopicPartition partition0 = new TopicPartition(topic, partition);
            consumer.assign(Collections.singletonList(partition0));
            consumer.seekToBeginning(Collections.singletonList(partition0));
            //get all messages from partition
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                logger.info("Received message. topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset() + ", key: " + record.key() + ", value: " + record.value());
            });

        }
    }
    public void consumeFromTo(LocalDateTime fromTime, LocalDateTime toTime) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-time-based-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        long fromTimeMillis = fromTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long toTimeMillis = toTime.toInstant(ZoneOffset.UTC).toEpochMilli();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition0 = new TopicPartition("my_topic", 0);
            consumer.assign(Collections.singletonList(partition0));

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            timestampsToSearch.put(partition0, fromTimeMillis);

            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);

            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition0);
            long startOffset = offsetAndTimestamp.offset();

            consumer.seek(partition0, startOffset);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    long timestamp = record.timestamp();
                    if (timestamp <= toTimeMillis) {
                        logger.info("Received message. topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset() + ", key: " + record.key() + ", value: " + record.value());
                    } else {
                        consumer.close();
                        return;
                    }
                });
            }
        }
    }
    public void flushTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Resource representing the topic configuration
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            // Config entry for a very small retention
            ConfigEntry retentionEntry = new ConfigEntry("retention.ms", "100");

            // Create and apply the new configuration
            Config config = new Config(Collections.singletonList(retentionEntry));
            Map<ConfigResource, Config> configs = new HashMap<>();
            configs.put(topicResource, config);
            AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configs);
            alterConfigsResult.all().get();

            // Wait a little bit for the messages to expire
            Thread.sleep(200);

            // Reset the retention to the original value (you should replace 604800000 with the original value)
            ConfigEntry originalRetentionEntry = new ConfigEntry("retention.ms", "604800000");
            Config originalConfig = new Config(Collections.singletonList(originalRetentionEntry));
            Map<ConfigResource, Config> originalConfigs = new HashMap<>();
            originalConfigs.put(topicResource, originalConfig);
            AlterConfigsResult originalAlterConfigsResult = adminClient.alterConfigs(originalConfigs);
            originalAlterConfigsResult.all().get();
        } catch (Exception e) {
            // Handle exception
        }
    }
}
