package com.pabu5h.evs2.evs2helper;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Duration;
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
    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

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
    public void send(String topic, String key, String message, boolean log) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.severe(ex.getMessage());
            } else {
                ProducerRecord<String, String> producerRecord = result.getProducerRecord();
                RecordMetadata recordMetadata = result.getRecordMetadata();
                if(log){
                    logger.info("message sent. topic: " + producerRecord.topic() + ", partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset() + ", key: " + producerRecord.key() + ", value: " + producerRecord.value());
                }
            }
        });
    }
    public void getBrokerStat() {
        System.out.println("Broker Stat:");
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            listTopicsResult.names().get().forEach(topicName -> {
                System.out.println("Topic: " + topicName);
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

                        System.out.println("Partition: " + info.partition() + ", Leader: " + info.leader().id() + ", Replicas: " + info.replicas().size() + ", ISR: " + info.isr().size());
                        System.out.println("Approximate number of messages: " + numberOfMessages);
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
                System.out.println("Received message. topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset() + ", key: " + record.key() + ", value: " + record.value());
            });

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
