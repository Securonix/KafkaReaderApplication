/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.snypr.kafkareader;

import java.util.UUID;
import com.securonix.snyper.config.beans.HadoopConfigBean;
import com.securonix.snyper.config.beans.KafkaConfigBean;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.securonix.kafkaclient.KafkaUtil;
import com.securonix.application.common.EventCompressor;
import com.securonix.kafka.CustomDefaultDecoder;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.Logger;

/**
 * This EnrichedTopicReader class reads data from Enriched Topic, and store
 * corresponding JSON data into a file.
 *
 * @author ManishKumar
 * @version 1.0
 * @since 2018-05-19
 */
public class EnrichedTopicReader {

    /**
     * Logger for the class
     */
    private final static Logger LOGGER = LogManager.getLogger();

    /**
     * Entry point for Application.This reads data from Enriched Topic.EEO
     * Object gets converted into JSON form, and gets store into a file.
     *
     * @param args Command line arguments
     *
     * @throws Exception in case of an error initializing the Application
     */
    public static void main(String args[]) throws Exception {

        // kafkaConfigBean is required to get Kafka consumer to read data from configured Topic.       
        KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();

        //Enriched Topic is configured to read data. 
        kafkaConfigBean.setEnrichedTopic("alpha-Enriched");

        // Data gets compressed to improve performance.Here copression type is set as gzip.
        kafkaConfigBean.setCompressionType("gzip");

        // Kafka brokers are configured here. Kafka brokers are required to read data from Kafka topic in distributed (Clustered) system.
        kafkaConfigBean.setBrokers("192-168-1-101.securonix.com:9092,192-168-1-103.securonix.com:9092");

        // groupId is defind for a consumer group.
        final String groupId = "EnrichedopicReader-" + UUID.randomUUID().toString();

        // props is used for Kafka Consumer initialisation.
        final Properties props = new Properties();

        //we can choose the earliest policy for AUTO_OFFSET_RESET_CONFIG, so the new consumer group starts from the beginning every time.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 
        // Unique groupId is defind for this Cunsumer Group and added into Kafka properties.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //provides the initial hosts that act as the starting point for a Kafka client to discover the full set of alive servers in the cluster
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192-168-1-101.securonix.com:9092,192-168-1-103.securonix.com:9092");
        
        
        //ENABLE_AUTO_COMMIT_CONFIG If true, periodically commit to Kafka the offsets of messages already returned by the consumer. This committed offset will be used when the process fails as the position from which the consumption will begin.

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try {

            // Initialise Kafka Consumer by passing all required kafka parameters.
            KafkaUtil.addSSLPropertiesToConsumer(props, groupId, kafkaConfigBean, org.apache.kafka.common.serialization.StringDeserializer.class.getName(), CustomDefaultDecoder.class.getName(), HadoopConfigBean.KAFKA_SOURCE.CONSOLE);

            // Get Kafka Consumer
            KafkaConsumer<String, byte[]> consumer = KafkaUtil.getKafkaConsumer(kafkaConfigBean, props);

            // Subscribe Enriched Topic for data poll.
            consumer.subscribe(Arrays.asList(kafkaConfigBean.getEnrichedTopic()));

            // jsonList is used to store all JSON requests (fetched from enriched topic).
            List<String> jsonList = new ArrayList<>();
            
            // File initializations done for writing data into confiured file.
            
            FileWriter fstream = null;
            BufferedWriter bw = null;
            
            // Logical counter can be used to control reading from kafka topic.
            int readCounter = 0;
            while (true) {

                // records can be polled withing configured time interval (in millisecond).
                ConsumerRecords<String, byte[]> records = (ConsumerRecords<String, byte[]>) consumer.poll(1000);
                if (records != null) {
                    String json = null;
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {

                            // Data gets uncompressed and create equivalent JSON message.
                            json = EventCompressor.uncompressString(record.value(), kafkaConfigBean.getCompressionType());
                        } catch (Exception ex) {
                            LOGGER.error("Error in uncompressing", ex);
                        }
                        if (json != null) {

                            // JSON message gets added into jsonList. 
                            jsonList.add(json);

                        };

                        //We can add logical condition to limit the feed processing.
                        if (readCounter > 0) {
                            break;
                        }

                        readCounter++;

                    }

                    if (!jsonList.isEmpty()) {

                        // Populate JSON data into given file path.
                        try {
                            fstream = new FileWriter("/Users/manishkumar/Documents/manish/code/tempFolder/enriched.txt", true);
                            bw = new BufferedWriter(fstream);
                            for (String str : jsonList) {
                                bw.write(str);
                                bw.newLine();
                            }
                            break;

                        } catch (IOException e) {
                            LOGGER.error("Failed to write JSON input into file" + e);
                        } finally {
                            if (bw != null) {
                                try {
                                    bw.close();
                                } catch (IOException ex) {
                                    LOGGER.error("Failed to close the connection" + ex);
                                }
                            }
                            if (fstream != null) {
                                try {
                                    fstream.close();
                                } catch (IOException ex) {
                                    LOGGER.error("Failed to close the connection" + ex);
                                }
                            }
                        }

                    }
                }

            }

        } catch (Exception e) {
            LOGGER.error("Failed to read from Enriched topic" + e);
        }

    }

}
