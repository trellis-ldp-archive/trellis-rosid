/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trellisldp.rosid.file.streaming;

import static java.lang.Long.parseLong;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE_AGGREGATE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_EVENT;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_DELETE;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.slf4j.Logger;

import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
public class FileProcessingPipeline {

    private static final Logger LOGGER = getLogger(FileProcessingPipeline.class);

    private final String dataConfiguration;
    private final String baseUrlConfiguration;
    private final String bootstrapServers;
    private final long aggregateSeconds;

    /**
     * Build a file processing pipeline
     * @param configuration the configuration
     */
    public FileProcessingPipeline(final Properties configuration) {
        this.bootstrapServers = configuration.getProperty("kafka.bootstrapServers");
        this.aggregateSeconds = parseLong(configuration.getProperty("trellis.aggregateSeconds", "1"));
        this.dataConfiguration = configuration.getProperty("trellis.data");
        this.baseUrlConfiguration = configuration.getProperty("trellis.baseUrl");
    }

    /**
     * Get the beam pipeline
     * @return the pipeline
     */
    public Pipeline getPipeline() {

        LOGGER.debug("Building Beam Pipeline");
        final PipelineOptions options = PipelineOptionsFactory.create();
        final Pipeline p = Pipeline.create(options);

        // Add membership triples
        p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_LDP_MEMBERSHIP_ADD).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferMembership.getIRIString(), true)))
            .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE));

        // Delete membership triples
        p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_LDP_MEMBERSHIP_DELETE).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferMembership.getIRIString(), false)))
            .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE));

        // Add containment triples
        p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_LDP_CONTAINMENT_ADD).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), true)))
            .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE));

        // Delete containment triples
        p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_LDP_CONTAINMENT_DELETE).withoutMetadata())
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), false)))
            .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE));

        if (aggregateSeconds > 0) {
            // Aggregate cache writes
            p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE).withoutMetadata())
                .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(aggregateSeconds)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(aggregateSeconds)))
                        .discardingFiredPanes().withAllowedLateness(Duration.ZERO))
                .apply(Combine.perKey(x -> x.iterator().next()))
                .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE));
        } else {
            // Skip aggregation
            p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_CACHE_AGGREGATE).withoutMetadata())
                .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_CACHE));
        }

        // Write to cache and dispatch to the event bus
        p.apply(getKafkaReader(bootstrapServers).withTopic(TOPIC_CACHE).withoutMetadata())
            .apply(ParDo.of(new CacheWriter(dataConfiguration)))
            .apply(ParDo.of(new EventProcessor(baseUrlConfiguration)))
            .apply(getKafkaWriter(bootstrapServers).withTopic(TOPIC_EVENT));

        return p;
    }

    private static KafkaIO.Read<String, String> getKafkaReader(final String bootstrapServers) {
        return KafkaIO.<String, String>read().withBootstrapServers(bootstrapServers)
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class);
    }

    private static KafkaIO.Write<String, String> getKafkaWriter(final String bootstrapServers) {
        return KafkaIO.<String, String>write().withBootstrapServers(bootstrapServers)
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class);
    }

    /**
     * Load a FileProcessingPipeline
     * @param args the arguments
     * @return the beam pipeline
     * @throws IOException in the event of an error loading the configuration
     */
    public static Pipeline loadPipeline(final String[] args) throws IOException {
        if (args.length >= 1) {
            LOGGER.debug("Loading Beam Pipeline with {}", args[0]);
            return new FileProcessingPipeline(loadConfiguration(args[0])).getPipeline();
        }
        LOGGER.error("No configuration file provided");
        throw new IOException("No configuration file provided");
    }

    /**
     * Load the configuration for the pipeline
     * @param filename the configuration filename
     * @return the configuration properties
     * @throws IOException in the event of an error loading the configuration
     */
    public static Properties loadConfiguration(final String filename) throws IOException {
        final Properties config = new Properties();
        try (final InputStream input = new FileInputStream(filename)) {
            LOGGER.debug("Loading configuration from {}", filename);
            config.load(input);
            return config;
        }
    }
}
