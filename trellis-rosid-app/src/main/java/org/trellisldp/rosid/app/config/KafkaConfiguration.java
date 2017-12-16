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
package org.trellisldp.rosid.app.config;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hibernate.validator.constraints.NotEmpty;

/**
 * @author acoburn
 */
public class KafkaConfiguration {

    private static final String DEFAULT_ACKS = "all";
    private static final Integer DEFAULT_BATCH_SIZE = 16384;
    private static final Integer DEFAULT_RETRIES = 0;
    private static final Integer DEFAULT_LINGER_MS = 1;
    private static final Integer DEFAULT_BUFFER_MEMORY = 33554432;

    @NotEmpty
    private String bootstrapServers;

    private final Map<String, String> other = new HashMap<>();

    /**
     * Create a new Kafka configuration
     */
    public KafkaConfiguration() {
        other.put("acks", DEFAULT_ACKS);
        other.put("batch.size", DEFAULT_BATCH_SIZE.toString());
        other.put("retries", DEFAULT_RETRIES.toString());
        other.put("linger.ms", DEFAULT_LINGER_MS.toString());
        other.put("buffer.memory", DEFAULT_BUFFER_MEMORY.toString());
    }

    /**
     * Set the kafka bootstrap server locations
     * @param bootstrapServers a comma-delimited list of kafka servers
     */
    @JsonProperty
    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Get the kafka bootstrap server locations
     * @return the kafka bootstrap server locations
     */
    @JsonProperty
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Set configuration values dynamically
     * @param name the configuration name
     * @param value the value
     */
    @JsonAnySetter
    public void set(final String name, final String value) {
        other.put(name, value);
    }

   /**
     * Get all configuration values in a Properties object
     * @return the properties
     */
    public Properties asProperties() {
        final Properties props = new Properties();
        other.forEach(props::setProperty);
        props.setProperty("bootstrap.servers", bootstrapServers);
        return props;
    }
}


