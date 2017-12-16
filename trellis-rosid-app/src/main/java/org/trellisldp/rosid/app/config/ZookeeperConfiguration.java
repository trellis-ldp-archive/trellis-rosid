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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * @author acoburn
 */
public class ZookeeperConfiguration {

    private static final Integer DEFAULT_TIMEOUT = 1000;
    private static final Integer DEFAULT_RETRY_MS = 2000;
    private static final Integer DEFAULT_RETRY_MAX = 10;
    private static final Integer DEFAULT_RETRY_MAX_MS = 30000;

    @NotNull
    private String ensemble;

    private Integer timeout = DEFAULT_TIMEOUT;

    private Integer retryMs = DEFAULT_RETRY_MS;

    private Integer retryMax = DEFAULT_RETRY_MAX;

    private Integer retryMaxMs = DEFAULT_RETRY_MAX_MS;

    /**
     * Set the zookeeper ensemble server locations
     * @param ensemble a comma-delimited list of zookeeper servers
     */
    @JsonProperty
    public void setEnsembleServers(final String ensemble) {
        this.ensemble = ensemble;
    }

    /**
     * Get the zookeeper ensemble server locations
     * @return the ensemble server locations
     */
    @JsonProperty
    public String getEnsembleServers() {
        return ensemble;
    }

    /**
     * Get the timeout value in milliseconds
     * @return the timout
     */
    @JsonProperty
    public Integer getTimeout() {
        return timeout;
    }

    /**
     * Set the timeout value in milliseconds
     * @param timeout the timeout
     */
    @JsonProperty
    public void setTimeout(final Integer timeout) {
        this.timeout = timeout;
    }

    /**
     * Get the retry value in milliseconds
     * @return the retry value
     */
    @JsonProperty
    public Integer getRetryMs() {
        return retryMs;
    }

    /**
     * Set the retry value in milliseconds
     * @param retryMs the retry value
     */
    @JsonProperty
    public void setRetryMs(final Integer retryMs) {
        this.retryMs = retryMs;
    }

    /**
     * Get the max retry value
     * @return the max retry value
     */
    @JsonProperty
    public Integer getRetryMax() {
        return retryMax;
    }

    /**
     * Set the max retry value
     * @param retryMax the max retry value
     */
    @JsonProperty
    public void setRetryMax(final Integer retryMax) {
        this.retryMax = retryMax;
    }

    /**
     * Get the max retry value in milliseconds
     * @return the max retry value
     */
    @JsonProperty
    public Integer getRetryMaxMs() {
        return retryMaxMs;
    }

    /**
     * Set the max retry value in milliseconds
     * @param retryMaxMs the max retry value
     */
    @JsonProperty
    public void setRetryMaxMs(final Integer retryMaxMs) {
        this.retryMaxMs = retryMaxMs;
    }
}

