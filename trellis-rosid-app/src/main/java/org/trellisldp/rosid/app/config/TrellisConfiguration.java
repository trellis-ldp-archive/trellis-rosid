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

import io.dropwizard.Configuration;

import java.util.List;

import javax.validation.constraints.NotNull;

/**
 * @author acoburn
 */
public class TrellisConfiguration extends Configuration {

    private Boolean async = false;

    private Integer cacheMaxAge = 86400;

    @NotNull
    private AuthConfiguration auth = new AuthConfiguration();

    @NotNull
    private String defaultName = "Trellis";

    @NotNull
    private AssetConfiguration assets = new AssetConfiguration();

    @NotNull
    private ResourceConfiguration resources;

    @NotNull
    private BinaryConfiguration binaries;

    @NotNull
    private String baseUrl;

    @NotNull
    private KafkaConfiguration kafka;

    @NotNull
    private ZookeeperConfiguration zookeeper;

    @NotNull
    private CORSConfiguration cors = new CORSConfiguration();

    @NotNull
    private NamespaceConfiguration namespaces = new NamespaceConfiguration();

    private List<String> whitelist;

    private List<String> whitelistDomains;

    private Long profileCacheSize = 100L;

    private Long profileCacheExpireHours = 24L;

    /**
     * Set async mode
     * @param async true if async mode is enabled; false otherwise
     */
    @JsonProperty
    public void setAsync(final Boolean async) {
        this.async = async;
    }

    /**
     * Get async mode
     * @return true if async mode is enabled; false otherwise
     */
    @JsonProperty
    public Boolean getAsync() {
        return async;
    }

    /**
     * Get the base URL for the partition
     * @return the partition baseURL
     */
    @JsonProperty
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Set the base URL for the partition
     * @param baseUrl the partition baseURL
     */
    @JsonProperty
    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Get the LDP-RS configuration
     * @return the LDP-RS configuration
     */
    @JsonProperty
    public ResourceConfiguration getResources() {
        return resources;
    }

    /**
     * Set the LDP-RS resource configuration
     * @param config the LDP-RS resource configuration
     */
    @JsonProperty
    public void setResources(final ResourceConfiguration config) {
        this.resources = config;
    }

    /**
     * Get the binary configuration
     * @return the binary configuration
     */
    @JsonProperty
    public BinaryConfiguration getBinaries() {
        return binaries;
    }

    /**
     * Set the binary configuration
     * @param config the binary configuration
     */
    @JsonProperty
    public void setBinaries(final BinaryConfiguration config) {
        this.binaries = config;
    }


    /**
     * Set the asset configuration
     * @param assets the asset config
     */
    @JsonProperty
    public void setAssets(final AssetConfiguration assets) {
        this.assets = assets;
    }

    /**
     * Get the asset configuration
     * @return the asset config
     */
    @JsonProperty
    public AssetConfiguration getAssets() {
        return assets;
    }

    /**
     * Get the application name
     * @return the name
     */
    @JsonProperty
    public String getDefaultName() {
        return defaultName;
    }

    /**
     * Set the application name
     * @param name the name
     */
    @JsonProperty
    public void setDefaultName(final String name) {
        this.defaultName = name;
    }

    /**
     * Set the kafka configuration
     * @param config the Kafka configuration
     */
    @JsonProperty
    public void setKafka(final KafkaConfiguration config) {
        this.kafka = config;
    }

    /**
     * Get the kafka configuration
     * @return the kafka configuration
     */
    @JsonProperty
    public KafkaConfiguration getKafka() {
        return kafka;
    }

    /**
     * Set the zookeeper configuration
     * @param zookeeper the zookeeper configuration
     */
    @JsonProperty
    public void setZookeeper(final ZookeeperConfiguration zookeeper) {
        this.zookeeper = zookeeper;
    }

    /**
     * Get the zookeeper configuration
     * @return the zookeeper configuration
     */
    @JsonProperty
    public ZookeeperConfiguration getZookeeper() {
        return zookeeper;
    }

    /**
     * Set the cache max-age value
     * @param cacheMaxAge the cache max age header value
     */
    @JsonProperty
    public void setCacheMaxAge(final Integer cacheMaxAge) {
        this.cacheMaxAge = cacheMaxAge;
    }

    /**
     * Get the value of the cache max age
     * @return the cache max age header value
     */
    @JsonProperty
    public Integer getCacheMaxAge() {
        return cacheMaxAge;
    }

    /**
     * Set the CORS configuration
     * @param cors the CORS configuration
     */
    @JsonProperty
    public void setCors(final CORSConfiguration cors) {
        this.cors = cors;
    }

    /**
     * Get the CORS configuration
     * @return the CORS configuration
     */
    @JsonProperty
    public CORSConfiguration getCors() {
        return cors;
    }

    /**
     * Set the Auth configuration
     * @param auth the Auth configuration
     */
    @JsonProperty
    public void setAuth(final AuthConfiguration auth) {
        this.auth = auth;
    }

    /**
     * Get the Auth configuration
     * @return the Auth configuration
     */
    @JsonProperty
    public AuthConfiguration getAuth() {
        return auth;
    }

    /**
     * Set the namespaces configuration
     * @param namespaces the namespaces configuration
     */
    @JsonProperty
    public void setNamespaces(final NamespaceConfiguration namespaces) {
        this.namespaces = namespaces;
    }

    /**
     * Get the namespace configuration
     * @return the namespace configuration
     */
    @JsonProperty
    public NamespaceConfiguration getNamespaces() {
        return namespaces;
    }

    /**
     * Get the whitelist of custom JSON-LD profiles
     * @return the json-ld profile whitelist
     */
    @JsonProperty
    public List<String> getJsonLdWhitelist() {
        return whitelist;
    }

    /**
     * Set the whitelist of custom JSON-LD profiles.
     * @param whitelist the json-ld profile witelist
     */
    @JsonProperty
    public void setJsonLdWhitelist(final List<String> whitelist) {
        this.whitelist = whitelist;
    }

    /**
     * Get the domain whitelist of custom JSON-LD profiles
     * @return the json-ld profile domain whitelist
     */
    @JsonProperty
    public List<String> getJsonLdDomainWhitelist() {
        return whitelistDomains;
    }

    /**
     * Set the domain whitelist of custom JSON-LD profiles.
     * @param whitelistDomains the json-ld domain profile witelist
     */
    @JsonProperty
    public void setJsonLdDomainWhitelist(final List<String> whitelistDomains) {
        this.whitelistDomains = whitelistDomains;
    }

    /**
     * Get the JSON-LD profile cache expire time in hours (default=24).
     * @return the json-ld profile cache expire time in hours
     */
    @JsonProperty
    public Long getJsonLdCacheExpireHours() {
        return profileCacheExpireHours;
    }

    /**
     * Set the JSON-LD profile cache exire time in hours.
     * @param profileCacheExpireHours the json-ld profile cache expire time in hours.
     */
    @JsonProperty
    public void setJsonLdCacheExpireHours(final Long profileCacheExpireHours) {
        this.profileCacheExpireHours = profileCacheExpireHours;
    }

    /**
     * Get the JSON-LD profile cache size (default=100).
     * @return the json-ld profile cache size
     */
    @JsonProperty
    public Long getJsonLdCacheSize() {
        return profileCacheSize;
    }

    /**
     * Set the JSON-LD profile cache size.
     * @param profileCacheSize the size of the json-ld profile cache
     */
    @JsonProperty
    public void setJsonLdCacheSize(final Long profileCacheSize) {
        this.profileCacheSize = profileCacheSize;
    }
}
