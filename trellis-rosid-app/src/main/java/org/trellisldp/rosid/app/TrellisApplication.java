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
package org.trellisldp.rosid.app;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.isNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.trellisldp.rosid.app.TrellisUtils.getAuthFilters;
import static org.trellisldp.rosid.app.TrellisUtils.getCorsConfiguration;
import static org.trellisldp.rosid.app.TrellisUtils.getKafkaProperties;
import static org.trellisldp.rosid.app.TrellisUtils.getWebacConfiguration;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_EVENT;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_NAMESPACES;

import io.dropwizard.Application;
import io.dropwizard.auth.chained.ChainedAuthFilter;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.trellisldp.agent.SimpleAgent;
import org.trellisldp.api.BinaryService;
import org.trellisldp.api.CacheService;
import org.trellisldp.api.IOService;
import org.trellisldp.api.IdentifierService;
import org.trellisldp.api.NamespaceService;
import org.trellisldp.api.ResourceService;
import org.trellisldp.binary.FileBasedBinaryService;
import org.trellisldp.http.AgentAuthorizationFilter;
import org.trellisldp.http.CacheControlFilter;
import org.trellisldp.http.CrossOriginResourceSharingFilter;
import org.trellisldp.http.LdpResource;
import org.trellisldp.http.WebAcFilter;
import org.trellisldp.id.UUIDGenerator;
import org.trellisldp.io.JenaIOService;
import org.trellisldp.kafka.KafkaPublisher;
import org.trellisldp.rosid.app.config.TrellisConfiguration;
import org.trellisldp.rosid.app.health.KafkaHealthCheck;
import org.trellisldp.rosid.app.health.ZookeeperHealthCheck;
import org.trellisldp.rosid.common.Namespaces;
import org.trellisldp.rosid.file.FileResourceService;
import org.trellisldp.webac.WebACService;

/**
 * @author acoburn
 */
public class TrellisApplication extends Application<TrellisConfiguration> {

    /**
     * The main entry point
     * @param args the argument list
     * @throws Exception if something goes horribly awry
     */
    public static void main(final String[] args) throws Exception {
        new TrellisApplication().run(args);
    }

    @Override
    public String getName() {
        return "Trellis LDP";
    }

    @Override
    public void initialize(final Bootstrap<TrellisConfiguration> bootstrap) {
        // Not currently used
    }

    @Override
    public void run(final TrellisConfiguration config,
                    final Environment environment) throws IOException {

        // Partition data configuration
        final String dataLocation = config.getResources().getPath();

        // Partition BaseURL configuration
        final String baseUrl = config.getBaseUrl();

        final CuratorFramework curator = TrellisUtils.getCuratorClient(config);

        final Producer<String, String> producer = new KafkaProducer<>(getKafkaProperties(config));

        final IdentifierService idService = new UUIDGenerator();

        final ResourceService resourceService = new FileResourceService(dataLocation, baseUrl, curator,
                producer, new KafkaPublisher(producer, TOPIC_EVENT), idService.getSupplier(), config.getAsync());

        final NamespaceService namespaceService = new Namespaces(curator, new TreeCache(curator, ZNODE_NAMESPACES),
                config.getNamespaces().getFile());

        final BinaryService binaryService = new FileBasedBinaryService(config.getBinaries().getPath(),
                idService.getSupplier("file:", config.getBinaries().getLevels(), config.getBinaries().getLength()));

        // IO Service
        final Set<String> whitelist = isNull(config.getJsonLdWhitelist())
            ? emptySet() : new HashSet<>(config.getJsonLdWhitelist());
        final Set<String> whitelistDomains = isNull(config.getJsonLdDomainWhitelist())
            ? emptySet() : new HashSet<>(config.getJsonLdDomainWhitelist());
        final CacheService<String, String> profileCache = new TrellisCache<>(newBuilder()
                .maximumSize(config.getJsonLdCacheSize())
                .expireAfterAccess(config.getJsonLdCacheExpireHours(), HOURS).build());
        final IOService ioService = new JenaIOService(namespaceService, TrellisUtils.getAssetConfiguration(config),
                whitelist, whitelistDomains, profileCache);

        // Health checks
        environment.healthChecks().register("zookeeper", new ZookeeperHealthCheck(curator));
        environment.healthChecks().register("kafka", new KafkaHealthCheck(curator));

        getAuthFilters(config).ifPresent(filters -> environment.jersey().register(new ChainedAuthFilter<>(filters)));

        // Resource matchers
        environment.jersey().register(new LdpResource(resourceService, ioService, binaryService, baseUrl));

        // Filters
        environment.jersey().register(new AgentAuthorizationFilter(new SimpleAgent(), emptyList()));
        environment.jersey().register(new CacheControlFilter(config.getCacheMaxAge()));

        // Authorization
        getWebacConfiguration(config).ifPresent(webacCache ->
            environment.jersey().register(new WebAcFilter(
                        asList("Authorization"), new WebACService(resourceService, webacCache))));

        // CORS
        getCorsConfiguration(config).ifPresent(cors -> environment.jersey().register(
                new CrossOriginResourceSharingFilter(cors.getAllowOrigin(), cors.getAllowMethods(),
                    cors.getAllowHeaders(), cors.getExposeHeaders(), cors.getAllowCredentials(), cors.getMaxAge())));
    }
}
