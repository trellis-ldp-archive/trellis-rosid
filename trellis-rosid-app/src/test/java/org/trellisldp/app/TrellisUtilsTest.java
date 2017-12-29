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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dropwizard.auth.AuthFilter;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.trellisldp.rosid.app.config.TrellisConfiguration;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class TrellisUtilsTest {

    @Test
    public void testGetAssetConfigurations() throws Exception {
        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));

        final Map<String, String> assets = TrellisUtils.getAssetConfiguration(config);
        assertEquals(3L, assets.size());
        assertEquals("http://example.org/image.icon", assets.get("icon"));
        assertEquals("http://example.org/styles1.css,http://example.org/styles2.css",
                assets.get("css"));
        assertEquals("http://example.org/scripts1.js,http://example.org/scripts2.js",
                assets.get("js"));
    }

    @Test
    public void testGetKafkaProperties() throws Exception {
        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));

        final Properties props = TrellisUtils.getKafkaProperties(config);

        assertEquals("org.apache.kafka.common.serialization.StringSerializer", props.getProperty("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", props.getProperty("value.serializer"));
        assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
    }

    @Test
    public void testGetWebacConfig() throws Exception {
        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));


        assertTrue(TrellisUtils.getWebacConfiguration(config).isPresent());

        config.getAuth().getWebac().setEnabled(false);

        assertFalse(TrellisUtils.getWebacConfiguration(config).isPresent());
    }

    @Test
    public void testGetCORSConfig() throws Exception {
        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));


        assertTrue(TrellisUtils.getCorsConfiguration(config).isPresent());

        config.getCors().setEnabled(false);

        assertFalse(TrellisUtils.getCorsConfiguration(config).isPresent());
    }

    @Test
    public void testGetCurator() throws Exception {
        final TestingServer zk = new TestingServer(true);

        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));

        config.getZookeeper().setEnsembleServers(zk.getConnectString());

        final CuratorFramework curator = TrellisUtils.getCuratorClient(config);
        assertEquals(CuratorFrameworkState.STARTED, curator.getState());
    }

    @Test
    public void testGetAuthFilters() throws Exception {
        final TrellisConfiguration config = new YamlConfigurationFactory<>(TrellisConfiguration.class,
                Validators.newValidator(), Jackson.newObjectMapper(), "")
            .build(new File(getClass().getResource("/config1.yml").toURI()));

        final Optional<List<AuthFilter>> filters = TrellisUtils.getAuthFilters(config);
        assertTrue(filters.isPresent());
        filters.ifPresent(f -> assertEquals(3L, f.size()));

        config.getAuth().getAnon().setEnabled(false);
        config.getAuth().getBasic().setEnabled(false);
        config.getAuth().getJwt().setEnabled(false);

        assertFalse(TrellisUtils.getAuthFilters(config).isPresent());
    }

}
