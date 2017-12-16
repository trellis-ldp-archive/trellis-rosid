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
package org.trellisldp.rosid.app.health;

import static java.util.Collections.singletonList;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.health.HealthCheck;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class KafkaHealthCheckTest {

    private static TestingServer zk;

    @Mock
    private CuratorFramework mockClient;

    @Mock
    private CuratorZookeeperClient mockZkClient;

    @Mock
    private ZooKeeper mockZookeeper;

    @BeforeAll
    public static void setUp() throws Exception {
        zk = new TestingServer(true);
    }

    @BeforeEach
    public void setUpMocks() throws Exception {
        initMocks(this);
        when(mockClient.getZookeeperClient()).thenReturn(mockZkClient);
        when(mockZkClient.isConnected()).thenReturn(true);
        when(mockZkClient.getZooKeeper()).thenReturn(mockZookeeper);
        when(mockZookeeper.getChildren("/brokers/ids", false)).thenReturn(singletonList("/brokers/ids/1"));
    }

    @Test
    public void testZkHealth() throws Exception {

        final CuratorFramework client = newClient(zk.getConnectString(), new RetryOneTime(100));
        client.start();
        client.blockUntilConnected();

        final HealthCheck check = new KafkaHealthCheck(client);
        final HealthCheck.Result res = check.execute();
        assertFalse(res.isHealthy());
        assertTrue(res.getMessage().contains("Error fetching kafka broker list"));

        client.createContainers("/brokers/ids");

        final HealthCheck.Result res2 = check.execute();
        assertFalse(res2.isHealthy());
        assertEquals("No Kafka brokers are connected.", res2.getMessage());

        client.createContainers("/brokers/ids/1");

        final HealthCheck.Result res3 = check.execute();
        assertTrue(res3.isHealthy());
    }

    @Test
    public void testNonConnected() throws Exception {
        when(mockZkClient.isConnected()).thenReturn(false);
        final HealthCheck check = new KafkaHealthCheck(mockClient);
        final HealthCheck.Result res = check.execute();
        assertFalse(res.isHealthy());
        assertEquals("Zookeeper client not connected", res.getMessage());
    }

    @Test
    public void testKeeperException() throws Exception {
        when(mockZookeeper.getChildren("/brokers/ids", false)).thenThrow(new KeeperException.NoNodeException());

        final HealthCheck check = new KafkaHealthCheck(mockClient);
        final HealthCheck.Result res = check.execute();
        assertFalse(res.isHealthy());
        assertTrue(res.getMessage().contains("Error fetching kafka broker list"));
    }

    @Test
    public void testException() throws Exception {
        when(mockZkClient.getZooKeeper()).thenThrow(new Exception());

        final HealthCheck check = new KafkaHealthCheck(mockClient);
        final HealthCheck.Result res = check.execute();
        assertFalse(res.isHealthy());
        assertTrue(res.getMessage().contains("Error checking on Kafka"));
    }
}
