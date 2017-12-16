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

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static com.codahale.metrics.health.HealthCheck.Result.unhealthy;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_COORDINATION;

import com.codahale.metrics.health.HealthCheck;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author acoburn
 */
public class ZookeeperHealthCheck extends HealthCheck {

    protected final CuratorFramework client;

    /**
     * Create an object that checks the health of a zk ensemble
     * @param client the curator client
     */
    public ZookeeperHealthCheck(final CuratorFramework client) {
        super();
        this.client = client;
    }

    @Override
    protected HealthCheck.Result check() throws InterruptedException {
        try {
            if (!client.getZookeeperClient().isConnected()) {
                return unhealthy("Zookeeper client not connected");
            } else if (!client.getZookeeperClient().getZooKeeper().getState().isAlive()) {
                return unhealthy("Zookeeper ensemble is not alive");
            } else if (client.checkExists().forPath(ZNODE_COORDINATION) == null) {
                return unhealthy("Zookeeper not properly initialized");
            }
            return healthy("Zookeeper appears to be healthy");
        } catch (final Exception ex) {
            return unhealthy("Error checking on Zookeeper: " + ex.getMessage());
        }
    }
}
