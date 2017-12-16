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
package org.trellisldp.rosid.common;

/**
 * @author acoburn
 */
public final class RosidConstants {

    /**
     * The topic that notifies about object changes
     */
    public static final String TOPIC_EVENT = "trellis.event";

    /**
     * The topic that is used to add LDP membership triples
     */
    public static final String TOPIC_LDP_MEMBERSHIP_ADD = "trellis.ldpmembership.add";

    /**
     * The topic that is used to remove LDP membership triples
     */
    public static final String TOPIC_LDP_MEMBERSHIP_DELETE = "trellis.ldpmembership.delete";

    /**
     * The topic that is used to add LDP containment triples
     */
    public static final String TOPIC_LDP_CONTAINMENT_ADD = "trellis.ldpcontainment.add";

    /**
     * The topic that is used to remove LDP containment triples
     */
    public static final String TOPIC_LDP_CONTAINMENT_DELETE = "trellis.ldpcontainment.delete";

    /**
     * The aggregated (windowed) topic that notifies the cache to be regenerated
     */
    public static final String TOPIC_CACHE_AGGREGATE = "trellis.cache.aggregate";

    /**
     * The topic that regenerates the cache
     */
    public static final String TOPIC_CACHE = "trellis.cache";

    /**
     * The ZooKeeper coordination parent node
     */
    public static final String ZNODE_COORDINATION = "/session";

    /**
     * The ZooKeeper namespace data node
     */
    public static final String ZNODE_NAMESPACES = "/namespaces";

    private RosidConstants() {
        // prevent instantiation
    }
}
