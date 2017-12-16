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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.curator.utils.ZKPaths.PATH_SEPARATOR;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_NAMESPACES;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.trellisldp.api.NamespaceService;
import org.trellisldp.api.RuntimeRepositoryException;

/**
 * @author acoburn
 */
public class Namespaces implements NamespaceService {

    private static final Logger LOGGER = getLogger(Namespaces.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TreeCache cache;

    private final CuratorFramework client;

    private final Map<String, String> data = new ConcurrentHashMap<>();

    /**
     * Create a zookeeper-based namespace service
     * @param client the curator client
     * @param cache the treecache
     */
    public Namespaces(final CuratorFramework client, final TreeCache cache) {
        this(client, cache, null);
    }

    /**
     * Create a zookeeper-based namespace service
     * @param client the curator client
     * @param cache the treecache
     * @param filePath the file
     */
    public Namespaces(final CuratorFramework client, final TreeCache cache, final String filePath) {
        requireNonNull(cache, "TreeCache may not be null!");
        this.client = client;
        this.cache = cache;
        try {
            this.client.create().orSetData().forPath(ZNODE_NAMESPACES);
            this.cache.getListenable().addListener((c, e) -> {
                final Map<String, ChildData> tree = cache.getCurrentChildren(ZNODE_NAMESPACES);
                readTree(tree).forEach(data::put);
            });
            init(filePath).forEach(data::put);
        } catch (final Exception ex) {
            LOGGER.error("Could not create a zk node cache: {}", ex);
            throw new RuntimeRepositoryException(ex);
        }
    }

    @Override
    public Map<String, String> getNamespaces() {
        return unmodifiableMap(data);
    }

    @Override
    public Optional<String> getNamespace(final String prefix) {
        return ofNullable(data.get(prefix));
    }

    @Override
    public Optional<String> getPrefix(final String namespace) {
        return data.entrySet().stream().filter(e -> e.getValue().equals(namespace)).map(Map.Entry::getKey).findFirst();
    }

    @Override
    public Boolean setPrefix(final String prefix, final String namespace) {
        if (data.containsKey(prefix)) {
            return false;
        }

        data.put(prefix, namespace);
        try {
            client.create().orSetData().forPath(ZNODE_NAMESPACES + PATH_SEPARATOR + prefix, namespace.getBytes(UTF_8));
            return true;
        } catch (final Exception ex) {
            LOGGER.error("Unable to set data: {}", ex.getMessage());
        }
        return false;
    }

    private Map<String, String> init(final String filePath) throws Exception {
        final Map<String, String> namespaces = new HashMap<>();
        if (nonNull(filePath)) {
            try (final FileInputStream input = new FileInputStream(new File(filePath))) {
                // TODO - JDK9 InputStream::readAllBytes
                final Map<String, String> ns = read(IOUtils.toByteArray(input));
                for (final Map.Entry<String, String> entry : ns.entrySet()) {
                    client.create().orSetData().forPath(ZNODE_NAMESPACES + PATH_SEPARATOR + entry.getKey(),
                            entry.getValue().getBytes(UTF_8));
                    namespaces.put(entry.getKey(), entry.getValue());
                }
            } catch (final IOException ex) {
                LOGGER.warn("Unable to read provided file: {}", ex.getMessage());
            }
        }
        readTree(cache.getCurrentChildren(ZNODE_NAMESPACES)).forEach(namespaces::put);
        return namespaces;
    }

    private static Map<String, String> readTree(final Map<String, ChildData> data) {
        if (nonNull(data)) {
            return data.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> new String(e.getValue().getData(), UTF_8)));
        }
        return emptyMap();
    }

    private static Map<String, String> read(final byte[] data) throws IOException {
        final Map<String, String> namespaces = new HashMap<>();
        of(MAPPER.readTree(data)).filter(JsonNode::isObject).ifPresent(json ->
            json.fields().forEachRemaining(node -> {
                if (node.getValue().isTextual()) {
                    namespaces.put(node.getKey(), node.getValue().textValue());
                } else {
                    LOGGER.warn("Ignoring non-textual node at key: {}", node.getKey());
                }
            }));
        return namespaces;
    }
}
