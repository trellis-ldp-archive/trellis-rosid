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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.curator.utils.ZKPaths.PATH_SEPARATOR;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_NAMESPACES;

import java.net.URL;
import java.math.BigInteger;
import java.security.SecureRandom;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateBuilder2;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.trellisldp.api.NamespaceService;
import org.trellisldp.api.RuntimeRepositoryException;
import org.trellisldp.vocabulary.JSONLD;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class NamespacesTest {

    private static final String nsDoc = "/testNamespaces.json";
    private static TestingServer curator;

    @Mock
    private static CuratorFramework mockCurator;

    @Mock
    private TreeCache mockCache;

    @Mock
    private ListenerContainer<TreeCacheListener> mockListenable;

    @Mock
    private CreateBuilder mockCreateBuilder;

    @Mock
    private CreateBuilder2 mockCreateBuilder2;

    @BeforeAll
    public static void setUp() throws Exception {
        curator = new TestingServer(true);
    }

    @BeforeEach
    public void setUpMocks() throws Exception {
        initMocks(this);
        when(mockCurator.create()).thenReturn(mockCreateBuilder);
        when(mockCreateBuilder.orSetData()).thenReturn(mockCreateBuilder2);
        when(mockCache.getListenable()).thenReturn(mockListenable);
    }

    @Test
    public void testNamespaces() throws Exception {
        final URL res = Namespaces.class.getResource(nsDoc);
        final CuratorFramework zk = newClient(curator.getConnectString(), new RetryNTimes(10, 1000));
        zk.start();
        final TreeCache cache = new TreeCache(zk, ZNODE_NAMESPACES);
        cache.start();

        final NamespaceService svc1 = new Namespaces(zk, cache, res.getPath() + randomFilename());

        assertEquals(0, svc1.getNamespaces().size());

        final NamespaceService svc2 = new Namespaces(zk, cache, res.getPath());

        assertEquals(2, svc2.getNamespaces().size());
        assertEquals(LDP.URI, svc2.getNamespace("ldp").get());
        assertEquals("ldp", svc2.getPrefix(LDP.URI).get());

        assertFalse(svc2.getNamespace("jsonld").isPresent());
        assertFalse(svc2.getPrefix(JSONLD.URI).isPresent());
        assertTrue(svc2.setPrefix("jsonld", JSONLD.URI));
        assertEquals(3, svc2.getNamespaces().size());
        assertEquals(JSONLD.URI, svc2.getNamespace("jsonld").get());
        assertEquals("jsonld", svc2.getPrefix(JSONLD.URI).get());

        final Namespaces svc3 = new Namespaces(zk, cache);
        await().atMost(5, SECONDS).until(() -> 3 == svc3.getNamespaces().size());
        assertEquals(JSONLD.URI, svc3.getNamespace("jsonld").get());
        assertFalse(svc3.setPrefix("jsonld", JSONLD.URI));
    }

    @Test
    public void testErrorHandler() throws Exception {
        doThrow(RuntimeException.class).when(mockCache).getCurrentChildren(ZNODE_NAMESPACES);
        assertThrows(RuntimeRepositoryException.class, () -> new Namespaces(mockCurator, mockCache));
    }

    @Test
    public void testError2() throws Exception {
        doThrow(Exception.class).when(mockCreateBuilder2)
            .forPath(ZNODE_NAMESPACES + PATH_SEPARATOR + "foo", "bar".getBytes(UTF_8));
        final Namespaces ns = new Namespaces(mockCurator, mockCache);
        assertFalse(ns.setPrefix("foo", "bar"));
    }

    private static String randomFilename() {
        final SecureRandom random = new SecureRandom();
        final String filename = new BigInteger(50, random).toString(32);
        return filename + ".json";
    }
}
