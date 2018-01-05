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

import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_COORDINATION;
import static org.trellisldp.vocabulary.RDF.type;

import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.trellisldp.api.EventService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class AbstractResourceServiceTest {

    private static final RDF rdf = new JenaRDF();
    private static final IRI existing = rdf.createIRI("trellis:repository/existing");
    private static final IRI unwritable = rdf.createIRI("trellis:repository/unwritable");
    private static final IRI resource = rdf.createIRI("trellis:repository/resource");
    private static final IRI locked = rdf.createIRI("trellis:repository/locked");

    private static TestingServer curator;

    @Mock
    private static Resource mockResource;

    @Mock
    private static CuratorFramework mockCurator;

    @Mock
    private static Supplier<String> mockIdSupplier;

    @Mock
    private EventService mockEventService;

    @Mock
    private InterProcessLock mockLock;

    @BeforeEach
    public void setUpTests() {
        initMocks(this);
    }

    @Captor
    private ArgumentCaptor<Notification> notificationCaptor;

    public static class MyResourceService extends AbstractResourceService {

        private InterProcessLock lock;
        private static final String baseUrl = "http://example.com/";

        public MyResourceService(final String connectString, final EventService eventService,
                final InterProcessLock lock) {
            this(getZkClient(connectString), eventService, lock);
        }

        public MyResourceService(final CuratorFramework curator, final EventService eventService,
                final InterProcessLock lock) {
            super(baseUrl, new MockProducer<>(true, new StringSerializer(), new StringSerializer()), curator,
                    eventService, mockIdSupplier, false);
            this.lock = lock;
        }

        public MyResourceService(final String baseUrl, final String connectString) {
            super(baseUrl, new MockProducer<>(true, new StringSerializer(), new StringSerializer()),
                    getZkClient(connectString), null, null, false);
        }

        @Override
        public Optional<Resource> get(final IRI identifier) {
            if (identifier.equals(existing)) {
                return of(mockResource);
            }
            return empty();
        }

        @Override
        public Optional<Resource> get(final IRI identifier, final Instant time) {
            if (identifier.equals(existing)) {
                return of(mockResource);
            }
            return empty();
        }

        @Override
        public Boolean write(final IRI identifier, final Stream<? extends Quad> delete,
                final Stream<? extends Quad> add, final Instant time, final Boolean async) {
            return !identifier.equals(unwritable);
        }

        @Override
        protected InterProcessLock getLock(final IRI identifier) {
            if (nonNull(lock)) {
                return lock;
            }
            return super.getLock(identifier);
        }

        @Override
        public Stream<IRI> compact(final IRI identifier, final Instant from, final Instant until) {
            throw new UnsupportedOperationException("compact is not implemented");
        }

        @Override
        public Stream<IRI> tryPurge(final IRI identifier) {
            return Stream.of(rdf.createIRI("file:partition/file.jpg"));
        }

        @Override
        public Stream<Triple> scan() {
            return asList(rdf.createTriple(rdf.createIRI("trellis:repository/existing"), type, LDP.Container))
                .stream();
        }
    }

    private static CuratorFramework getZkClient(final String connectString) {
        final CuratorFramework zk = newClient(connectString, new RetryNTimes(10, 1000));
        zk.start();
        return zk;
    }

    @BeforeAll
    public static void setUp() throws Exception {
        curator = new TestingServer(true);
    }

    @Test
    public void testSkolemization() {
        final BlankNode bnode = rdf.createBlankNode("testing");
        final IRI iri = rdf.createIRI("trellis:bnode/testing");
        final IRI root = rdf.createIRI("trellis:");
        final IRI resource = rdf.createIRI("trellis:resource");
        final IRI child = rdf.createIRI("trellis:resource/child");
        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, null);

        assertTrue(svc.skolemize(bnode) instanceof IRI);
        assertTrue(((IRI) svc.skolemize(bnode)).getIRIString().startsWith("trellis:bnode/"));
        assertTrue(svc.unskolemize(iri) instanceof BlankNode);
        assertEquals(svc.unskolemize(iri), svc.unskolemize(iri));

        assertFalse(svc.unskolemize(rdf.createLiteral("Test")) instanceof BlankNode);
        assertFalse(svc.unskolemize(resource) instanceof BlankNode);
        assertFalse(svc.skolemize(rdf.createLiteral("Test2")) instanceof IRI);
        assertEquals(of(resource), svc.getContainer(child));
        assertEquals(of(root), svc.getContainer(resource));
        assertFalse(svc.getContainer(root).isPresent());

        assertEquals(mockIdSupplier, svc.getIdentifierSupplier());
    }

    @Test
    public void testPutCreate() throws InterruptedException, ExecutionException {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Create));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), null, null);
        assertTrue(svc.put(resource, LDP.RDFSource, dataset).get());
        assertFalse(svc.put(existing, LDP.NonRDFSource, dataset).get());
        assertFalse(svc.put(unwritable, LDP.Container, dataset).get());
        verify(mockEventService, times(0)).emit(any(Notification.class));
    }

    @Test
    public void testPutDelete() throws InterruptedException, ExecutionException {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Delete));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, null);
        assertFalse(svc.put(resource, LDP.Container, dataset).get());
        assertTrue(svc.put(existing, LDP.RDFSource, dataset).get());
        assertFalse(svc.put(unwritable, null, dataset).get());
        verify(mockEventService).emit(notificationCaptor.capture());
        assertEquals(of(rdf.createIRI("http://example.com/repository/existing")),
                notificationCaptor.getValue().getTarget());
    }

    @Test
    public void testPutUpdate() throws InterruptedException, ExecutionException {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Update));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, resource, DC.title, rdf.createLiteral("a title")));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, null);
        assertTrue(svc.put(resource, null, dataset).get());
        assertTrue(svc.put(existing, null, dataset).get());
        assertFalse(svc.put(unwritable, null, dataset).get());
        verify(mockEventService, times(2)).emit(any(Notification.class));
    }

    @Test
    public void testLockedResource() throws Exception {
        final String path = ZNODE_COORDINATION + "/" + md5Hex(locked.getIRIString());
        final InterProcessLock lock = new InterProcessSemaphoreMutex(getZkClient(curator.getConnectString()), path);
        assertTrue(lock.acquire(100L, MILLISECONDS));

        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, locked, DC.title, rdf.createLiteral("A title")));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, null);
        assertFalse(svc.put(locked, null, dataset).get());
        assertTrue(svc.put(resource, null, dataset).get());
        assertTrue(svc.put(existing, null, dataset).get());
        verify(mockEventService, times(2)).emit(any(Notification.class));
    }

    @Test
    public void testFailedLock1() throws Exception {
        doThrow(new Exception("Error")).when(mockCurator).createContainers(ZNODE_COORDINATION);
        assertThrows(RuntimeTrellisException.class, () ->
                new MyResourceService(mockCurator, mockEventService, null));
    }

    @Test
    public void testFailedLock2() throws Exception {
        doThrow(new Exception("Error")).when(mockLock).acquire(any(Long.class), any(TimeUnit.class));

        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, locked, DC.title, rdf.createLiteral("A title")));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, mockLock);
        assertFalse(svc.put(resource, null, dataset).get());
        assertFalse(svc.put(existing, null, dataset).get());
        verify(mockEventService, times(0)).emit(any(Notification.class));
    }

    @Test
    public void testFailedLock3() throws Exception {
        doThrow(new Exception("Error")).when(mockLock).release();
        when(mockLock.acquire(any(Long.class), any(TimeUnit.class))).thenReturn(true);

        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, locked, DC.title, rdf.createLiteral("A title")));
        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, mockLock);
        assertTrue(svc.put(resource, null, dataset).get());
        assertTrue(svc.put(existing, null, dataset).get());
        verify(mockEventService, times(2)).emit(any(Notification.class));
    }

    @Test
    public void testFailedLock4() throws Exception {
        doThrow(new Exception("Error")).when(mockLock).release();
        when(mockLock.acquire(any(Long.class), any(TimeUnit.class))).thenReturn(true);
        when(mockLock.isAcquiredInThisProcess()).thenReturn(true);

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, mockLock);
        assertThrows(RuntimeTrellisException.class, () -> svc.purge(resource));
    }

    @Test
    public void testFailedLock5() throws Exception {
        doThrow(new Exception("Error")).when(mockLock).acquire(any(Long.class), any(TimeUnit.class));

        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, mockLock);
        assertThrows(RuntimeTrellisException.class, () -> svc.purge(resource));
    }

    @Test
    public void testFailedLock6() throws Exception {
        final ResourceService svc = new MyResourceService(curator.getConnectString(), mockEventService, mockLock);
        assertThrows(RuntimeTrellisException.class, () -> svc.purge(resource));
    }


    @Test
    public void testExport() {
        final Set<IRI> graphs = new HashSet<>();
        graphs.add(Trellis.PreferAccessControl);
        graphs.add(Trellis.PreferAudit);
        graphs.add(Trellis.PreferServerManaged);
        graphs.add(Trellis.PreferUserManaged);
        when(mockResource.getIdentifier()).thenReturn(existing);
        when(mockResource.stream(eq(graphs))).thenAnswer(inv ->
                Stream.of(rdf.createTriple(existing, DC.title, rdf.createLiteral("A title"))));
        final ResourceService svc = new MyResourceService(curator.getConnectString(), null, null);

        final List<Quad> export = svc.export(graphs).collect(toList());
        assertEquals(1L, export.size());
        assertEquals(of(existing), export.get(0).getGraphName());
        assertEquals(existing, export.get(0).getSubject());
        assertEquals(DC.title, export.get(0).getPredicate());
        assertEquals(rdf.createLiteral("A title"), export.get(0).getObject());
    }

    @Test
    public void testPurge() {
        final ResourceService svc = new MyResourceService(curator.getConnectString(), null, null);
        final List<IRI> binaries = svc.purge(existing).collect(toList());
        assertEquals(1L, binaries.size());
        assertEquals("file:partition/file.jpg", binaries.get(0).getIRIString());
    }
}
