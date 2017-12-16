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
package org.trellisldp.rosid.file;

import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static java.time.Instant.parse;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.trellisldp.vocabulary.RDF.type;

import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.RDFS;
import org.trellisldp.vocabulary.Trellis;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class RDFPatchTest {

    private static final RDF rdf = new JenaRDF();
    private static final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private static File resDir1 = new File("build/data/res1");
    private static File resDir10 = new File("build/data/res10");

    @BeforeEach
    public void setUp() throws IOException {
        resDir1.mkdirs();
        resDir10.mkdirs();
    }

    @AfterAll
    public static void tearDown() {
        try {
            deleteDirectory(resDir1);
            deleteDirectory(resDir10);
        } catch (final IOException ex) {
            // ignore errors
        }
    }

    @Test
    public void testStream1() throws Exception {
        final File file = new File(getClass().getResource("/journal1.txt").toURI());
        final Instant time = parse("2017-02-11T02:51:35Z");
        final Graph graph = rdf.createGraph();
        RDFPatch.asStream(rdf, file, identifier, time).map(Quad::asTriple).forEach(graph::add);
        assertEquals(3L, graph.size());
        assertTrue(graph.contains(identifier, rdf.createIRI("http://www.w3.org/2004/02/skos/core#prefLabel"), null));
    }

    @Test
    public void testStream2() throws Exception {
        final File file = new File(getClass().getResource("/journal1.txt").toURI());
        final Instant time = parse("2017-02-09T02:51:35Z");
        final Graph graph = rdf.createGraph();
        RDFPatch.asStream(rdf, file, identifier, time).map(Quad::asTriple).forEach(graph::add);
        assertEquals(4L, graph.size());
        assertTrue(graph.contains(identifier, rdf.createIRI("http://www.w3.org/2004/02/skos/core#prefLabel"), null));
        assertTrue(graph.contains(identifier, DC.isPartOf, null));
    }

    @Test
    public void testStream3() throws Exception {
        final File file = new File(getClass().getResource("/journal1.txt").toURI());
        final Instant time = parse("2017-01-30T02:51:35Z");
        final Graph graph = rdf.createGraph();
        RDFPatch.asStream(rdf, file, identifier, time).map(Quad::asTriple).forEach(graph::add);
        assertEquals(8L, graph.size());
        assertFalse(graph.contains(identifier, rdf.createIRI("http://www.w3.org/2004/02/skos/core#prefLabel"), null));
        assertTrue(graph.contains(identifier, DC.extent, null));
        assertTrue(graph.contains(identifier, DC.spatial, null));
        assertTrue(graph.contains(identifier, DC.title, null));
        assertTrue(graph.contains(identifier, DC.description, null));
        assertTrue(graph.contains(identifier, DC.subject, null));
        assertEquals(2L, graph.stream(identifier, DC.subject, null).count());
    }

    @Test
    public void testStream4() throws Exception {
        final File file = new File(getClass().getResource("/journal1.txt").toURI());
        final Instant time = parse("2017-01-15T09:14:00Z");
        final Graph graph = rdf.createGraph();
        RDFPatch.asStream(rdf, file, identifier, time).map(Quad::asTriple).forEach(graph::add);
        assertEquals(6L, graph.size());
        assertFalse(graph.contains(identifier, rdf.createIRI("http://www.w3.org/2004/02/skos/core#prefLabel"), null));
        assertFalse(graph.contains(identifier, DC.extent, null));
        assertFalse(graph.contains(identifier, DC.spatial, null));
        assertTrue(graph.contains(identifier, DC.title, null));
        assertTrue(graph.contains(identifier, DC.description, null));
        assertTrue(graph.contains(identifier, DC.subject, null));
        assertEquals(2L, graph.stream(identifier, DC.subject, null).count());
    }

    @Test
    public void testPatchWriter() throws IOException {
        final File file = new File(resDir1, RESOURCE_JOURNAL);
        final Instant time = now();
        final List<Quad> delete = new ArrayList<>();
        final List<Quad> add = new ArrayList<>();
        final Quad title = rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("Title"));
        add.add(title);
        add.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.description,
                    rdf.createLiteral("A longer description")));
        add.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        RDFPatch.write(file, delete.stream(), add.stream(), time);
        final List<Quad> data1 = RDFPatch.asStream(rdf, file, identifier, time).collect(toList());
        assertEquals(add.size() + 1, data1.size());
        add.forEach(q -> assertTrue(data1.contains(q)));

        final Instant later = time.plusSeconds(10L);
        add.clear();
        delete.add(title);
        add.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("Other Title")));
        add.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, RDFS.label, rdf.createLiteral("Label")));
        RDFPatch.write(file, delete.stream(), add.stream(), later);
        final List<Quad> data2 = RDFPatch.asStream(rdf, file, identifier, later).collect(toList());
        assertEquals(data2.size(), data1.size() - delete.size() + add.size());
        add.forEach(q -> assertTrue(data2.contains(q)));
        delete.forEach(q -> assertFalse(data2.contains(q)));
        assertFalse(data2.contains(title));

        RDFPatch.write(file, empty(), of(rdf.createQuad(LDP.PreferContainment, identifier, LDP.contains,
                    rdf.createIRI("trellis:repository/resource/1"))), later.plusSeconds(10L));

        final List<VersionRange> versions = RDFPatch.asTimeMap(file);
        assertEquals(1L, versions.size());
        assertEquals(time.truncatedTo(MILLIS), versions.get(0).getFrom().truncatedTo(MILLIS));
        assertEquals(later.truncatedTo(MILLIS), versions.get(0).getUntil().truncatedTo(MILLIS));
    }

    @Test
    public void testWriteErrors() throws Exception {
        final File file = new File(getClass().getResource("/readonly/resource.rdfp").toURI());
        assumeTrue(file.setWritable(false));
        assertFalse(RDFPatch.write(file, empty(), empty(), now()));
        file.setWritable(true);
    }

    @Test
    public void testStreamReader() throws Exception {
        final File file = new File(getClass().getResource("/journal1.txt").toURI());
        final Instant time = parse("2017-02-11T02:51:35Z");
        try (final RDFPatch.StreamReader reader = new RDFPatch.StreamReader(rdf, file, identifier, time)) {
            while (reader.hasNext()) {
                assertNotNull(reader.next());
            }
            assertThrows(NoSuchElementException.class, reader::next);
        }
    }

    @Test
    public void testTimeMapReader() throws Exception {
        final File file = new File(resDir1, RESOURCE_JOURNAL);
        try (final RDFPatch.TimeMapReader reader = new RDFPatch.TimeMapReader(file)) {
            while (reader.hasNext()) {
                assertNotNull(reader.next());
            }
            assertThrows(NoSuchElementException.class, reader::next);
        }
    }

    @Test
    public void testStreamReaderNoFile() throws Exception {
        final String dir = new File(getClass().getResource("/journal1.txt").toURI()).getParent();
        final File file = new File(dir, "non-existent-resource");
        assertFalse(file.exists());
        assertThrows(UncheckedIOException.class, () -> new RDFPatch.StreamReader(rdf, file, identifier, now()));
    }

    @Test
    public void testTimeMapReaderNoFile() throws Exception {
        final String dir = new File(getClass().getResource("/journal1.txt").toURI()).getParent();
        final File file = new File(dir, "non-existent-resource");
        assertFalse(file.exists());
        assertThrows(UncheckedIOException.class, () -> new RDFPatch.TimeMapReader(file));
    }
}
