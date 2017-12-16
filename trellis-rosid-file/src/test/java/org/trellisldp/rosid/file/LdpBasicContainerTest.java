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

import static java.time.Instant.parse;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.rosid.file.TestUtils.rdf;
import static org.trellisldp.vocabulary.RDF.type;

import java.io.File;
import java.time.Instant;
import java.util.List;

import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.RDFS;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class LdpBasicContainerTest {

    private File file;
    private IRI identifier = rdf.createIRI("trellis:repository/ldpbc");

    @BeforeEach
    public void setUp() throws Exception {
        file = new File(getClass().getResource("/ldpbc").toURI());
    }

    @Test
    public void testVersionedResource() {
        final Instant time = parse("2017-02-16T11:15:03Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertFalse(res.hasAcl());
        assertEquals(LDP.BasicContainer, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(3L, res.getExtraLinkRelations().count());
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("inbox") && e.getKey().equals("http://example.org/receiver/inbox")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Foo")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));
    }

    @Test
    public void testResourceFuture() {
        final Instant time = parse("2017-03-15T11:15:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.BasicContainer, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI) x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(3L, res.getExtraLinkRelations().count());
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("inbox") && e.getKey().equals("http://example.org/receiver/inbox")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Foo")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testResourcePast() {
        final Instant time = parse("2017-02-15T11:00:00Z");
        final Resource res = VersionedResource.find(file, identifier, time).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.BasicContainer, res.getInteractionModel());
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertTrue(res.isMemento());
        assertEquals(parse("2017-02-15T10:05:00Z"), res.getModified());
        assertEquals(0L, res.getExtraLinkRelations().count());
        assertEquals(0L, res.stream().filter(TestUtils.isContainment.or(TestUtils.isMembership)).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(0L, triples.size());

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }

    @Test
    public void testResourcePrehistory() {
        final Instant time = parse("2017-01-15T11:00:00Z");
        assertFalse(VersionedResource.find(file, identifier, time).isPresent());
    }

    @Test
    public void testCachedResource() {
        final Resource res = CachedResource.find(file, identifier).get();
        assertEquals(identifier, res.getIdentifier());
        assertEquals(LDP.BasicContainer, res.getInteractionModel());
        final List<IRI> contained = res.stream(LDP.PreferContainment).map(Triple::getObject).map(x -> (IRI)x)
            .collect(toList());
        assertEquals(3L, contained.size());
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/1")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/2")));
        assertTrue(contained.contains(rdf.createIRI("trellis:repository/ldpbc/3")));
        assertFalse(res.getMembershipResource().isPresent());
        assertFalse(res.getMemberRelation().isPresent());
        assertFalse(res.getMemberOfRelation().isPresent());
        assertFalse(res.getInsertedContentRelation().isPresent());
        assertFalse(res.getBinary().isPresent());
        assertFalse(res.isMemento());
        assertEquals(parse("2017-02-16T11:15:03Z"), res.getModified());
        assertEquals(3L, res.getExtraLinkRelations().count());
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("inbox") && e.getKey().equals("http://example.org/receiver/inbox")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Foo")));
        assertTrue(res.getExtraLinkRelations().anyMatch(e ->
                    e.getValue().equals("type") && e.getKey().equals("http://example.org/types/Bar")));
        assertEquals(3L, res.stream().filter(TestUtils.isContainment).count());
        assertEquals(0L, res.stream().filter(TestUtils.isMembership).count());

        final List<Triple> triples = res.stream().filter(TestUtils.isUserManaged).map(Quad::asTriple).collect(toList());
        assertEquals(5L, triples.size());
        assertTrue(triples.contains(rdf.createTriple(identifier, LDP.inbox,
                        rdf.createIRI("http://example.org/receiver/inbox"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Foo"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, type,
                        rdf.createIRI("http://example.org/types/Bar"))));
        assertTrue(triples.contains(rdf.createTriple(identifier, RDFS.label,
                        rdf.createLiteral("A label", "eng"))));
        assertTrue(triples.contains(rdf.createTriple(rdf.createIRI("http://example.org/some/other/resource"),
                    RDFS.label, rdf.createLiteral("Some other resource", "eng"))));

        final List<VersionRange> mementos = res.getMementos();
        assertEquals(1L, mementos.size());
        assertEquals(parse("2017-02-15T10:05:00Z"), mementos.get(0).getFrom());
        assertEquals(parse("2017-02-15T11:15:00Z"), mementos.get(0).getUntil());
    }
}
