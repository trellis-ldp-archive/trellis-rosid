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

import static java.time.Instant.now;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_DELETE;
import static org.trellisldp.vocabulary.RDF.type;

import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.trellisldp.api.Resource;
import org.trellisldp.vocabulary.AS;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.PROV;
import org.trellisldp.vocabulary.SKOS;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class EventProducerTest {

    private static final RDF rdf = new JenaRDF();

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI other1 = rdf.createIRI("trellis:repository/other1");
    private final IRI other2 = rdf.createIRI("trellis:repository/other2");
    private final IRI inbox = rdf.createIRI("http://example.org/inbox");
    private final IRI subject = rdf.createIRI("http://example.org/subject");
    private final IRI parent = rdf.createIRI("trellis:repository");
    private final IRI member = rdf.createIRI("trellis:repository/member");

    private final MockProducer<String, String> producer = new MockProducer<>(true,
            new StringSerializer(), new StringSerializer());

    @Mock
    private Producer<String, String> mockProducer;

    @Mock
    private Future<RecordMetadata> mockFuture;

    @Mock
    private Resource mockParent;

    @BeforeEach
    public void setUpTests() {
        initMocks(this);
    }

    @Test
    public void testEventProducer() throws Exception {
        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent), false);
        event.into(existing.stream());

        assertTrue(event.emit());
        assertEquals(4L, event.getRemoved().count());
        assertEquals(5L, event.getAdded().count());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(0L, records.size());
    }

    @Test
    public void testEventCreation() throws Exception {
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.Container);

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Create));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent));
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(1L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_ADD)).count());
    }

    @Test
    public void testEventCreationDirectContainer() throws Exception {
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.DirectContainer);
        when(mockParent.getMembershipResource()).thenReturn(of(member));
        when(mockParent.getMemberRelation()).thenReturn(of(DC.subject));
        when(mockParent.getMemberOfRelation()).thenReturn(empty());

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), type, AS.Create));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent));
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(2L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_ADD)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_MEMBERSHIP_ADD)).count());
    }

    @Test
    public void testEventDeleteDirectContainer() throws Exception {
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.DirectContainer);
        when(mockParent.getMembershipResource()).thenReturn(of(member));
        when(mockParent.getMemberOfRelation()).thenReturn(of(DC.subject));
        when(mockParent.getMemberRelation()).thenReturn(empty());

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);

        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Delete));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent), true);
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(3L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_DELETE)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_MEMBERSHIP_DELETE)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_CACHE)).count());
    }

    @Test
    public void testEventCreationIndirectContainer() throws Exception {
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.IndirectContainer);
        when(mockParent.getMembershipResource()).thenReturn(of(member));
        when(mockParent.getMemberRelation()).thenReturn(of(SKOS.broader));
        when(mockParent.getInsertedContentRelation()).thenReturn(of(DC.subject));

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Create));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, PROV.Activity));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, PROV.wasAssociatedWith,
                    rdf.createIRI("user:foo")));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent));
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(8L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(2L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_ADD)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_MEMBERSHIP_ADD)).count());
    }


    @Test
    public void testEventDeletion() throws Exception {
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.Container);

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Delete));

        producer.clear();
        final EventProducer event = new EventProducer(producer, identifier, modified, of(mockParent), true);
        event.into(existing.stream());

        assertEquals(4L, event.getRemoved().count());
        assertEquals(6L, event.getAdded().count());
        assertTrue(event.emit());
        final List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(2L, records.size());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_LDP_CONTAINMENT_DELETE)).count());
        assertEquals(1L, records.stream().filter(r -> r.topic().equals(TOPIC_CACHE)).count());
    }

    @Test
    public void testProducerFailure() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted exception!"));
        when(mockParent.getIdentifier()).thenReturn(parent);
        when(mockParent.getInteractionModel()).thenReturn(LDP.Container);

        final Literal time = rdf.createLiteral(now().toString(), XSD.dateTime);
        final Literal otherTime = rdf.createLiteral(now().plusSeconds(20).toString(), XSD.dateTime);
        final Dataset existing = rdf.createDataset();
        final BlankNode bnode = rdf.createBlankNode();
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title, rdf.createLiteral("A title")));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other1));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, time));
        existing.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        existing.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                    rdf.createLiteral(time.toString(), XSD.dateTime)));
        existing.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final Dataset modified = rdf.createDataset();
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.title,
                    rdf.createLiteral("Better title")));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.hasPart, other2));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.modified, otherTime));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, DC.subject, subject));
        modified.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        modified.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.Container));
        modified.add(rdf.createQuad(Trellis.PreferAudit, bnode, type, AS.Delete));

        final EventProducer event = new EventProducer(mockProducer, identifier, modified, of(mockParent), true);
        event.into(existing.stream());
        assertFalse(event.emit());
    }
}
