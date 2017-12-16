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

import static org.trellisldp.vocabulary.RDF.type;
import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.trellisldp.vocabulary.ACL;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.OA;
import org.trellisldp.vocabulary.RDFS;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class ResourceDataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final RDF rdf = new JenaRDF();

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    @Test
    public void testDeserialize() throws IOException {
        final ResourceData res = MAPPER.readValue(getClass().getResourceAsStream("/resource1.json"),
                ResourceData.class);

        assertEquals("trellis:repository/resource1", res.getId());
        assertEquals(LDP.Container.getIRIString(), res.getLdpType());
        assertTrue(res.getUserTypes().contains("http://example.org/ns/CustomType"));
        assertEquals("http://receiver.example.org/inbox", res.getInbox());
        assertEquals("file:/path/to/binary", res.getBinary().getId());
        assertEquals("image/jpeg", res.getBinary().getFormat());
        assertEquals(Long.valueOf(103527L), res.getBinary().getSize());
        assertNull(res.getInsertedContentRelation());
    }

    @Test
    public void testRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final IRI annotationService = rdf.createIRI("http://example.com/annotations");
        final Instant time = now();
        final Literal modified = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.RDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, OA.annotationService,
                    annotationService));

        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset, singletonList(time));
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertEquals(identifier.getIRIString(), data.getId());
            assertEquals(inbox.getIRIString(), data.getInbox());
            assertEquals(LDP.RDFSource.getIRIString(), data.getLdpType());
            assertEquals(time, data.getModified());
            assertEquals("http://www.trellisrepo.org/ns/trellisresource.jsonld", data.getContext());
            assertEquals(annotationService.getIRIString(), data.getAnnotationService());
            assertTrue(data.getGeneratedAtTime().contains(time));
            assertEquals(1L, data.getGeneratedAtTime().size());
        });
    }

    @Test
    public void testNonRDFSource() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI binary = rdf.createIRI("file://path/to/resource");
        final IRI inbox = rdf.createIRI("http://example.com/receiver/inbox");
        final Literal format = rdf.createLiteral("image/jpeg");
        final Literal extent = rdf.createLiteral("12345", XSD.long_);
        final Instant time = now();
        final Literal modified = rdf.createLiteral(time.toString(), XSD.dateTime);
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.inbox, inbox));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.NonRDFSource));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.hasPart, binary));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.format, format));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, binary, DC.extent, extent));

        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset, emptyList());
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertFalse(data.getHasAcl());
            assertEquals(identifier.getIRIString(), data.getId());
            assertEquals(inbox.getIRIString(), data.getInbox());
            assertEquals(LDP.NonRDFSource.getIRIString(), data.getLdpType());
            assertEquals(time, data.getModified());
            assertNotNull(data.getBinary());
            assertEquals(binary.getIRIString(), data.getBinary().getId());
            assertEquals(12345L, (long) data.getBinary().getSize());
            assertEquals(format.getLexicalForm(), data.getBinary().getFormat());
            assertEquals(time, data.getBinary().getModified());
            assertTrue(data.getGeneratedAtTime().isEmpty());
        });
    }

    @Test
    public void testIndirectContainer() {
        final IRI identifier = rdf.createIRI("trellis:repository/resource");
        final IRI other = rdf.createIRI("trellis:repository/other");
        final IRI diff = rdf.createIRI("trellis:repository/diff");
        final Instant time = now();
        final Literal modified = rdf.createLiteral(time.toString(), XSD.dateTime);

        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, type, LDP.IndirectContainer));
        dataset.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified, modified));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.insertedContentRelation,
                    RDFS.label));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.hasMemberRelation, DC.title));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.membershipResource, other));
        dataset.add(rdf.createQuad(Trellis.PreferUserManaged, identifier, LDP.isMemberOfRelation, diff));
        dataset.add(rdf.createQuad(Trellis.PreferAccessControl, rdf.createBlankNode(), type, ACL.Authorization));

        final Optional<ResourceData> rd = ResourceData.from(identifier, dataset, singletonList(time));
        assertTrue(rd.isPresent());
        rd.ifPresent(data -> {
            assertTrue(data.getHasAcl());
            assertEquals(identifier.getIRIString(), data.getId());
            assertEquals(RDFS.label.getIRIString(), data.getInsertedContentRelation());
            assertEquals(DC.title.getIRIString(), data.getHasMemberRelation());
            assertEquals(other.getIRIString(), data.getMembershipResource());
            assertEquals(LDP.IndirectContainer.getIRIString(), data.getLdpType());
            assertEquals(time, data.getModified());
            assertEquals(diff.getIRIString(), data.getIsMemberOfRelation());
            assertNull(data.getBinary());
            assertTrue(data.getGeneratedAtTime().contains(time));
            assertEquals(1L, data.getGeneratedAtTime().size());
        });
    }
}
