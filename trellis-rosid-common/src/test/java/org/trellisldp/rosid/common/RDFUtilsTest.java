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
import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.rosid.common.RDFUtils.endedAtQuad;
import static org.trellisldp.rosid.common.RDFUtils.getParent;
import static org.trellisldp.rosid.common.RDFUtils.hasObjectIRI;
import static org.trellisldp.rosid.common.RDFUtils.hasSubjectIRI;
import static org.trellisldp.api.RDFUtils.getInstance;

import java.time.Instant;
import java.util.List;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.trellisldp.vocabulary.PROV;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class RDFUtilsTest {

    private static final RDF rdf = getInstance();

    private final IRI identifier = rdf.createIRI("trellis:repository/resource");
    private final IRI agent = rdf.createIRI("http://example.org/agent");

    @Test
    public void testFilters() {
        final Dataset dataset = rdf.createDataset();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, agent));
        dataset.add(rdf.createQuad(Trellis.PreferAudit, rdf.createBlankNode(), PROV.endedAtTime,
                    rdf.createLiteral(now().toString(), XSD.dateTime)));
        assertEquals(1L, dataset.stream().filter(hasObjectIRI).count());
        assertEquals(1L, dataset.stream().filter(hasSubjectIRI).count());
    }

    @Test
    public void testGetParent() {
        assertEquals(of("trellis:repository"), getParent("trellis:repository/resource"));
        assertFalse(getParent("trellis:repository").isPresent());
        assertEquals(of("trellis:repository/resource"), getParent("trellis:repository/resource/child"));
    }

    @Test
    public void testEndedAtQuad() {
        final Dataset dataset = rdf.createDataset();
        final Instant time = now();
        dataset.add(rdf.createQuad(Trellis.PreferAudit, identifier, PROV.wasGeneratedBy, rdf.createBlankNode()));

        final List<Quad> quads = endedAtQuad(identifier, dataset, time).collect(toList());
        assertEquals(1L, quads.size());
        final RDFTerm literal = quads.get(0).getObject();
        assertTrue(literal instanceof Literal);
        assertEquals(time.toString(), ((Literal) literal).getLexicalForm());
    }

    @Test
    public void testSerializeDataset() {
        final String data = "<info:foo> <ex:bar> \"A literal\"@en .\n<info:foo> <ex:baz> <info:trellis> .\n";
        final String dataInv = "<info:foo> <ex:baz> <info:trellis> .\n<info:foo> <ex:bar> \"A literal\"@en .\n";
        final String serialized = RDFUtils.serialize(RDFUtils.deserialize(data));
        assertTrue(serialized.equals(data) || serialized.equals(dataInv));
        assertEquals("", RDFUtils.serialize((Dataset) null));

        assertEquals(2L, RDFUtils.deserialize(data).size());
        assertEquals(0L, RDFUtils.deserialize(null).size());
    }

    @Test
    public void testSerializeList() {
        final String data = "<info:foo> <ex:bar> \"A literal\"@en <ex:Graph> .\n" +
            "<info:foo> <ex:baz> <info:trellis> <ex:Graph> .\n";
        final String dataInv = "<info:foo> <ex:baz> <info:trellis> <ex:Graph> .\n" +
            "<info:foo> <ex:bar> \"A literal\"@en <ex:Graph> .\n";
        final String serialized = RDFUtils.serialize(asList(
                    rdf.createQuad(rdf.createIRI("ex:Graph"), rdf.createIRI("info:foo"),
                        rdf.createIRI("ex:bar"), rdf.createLiteral("A literal", "en")),
                    rdf.createQuad(rdf.createIRI("ex:Graph"), rdf.createIRI("info:foo"),
                        rdf.createIRI("ex:baz"), rdf.createIRI("info:trellis"))));

        assertTrue(serialized.equals(data) || serialized.equals(dataInv));
    }
}
