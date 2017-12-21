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

import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.sparql.core.DatasetGraphFactory.create;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.vocabulary.PROV.endedAtTime;
import static org.trellisldp.vocabulary.PROV.wasGeneratedBy;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;
import static org.trellisldp.vocabulary.XSD.dateTime;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

/**
 * @author acoburn
 */
public final class RDFUtils {

    private static final Logger LOGGER = getLogger(RDFUtils.class);

    private static final JenaRDF rdf = new JenaRDF();

    /**
     * Get the PROV.endedAtTime quad, wrapped in a Stream
     * @param identifier the identifier
     * @param dataset the dataset
     * @param time the time
     * @return the quad
     */
    public static Stream<Quad> endedAtQuad(final IRI identifier, final Dataset dataset, final Instant time) {
        return dataset.stream(of(PreferAudit), identifier, wasGeneratedBy, null)
            .map(Quad::getObject).filter(term -> term instanceof BlankNodeOrIRI)
            .map(term -> (BlankNodeOrIRI) term)
            .map(term -> (Quad) rdf.createQuad(PreferAudit, term, endedAtTime,
                    rdf.createLiteral(time.toString(), dateTime))).limit(1);
    }

    /**
     * A predicate that returns true if the object of the provided quad is an IRI
     */
    public static final Predicate<Quad> hasObjectIRI = quad -> quad.getObject() instanceof IRI;

    /**
     * A predicate that returns true if the subject of the provided quad is an IRI
     */
    public static final Predicate<Quad> hasSubjectIRI = quad -> quad.getSubject() instanceof IRI;

    /**
     * Get the IRI of the parent resource, if it exists
     * @param identifier the identifier
     * @return the parent if it exists
     */
    public static Optional<String> getParent(final String identifier) {
        return of(identifier.lastIndexOf('/')).filter(idx -> idx > 0).map(idx -> identifier.substring(0, idx));
    }

    /**
     * Serialize a list of quads
     * @param quads the quads
     * @return a string
     */
    public static String serialize(final List<Quad> quads) {
        try (final StringWriter str = new StringWriter()) {
            final DatasetGraph datasetGraph = create();
            quads.stream().map(rdf::asJenaQuad).forEach(datasetGraph::add);
            write(str, datasetGraph, NQUADS);
            datasetGraph.close();
            return str.toString();
        } catch (final IOException ex) {
            LOGGER.error("Error processing dataset in quad serializer: ", ex.getMessage());
            throw new RuntimeTrellisException("Error processing dataset", ex);
        }
    }

    /**
     * Serialize a dataset
     * @param dataset the dataset
     * @return a string
     */
    public static String serialize(final Dataset dataset) {
        if (nonNull(dataset)) {
            return serialize(dataset.stream().collect(toList()));
        }
        return "";
    }

    /**
     * Deserialize a string
     * @param data the data
     * @return a Dataset
     */
    public static Dataset deserialize(final String data) {
        final DatasetGraph dataset = create();
        if (nonNull(data)) {
            RDFParser.fromString(data).lang(NQUADS).parse(dataset);
        }
        return rdf.asDataset(dataset);
    }

    private RDFUtils() {
        // prevent instantiation
    }
}
