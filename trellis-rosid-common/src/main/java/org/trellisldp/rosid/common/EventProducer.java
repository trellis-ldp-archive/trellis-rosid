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
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.getInstance;
import static org.trellisldp.api.RDFUtils.toDataset;
import static org.trellisldp.rosid.common.RDFUtils.serialize;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_CACHE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_CONTAINMENT_DELETE;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_ADD;
import static org.trellisldp.rosid.common.RosidConstants.TOPIC_LDP_MEMBERSHIP_DELETE;
import static org.trellisldp.vocabulary.AS.Create;
import static org.trellisldp.vocabulary.AS.Delete;
import static org.trellisldp.vocabulary.AS.Update;
import static org.trellisldp.vocabulary.DC.modified;
import static org.trellisldp.vocabulary.LDP.DirectContainer;
import static org.trellisldp.vocabulary.LDP.IndirectContainer;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.PreferMembership;
import static org.trellisldp.vocabulary.LDP.contains;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.vocabulary.Trellis.PreferAccessControl;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;
import static org.trellisldp.vocabulary.Trellis.PreferUserManaged;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;

/**
 * @author acoburn
 */
class EventProducer {

    private static final Logger LOGGER = getLogger(EventProducer.class);

    private static final RDF rdf = getInstance();

    private static final Set<IRI> graphsOfInterest = new HashSet<>(asList(
                PreferAccessControl, PreferServerManaged, PreferUserManaged));

    private final Set<Quad> existing = new HashSet<>();

    private final Producer<String, String> producer;

    private final IRI identifier;

    private final Optional<Resource> parent;

    private final Dataset dataset;

    private final Boolean async;

    /**
     * Create a new event producer
     * @param producer the kafka producer
     * @param identifier the identifier
     * @param dataset the dataset
     * @param the parent resource, if one exists
     * @param async whether the cache is generated asynchronously
     */
    public EventProducer(final Producer<String, String> producer, final IRI identifier, final Dataset dataset,
            final Optional<Resource> parent, final Boolean async) {
        this.producer = producer;
        this.identifier = identifier;
        this.dataset = dataset;
        this.parent = parent;
        this.async = async;
    }

    /**
     * Create a new event producer
     * @param producer the kafka producer
     * @param identifier the identifier
     * @param dataset the dataset
     * @param the parent resource, if one exists
     */
    public EventProducer(final Producer<String, String> producer, final IRI identifier, final Dataset dataset,
            final Optional<Resource> parent) {
        this(producer, identifier, dataset, parent, false);
    }

    private static final Function<Quad, Quad> auditTypeMapper = q-> {
        if (type.equals(q.getPredicate()) && (q.getObject().equals(Delete) || q.getObject().equals(Create))) {
            return rdf.createQuad(PreferAudit, q.getSubject(), q.getPredicate(), Update);
        }
        return q;
    };

    private ProducerRecord<String, String> buildContainmentMessage(final String topic, final IRI resource,
            final Resource parent, final Dataset dataset) throws Exception {
        try (final Dataset data = dataset.stream(of(PreferAudit), null, null, null).map(auditTypeMapper)
                .collect(toDataset())) {
            data.add(PreferContainment, parent.getIdentifier(), contains, resource);
            return new ProducerRecord<>(topic, parent.getIdentifier().getIRIString(), serialize(data));
        }
    }

    private Optional<ProducerRecord<String, String>> buildMembershipMessage(final String topic, final IRI resource,
            final Resource parent, final Dataset dataset) throws Exception {
        try (final Dataset data = rdf.createDataset()) {
            if (DirectContainer.equals(parent.getInteractionModel())) {
                parent.getMembershipResource().ifPresent(member -> {
                    parent.getMemberRelation().ifPresent(relation ->
                        data.add(rdf.createQuad(PreferMembership, member, relation, resource)));
                    parent.getMemberOfRelation().ifPresent(relation ->
                        data.add(rdf.createQuad(PreferMembership, resource, relation, member)));
                });
            } else if (IndirectContainer.equals(parent.getInteractionModel())) {
                parent.getMembershipResource().ifPresent(member ->
                    parent.getMemberRelation().ifPresent(relation ->
                        parent.getInsertedContentRelation().ifPresent(inserted ->
                            dataset.stream(of(PreferUserManaged), null, inserted, null).sequential().forEachOrdered(q ->
                                data.add(rdf.createQuad(PreferMembership, member, relation, q.getObject()))))));
            }
            final Optional<String> key = data.stream(of(PreferMembership), null, null, null).map(Quad::getSubject)
                .filter(x -> x instanceof IRI).map(x -> (IRI) x).map(IRI::getIRIString).findFirst();
            if (key.isPresent()) {
                dataset.stream(of(PreferAudit), null, null, null).map(auditTypeMapper).forEachOrdered(data::add);
                return of(new ProducerRecord<>(topic, key.get(), serialize(data)));
            }
            return empty();
        }
    }

    private Consumer<Resource> emitToParent(final IRI identifier, final Dataset dataset,
            final List<Future<RecordMetadata>> results) {
        final Boolean isCreate = dataset.contains(of(PreferAudit), null, type, Create);
        final Boolean isDelete = dataset.contains(of(PreferAudit), null, type, Delete);
        final String containmentTopic = isDelete ? TOPIC_LDP_CONTAINMENT_DELETE : TOPIC_LDP_CONTAINMENT_ADD;
        final String membershipTopic = isDelete ? TOPIC_LDP_MEMBERSHIP_DELETE : TOPIC_LDP_MEMBERSHIP_ADD;

        return container -> {
            if (isDelete || isCreate) {
                try {
                    LOGGER.info("Sending to parent: {}", container.getIdentifier());
                    results.add(producer.send(buildContainmentMessage(containmentTopic, identifier, container,
                                    dataset)));

                    buildMembershipMessage(membershipTopic, identifier, container, dataset).ifPresent(msg -> {
                            LOGGER.info("Sending to member resource: {}", container.getMembershipResource());
                            results.add(producer.send(msg));
                    });
                } catch (final Exception ex) {
                    LOGGER.error("Error processing dataset: {}", ex.getMessage());
                }
            }
        };
    }

    /**
     * Emit messages to the relevant kafka topics
     * @return true if the messages were successfully delivered to the kafka topics; false otherwise
     */
    public Boolean emit() {

        try {
            final List<Future<RecordMetadata>> results = new ArrayList<>();

            if (async) {
                results.add(producer.send(new ProducerRecord<>(TOPIC_CACHE, identifier.getIRIString(),
                                serialize(dataset))));
            }

            // Update the containment triples of the parent resource if this is a delete or create operation
            parent.ifPresent(emitToParent(identifier, dataset, results));

            for (final Future<RecordMetadata> result : results) {
                final RecordMetadata res = result.get();
                LOGGER.debug("Send record to topic: {}, {}", res, res.timestamp());
            }

            return true;
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.error("Error sending record to kafka topic: {}", ex.getMessage());
            return false;
        }
    }

    /**
     * Stream out the added quads
     * @return the added quads
     */
    public Stream<Quad> getAdded() {
        return dataset.stream().filter(q -> !existing.contains(q)).map(q -> (Quad) q);
    }

    /**
     * Stream out the removed quads
     * @return the removed quads
     */
    public Stream<Quad> getRemoved() {
        return existing.stream().filter(q -> !dataset.contains(q))
            // exclude the server-managed modified triple
            .filter(q -> !q.getGraphName().equals(of(PreferServerManaged)) || !modified.equals(q.getPredicate()))
            .map(q -> (Quad) q);
    }

    /**
     * Stream a collection of quads into the event producer
     * @param quads the quads
     */
    public void into(final Stream<? extends Quad> quads) {
        quads.filter(q -> q.getGraphName().filter(graphsOfInterest::contains).isPresent())
            .forEachOrdered(existing::add);
    }
}
