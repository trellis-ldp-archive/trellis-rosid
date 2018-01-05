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

import static java.time.Instant.MAX;
import static java.time.Instant.now;
import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Stream.concat;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.curator.utils.ZKPaths.PATH_SEPARATOR;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.getInstance;
import static org.trellisldp.rosid.common.RDFUtils.endedAtQuad;
import static org.trellisldp.rosid.common.RDFUtils.getParent;
import static org.trellisldp.rosid.common.RosidConstants.ZNODE_COORDINATION;
import static org.trellisldp.vocabulary.AS.Create;
import static org.trellisldp.vocabulary.AS.Delete;
import static org.trellisldp.vocabulary.RDF.type;
import static org.trellisldp.vocabulary.Trellis.DeletedResource;
import static org.trellisldp.vocabulary.Trellis.PreferAudit;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;
import static org.trellisldp.vocabulary.Trellis.PreferUserManaged;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.trellisldp.api.EventService;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
public abstract class AbstractResourceService implements ResourceService {

    private static final Logger LOGGER = getLogger(AbstractResourceService.class);

    protected static final RDF rdf = getInstance();

    private final Supplier<String> idSupplier;

    protected final Boolean async;

    protected final EventService notifications;

    protected final String baseUrl;

    protected final Producer<String, String> producer;

    protected final CuratorFramework curator;

    /**
     * Create an AbstractResourceService with the given producer
     * @param baseUrl the base URL
     * @param producer the kafka producer
     * @param curator the zookeeper curator
     * @param notifications the event service
     * @param idSupplier a supplier of new identifiers
     * @param async write cached resources asynchronously if true, synchronously if false
     */
    public AbstractResourceService(final String baseUrl, final Producer<String, String> producer,
            final CuratorFramework curator, final EventService notifications, final Supplier<String> idSupplier,
            final Boolean async) {

        this.baseUrl = baseUrl;
        this.notifications = notifications;
        this.async = async;
        this.idSupplier = idSupplier;
        this.producer = producer;
        this.curator = curator;

        try {
            this.curator.createContainers(ZNODE_COORDINATION);
        } catch (final Exception ex) {
            LOGGER.error("Could not create zk session node: {}", ex.getMessage());
            throw new RuntimeTrellisException(ex);
        }
    }

    /**
     * Write to the persistence layer
     * @param identifier the identifier
     * @param delete the quads to delete
     * @param add the quads to add
     * @param time the time the resource is written
     * @param async true if the cache is written asynchronously; false otherwise
     * @return true if the write was successful; false otherwise
     */
    protected abstract Boolean write(final IRI identifier, final Stream<? extends Quad> delete,
            final Stream<? extends Quad> add, final Instant time, final Boolean async);


    /**
     * Purge data from the persistence layer
     * @param identifier the identifier
     * @return a stream of binary
     */
    protected abstract Stream<IRI> tryPurge(final IRI identifier);

    @Override
    public Future<Boolean> put(final IRI identifier, final IRI ixnModel, final Dataset dataset) {
        final InterProcessLock lock = getLock(identifier);

        try {
            if (!lock.acquire(Long.parseLong(System.getProperty("zk.lock.wait.ms", "100")), MILLISECONDS)) {
                return completedFuture(false);
            }
        } catch (final Exception ex) {
            LOGGER.error("Error acquiring resource lock: {}", ex.getMessage());
            return completedFuture(false);
        }

        // Set the interaction model
        if (nonNull(ixnModel)) {
            dataset.remove(of(PreferServerManaged), null, type, null);
            dataset.add(PreferServerManaged, identifier, type, ixnModel);
        }

        // Add or remove the "deleted" marker, as appropriate
        if (LDP.Resource.equals(ixnModel) && dataset.contains(of(PreferAudit), null, type, Delete)) {
            dataset.add(PreferUserManaged, identifier, type, DeletedResource);
        } else {
            dataset.remove(of(PreferUserManaged), identifier, type, DeletedResource);
        }

        final Boolean status = tryWrite(identifier, dataset);

        try {
            lock.release();
        } catch (final Exception ex) {
            LOGGER.error("Error releasing resource lock: {}", ex.getMessage());
        }

        if (status && nonNull(notifications)) {
            notifications.emit(new Notification(toExternal(identifier, baseUrl).getIRIString(), dataset));
        }

        return completedFuture(status);
    }

    @Override
    public Stream<IRI> purge(final IRI identifier) {
        final InterProcessLock lock = getLock(identifier);

        try {
            lock.acquire(Long.parseLong(System.getProperty("zk.lock.wait.ms", "100")), MILLISECONDS);
        } catch (final Exception ex) {
            LOGGER.error("Error acquiring lock: {}", ex.getMessage());
        }

        if (!lock.isAcquiredInThisProcess()) {
            throw new RuntimeTrellisException("Could not acquire resource lock for " + identifier);
        }

        get(identifier, MAX).ifPresent(res -> {
            try (final Dataset dataset = rdf.createDataset()) {
                dataset.add(rdf.createQuad(PreferAudit, rdf.createBlankNode(), type, Delete));
                tryWrite(identifier, dataset);
            } catch (final Exception ex) {
                LOGGER.error("Error closing dataset: {}", ex.getMessage());
            }
        });

        final Stream<IRI> stream = tryPurge(identifier);

        try {
            lock.release();
        } catch (final Exception ex) {
            LOGGER.error("Error releasing resource lock: {}", ex.getMessage());
            throw new RuntimeTrellisException("Error releasing resource lock", ex);
        }

        return stream;
    }

    /**
     * Write the resource data to the persistence layer
     * @param identifier the identifier
     * @param dataset the dataset
     * @return true if the operation was successful; false otherwise
     */
    private Boolean tryWrite(final IRI identifier, final Dataset dataset) {
        final Boolean isCreate = dataset.contains(of(PreferAudit), null, type, Create);
        final Boolean isDelete = dataset.contains(of(PreferAudit), null, type, Delete);
        final Optional<Resource> resource = get(identifier, MAX);

        if (resource.isPresent() && isCreate) {
            LOGGER.warn("The resource already exists and cannot be created: {}", identifier.getIRIString());
            return false;
        } else if (!resource.isPresent() && isDelete) {
            LOGGER.warn("The resource does not exist and cannot be deleted: {}", identifier.getIRIString());
            return false;
        }

        final Optional<Resource> parent = getContainer(identifier).flatMap(this::get);
        final EventProducer eventProducer = new EventProducer(producer, identifier, dataset, parent, async);
        try (final Stream<? extends Quad> stream = resource.map(Resource::stream).orElseGet(Stream::empty)) {
            eventProducer.into(stream);
        }

        final Instant time = now();
        if (!write(identifier, eventProducer.getRemoved(),
                    concat(eventProducer.getAdded(), endedAtQuad(identifier, dataset, time)), time, false)) {
            LOGGER.error("Could not write data to persistence layer!");
            return false;
        }

        return eventProducer.emit();
    }

    @Override
    public Optional<IRI> getContainer(final IRI identifier) {
        return getParent(identifier.getIRIString()).map(rdf::createIRI);
    }

    @Override
    public Supplier<String> getIdentifierSupplier() {
        return idSupplier;
    }

    protected InterProcessLock getLock(final IRI identifier) {
        final String path = ZNODE_COORDINATION + PATH_SEPARATOR + md5Hex(identifier.getIRIString());
        return new InterProcessSemaphoreMutex(curator, path);
    }
}
