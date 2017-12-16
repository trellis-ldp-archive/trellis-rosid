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

import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.trellisldp.api.RDFUtils.getInstance;

import java.io.File;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.rosid.common.ResourceData;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.OA;

/**
 * An object to mediate access to a file-based resource representation
 * @author acoburn
 */
abstract class AbstractFileResource implements Resource {

    protected static final RDF rdf = getInstance();

    protected final IRI identifier;
    protected final File directory;
    protected final ResourceData data;

    /**
     * An abstract contructor for creating a file-based resource
     * @param directory the directory
     * @param identifier the identifier
     * @param data the data
     */
    protected AbstractFileResource(final File directory, final IRI identifier, final ResourceData data) {
        requireNonNull(directory, "The data directory cannot be null!");
        requireNonNull(identifier, "The identifier cannot be null!");
        requireNonNull(data, "The resource data cannot be null!");

        this.identifier = identifier;
        this.directory = directory;
        this.data = data;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public Boolean hasAcl() {
        return ofNullable(data.getHasAcl()).orElse(false);
    }

    @Override
    public List<VersionRange> getMementos() {
        return ofNullable(data.getGeneratedAtTime()).filter(list -> list.size() > 1)
            .map(dateTimes -> {
                final List<VersionRange> mementos = new ArrayList<>();
                Instant last = dateTimes.get(0);
                for (final Instant time : dateTimes.subList(1, dateTimes.size())) {
                    mementos.add(new VersionRange(last, time));
                    last = time;
                }
                return mementos;
            }).orElseGet(Collections::emptyList);
    }

    @Override
    public IRI getInteractionModel() {
        return ofNullable(data.getLdpType()).map(rdf::createIRI).orElse(LDP.Resource);
    }

    @Override
    public Optional<IRI> getMembershipResource() {
        return ofNullable(data.getMembershipResource()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberRelation() {
        return ofNullable(data.getHasMemberRelation()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getMemberOfRelation() {
        return ofNullable(data.getIsMemberOfRelation()).map(rdf::createIRI);
    }

    @Override
    public Optional<IRI> getInsertedContentRelation() {
        final Optional<IRI> relation = ofNullable(data.getInsertedContentRelation()).map(rdf::createIRI);
        if (!relation.isPresent() && LDP.DirectContainer.equals(getInteractionModel())) {
            return of(LDP.MemberSubject);
        }
        return relation;
    }

    @Override
    public Stream<Entry<String, String>> getExtraLinkRelations() {
        final Stream.Builder<Entry<String, String>> builder = Stream.builder();
        ofNullable(data.getInbox()).map(inbox -> new SimpleEntry<>(inbox, "inbox"))
            .ifPresent(builder::accept);
        ofNullable(data.getAnnotationService())
            .map(as -> new SimpleEntry<>(as, OA.annotationService.getIRIString())).ifPresent(builder::accept);
        ofNullable(data.getUserTypes()).orElseGet(Collections::emptyList).stream()
            .map(t -> new SimpleEntry<>(t, "type")).forEach(builder::accept);
        return builder.build();
    }

    @Override
    public Optional<Binary> getBinary() {
        return ofNullable(data.getBinary()).map(binary ->
            new Binary(rdf.createIRI(binary.getId()), binary.getModified(), binary.getFormat(), binary.getSize()));
    }

    @Override
    public Instant getModified() {
        return data.getModified();
    }
}
