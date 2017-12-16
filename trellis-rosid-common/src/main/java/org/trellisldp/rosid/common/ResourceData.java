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
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import org.trellisldp.vocabulary.ACL;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.OA;
import org.trellisldp.vocabulary.RDF;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
public class ResourceData {

    /**
     * The binary-specific data
     */
    public static class BinaryData {

        private String id;
        private String format;
        private Long size;
        private Instant modified;

        /**
         * Get the binary object identifier
         * @return the binary identifier
         */
        @JsonProperty("@id")
        public String getId() {
            return id;
        }

        /**
         * Set the identifier for the binary object
         * @param id the identifier
         */
        @JsonProperty("@id")
        public void setId(final String id) {
            this.id = id;
        }

        /**
         * Get the binary object format (MIMEType)
         * @return the binary object MIMEType
         */
        public String getFormat() {
            return format;
        }

        /**
         * Set the binary object format
         * @param format the MIMEType of the binary object
         */
        public void setFormat(final String format) {
            this.format = format;
        }

        /**
         * The binary object size
         * @return the binary size
         */
        public Long getSize() {
            return size;
        }

        /**
         * Set the binary object size
         * @param size the size of the binary
         */
        public void setSize(final Long size) {
            this.size = size;
        }

        /**
         * Get the modification date of the binary object
         * @return the modification value
         */
        public Instant getModified() {
            return modified;
        }

        /**
         * Set the modification value
         * @param modified the modified value
         */
        public void setModified(final Instant modified) {
            this.modified = modified;
        }
    }

    private String context = "http://www.trellisrepo.org/ns/trellisresource.jsonld";
    private String id;
    private String ldpType;
    private List<String> userTypes;
    private List<Instant> generatedAtTime;
    private BinaryData binary;
    private String insertedContentRelation;
    private String isMemberOfRelation;
    private String hasMemberRelation;
    private String membershipResource;
    private Instant modified;
    private String annotationService;
    private String inbox;
    private Boolean hasAcl;

    /**
     * The JSON-LD context of the resource data
     * @return the context value
     */
    @JsonProperty("@context")
    public String getContext() {
        return context;
    }

    /**
     * Set the JSON-LD context
     * @param context the context value
     */
    @JsonProperty("@context")
    public void setContext(final String context) {
        this.context = context;
    }

    /**
     * Get the resource identifier
     * @return the resource identifier
     */
    @JsonProperty("@id")
    public String getId() {
        return id;
    }

    /**
     * Set the resource identifier
     * @param id the resource identifier
     */
    @JsonProperty("@id")
    public void setId(final String id) {
        this.id = id;
    }

    /**
     * Get the interaction model for the resource
     * @return the LDP type of the resource
     */
    @JsonProperty("@type")
    public String getLdpType() {
        return ldpType;
    }

    /**
     * Set the interaction model for the resource
     * @param ldpType the LDP type of the resource
     */
    @JsonProperty("@type")
    public void setLdpType(final String ldpType) {
        this.ldpType = ldpType;
    }

    /**
     * Get any additional RDF types for the resource
     * @return the user-defined RDF types
     */
    @JsonProperty("type")
    public List<String> getUserTypes() {
        return userTypes;
    }

    /**
     * Set any additional RDF types for the resource
     * @param userTypes the user-defined RDF types
     */
    @JsonProperty("type")
    public void setUserTypes(final List<String> userTypes) {
        this.userTypes = userTypes;
    }

    /**
     * A marker for whether this resource contains its own ACL resource
     * @return true if the resource has its own ACL resource; false otherwise
     */
    public Boolean getHasAcl() {
        return hasAcl;
    }

    /**
     * Set a marker for whether the resource has its own ACL resource
     * @param hasAcl true if the resource has its own ACL resource; false otherwise
     */
    public void setHasAcl(final Boolean hasAcl) {
        this.hasAcl = hasAcl;
    }

    /**
     * Get a list of memento datetime values
     * @return the memento datetime values
     */
    public List<Instant> getGeneratedAtTime() {
        return generatedAtTime;
    }

    /**
     * Set the list of memento datetime values
     * @param generatedAtTime the memento datetime values
     */
    public void setGeneratedAtTime(final List<Instant> generatedAtTime) {
        this.generatedAtTime = generatedAtTime;
    }

    /**
     * Get the binary object data, if available
     * @return the binary data
     */
    public BinaryData getBinary() {
        return binary;
    }

    /**
     * Set the binary object data, if relevant
     * @param binary the binary data
     */
    public void setBinary(final BinaryData binary) {
        this.binary = binary;
    }

    /**
     * Get the ldp:inbox for the resource, if available
     * @return the inbox value
     */
    public String getInbox() {
        return inbox;
    }

    /**
     * Set the ldp:inbox for the resource, if available
     * @param inbox the ldp:inbox value
     */
    public void setInbox(final String inbox) {
        this.inbox = inbox;
    }

    /**
     * Get the oa:annotationService for the resource, if available
     * @return the annotation service IRI
     */
    public String getAnnotationService() {
        return annotationService;
    }

    /**
     * Set the oa:annotationService for the resource, if available
     * @param annotationService the annotation service IRI
     */
    public void setAnnotationService(final String annotationService) {
        this.annotationService = annotationService;
    }

    /**
     * Get the modification date
     * @return the modification date
     */
    public Instant getModified() {
        return modified;
    }

    /**
     * Set the modification date
     * @param modified the modification date
     */
    public void setModified(final Instant modified) {
        this.modified = modified;
    }

    /**
     * Get the ldp:membershipResource, if available
     * @return the ldp:membershipResource IRI
     */
    public String getMembershipResource() {
        return membershipResource;
    }

    /**
     * Set the ldp:membershipResource, if available
     * @param membershipResource the ldp:membershipResource IRI
     */
    public void setMembershipResource(final String membershipResource) {
        this.membershipResource = membershipResource;
    }

    /**
     * Get the ldp:hasMemberRelation, if available
     * @return the ldp:hasMemberRelation IRI
     */
    public String getHasMemberRelation() {
        return hasMemberRelation;
    }

    /**
     * Set the ldp:hasMemberRelation, if available
     * @param hasMemberRelation the ldp:hasMemberRelation IRI
     */
    public void setHasMemberRelation(final String hasMemberRelation) {
        this.hasMemberRelation = hasMemberRelation;
    }

    /**
     * Get the ldp:isMemberOfRelation, if available
     * @return the ldp:isMemberOfRelation IRI
     */
    public String getIsMemberOfRelation() {
        return isMemberOfRelation;
    }

    /**
     * Set the ldp:isMemberOfRelation, if available
     * @param isMemberOfRelation the ldp:isMemberOfRelation IRI
     */
    public void setIsMemberOfRelation(final String isMemberOfRelation) {
        this.isMemberOfRelation = isMemberOfRelation;
    }

    /**
     * Get the ldp:insertedContentRelation, if available
     * @return the ldp:insertedContentRelation IRI
     */
    public String getInsertedContentRelation() {
        return insertedContentRelation;
    }

    /**
     * Set the ldp:insertedContentRelation, if available
     * @param insertedContentRelation the ldp:insertedContentRelation IRI
     */
    public void setInsertedContentRelation(final String insertedContentRelation) {
        this.insertedContentRelation = insertedContentRelation;
    }

    private static final Function<Triple, String> objectUriAsString = triple ->
        ((IRI) triple.getObject()).getIRIString();

    private static final Function<Triple, String> objectLiteralAsString = triple ->
        ((Literal) triple.getObject()).getLexicalForm();

    /**
     * Create a ResourcData object from an identifier and a dataset
     * @param identifier the identifier
     * @param dataset the dataset
     * @param mementos the mementos
     * @return the resource data, if present from the dataset
     */
    public static Optional<ResourceData> from(final IRI identifier, final Dataset dataset,
            final List<Instant> mementos) {
        requireNonNull(identifier, "identifier may not be null!");
        requireNonNull(dataset, "dataset may not be null!");
        requireNonNull(mementos, "mementos may not be null!");

        final ResourceData rd = new ResourceData();
        rd.setId(identifier.getIRIString());
        rd.setGeneratedAtTime(mementos);

        rd.setHasAcl(dataset.stream(of(Trellis.PreferAccessControl), null, RDF.type, ACL.Authorization)
                .findAny().isPresent());

        dataset.getGraph(Trellis.PreferServerManaged).ifPresent(graph -> {
            graph.stream(identifier, DC.modified, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                .ifPresent(rd::setModified);

            graph.stream(identifier, RDF.type, null).findFirst().map(objectUriAsString).ifPresent(rd::setLdpType);

            // Populate binary, if present
            graph.stream(identifier, DC.hasPart, null).findFirst().map(Triple::getObject).map(x -> (IRI) x)
                    .ifPresent(id -> {
                rd.setBinary(new ResourceData.BinaryData());
                rd.getBinary().setId(id.getIRIString());

                graph.stream(id, DC.modified, null).findFirst().map(objectLiteralAsString).map(Instant::parse)
                    .ifPresent(rd.getBinary()::setModified);

                graph.stream(id, DC.format, null).findFirst().map(objectLiteralAsString)
                    .ifPresent(rd.getBinary()::setFormat);

                graph.stream(id, DC.extent, null).findFirst().map(objectLiteralAsString).map(Long::parseLong)
                    .ifPresent(rd.getBinary()::setSize);
            });
        });

        dataset.getGraph(Trellis.PreferUserManaged).ifPresent(graph -> {
            rd.setUserTypes(graph.stream(identifier, RDF.type, null).map(objectUriAsString).collect(toList()));

            graph.stream(identifier, LDP.inbox, null).findFirst().map(objectUriAsString).ifPresent(rd::setInbox);

            graph.stream(identifier, LDP.membershipResource, null).findFirst().map(objectUriAsString)
                .ifPresent(rd::setMembershipResource);

            graph.stream(identifier, LDP.hasMemberRelation, null).findFirst().map(objectUriAsString)
                .ifPresent(rd::setHasMemberRelation);

            graph.stream(identifier, LDP.isMemberOfRelation, null).findFirst().map(objectUriAsString)
                .ifPresent(rd::setIsMemberOfRelation);

            graph.stream(identifier, LDP.insertedContentRelation, null).findFirst().map(objectUriAsString)
                .ifPresent(rd::setInsertedContentRelation);

            graph.stream(identifier, OA.annotationService, null).findFirst().map(objectUriAsString)
                .ifPresent(rd::setAnnotationService);
        });
        return of(rd).filter(x -> nonNull(x.getLdpType())).filter(x -> nonNull(x.modified));
    }
}
