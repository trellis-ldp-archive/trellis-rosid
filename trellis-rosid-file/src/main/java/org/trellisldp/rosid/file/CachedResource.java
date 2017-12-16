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

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.move;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.time.Instant.now;
import static java.util.Objects.isNull;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.Constants.RESOURCE_CACHE;
import static org.trellisldp.rosid.file.Constants.RESOURCE_JOURNAL;
import static org.trellisldp.rosid.file.Constants.RESOURCE_QUADS;
import static org.trellisldp.rosid.file.FileUtils.stringToQuad;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;

import org.trellisldp.api.Resource;
import org.trellisldp.rosid.common.ResourceData;

/**
 * An object that mediates access to the resource cache files.
 *
 * @author acoburn
 */
public class CachedResource extends AbstractFileResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOGGER = getLogger(CachedResource.class);

    private static final RandomStringGenerator generator = new RandomStringGenerator.Builder()
        .withinRange('a', 'z').build();

    static {
        MAPPER.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    /**
     * Create a File-based resource reader
     * @param directory the data storage directory
     * @param identifier the resource to retrieve
     * @param data the resource data
     */
    protected CachedResource(final File directory, final IRI identifier, final ResourceData data) {
        super(directory, identifier, data);
        LOGGER.debug("Fetching a Cached Resource for {}", identifier.getIRIString());
    }

    /**
     * Retrieve a cached resource, if it exists
     * @param directory the directory
     * @param identifier the identifier
     * @return the resource
     */
    public static Optional<Resource> find(final File directory, final IRI identifier) {
        return read(directory).map(d -> new CachedResource(directory, identifier, d));
    }

    /**
     * Read the cached resource from a directory
     * @param directory the directory
     * @return the resource data, if present
     */
    public static Optional<ResourceData> read(final File directory) {
        if (isNull(directory)) {
            return Optional.empty();
        }

        try {
            LOGGER.debug("Parsing JSON metadata");
            return Optional.of(MAPPER.readValue(new File(directory, RESOURCE_CACHE), ResourceData.class));
        } catch (final IOException ex) {
            LOGGER.warn("Error reading cached resource: {}", ex.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final String identifier) {
        return write(directory, rdf.createIRI(identifier));
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final IRI identifier) {
        return write(directory, identifier, now());
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final String identifier, final Instant time) {
        return write(directory, rdf.createIRI(identifier), time);
    }

    /**
     * Write the resource data into a file as JSON
     * @param directory the directory
     * @param identifier the resource identifier
     * @param time the time
     * @return true if the write operation succeeds
     */
    public static Boolean write(final File directory, final IRI identifier, final Instant time) {

        if (isNull(directory)) {
            return false;
        }

        // Write the JSON file
        LOGGER.debug("Writing JSON cache for {}", identifier);
        final Optional<ResourceData> data = VersionedResource.read(directory, identifier, time);
        final File jsonSource = new File(directory, RESOURCE_CACHE + random(16));
        try {
            if (data.isPresent()) {
                MAPPER.writeValue(jsonSource, data.get());
                moveIntoPlace(jsonSource, new File(directory, RESOURCE_CACHE));
            } else {
                LOGGER.error("No resource data to cache for {}", identifier.getIRIString());
                return false;
            }
        } catch (final IOException ex) {
            LOGGER.error("Error writing resource metadata cache for {}: {}",
                    identifier.getIRIString(), ex.getMessage());
            return false;
        }

        // Write the quads
        LOGGER.debug("Writing NQuads cache for {}", identifier);
        final File nquadSource = new File(directory, RESOURCE_QUADS + random(16));
        try (final BufferedWriter writer = newBufferedWriter(nquadSource.toPath(), UTF_8, CREATE, WRITE,
                    TRUNCATE_EXISTING)) {
            final File file = new File(directory, RESOURCE_JOURNAL);
            try (final Stream<? extends Quad> stream = RDFPatch.asStream(rdf, file, identifier, time)) {
                final Iterator<String> lineIter = stream.map(RDFPatch.quadToString).iterator();
                while (lineIter.hasNext()) {
                    writer.write(lineIter.next() + lineSeparator());
                }
            }
        } catch (final IOException ex) {
            LOGGER.error("Error writing resource cache for {}: {}", identifier.getIRIString(), ex.getMessage());
            return false;
        }

        try {
            LOGGER.trace("Moving NQuad cache into place for {}", identifier);
            moveIntoPlace(nquadSource, new File(directory, RESOURCE_QUADS));
        } catch (final IOException ex) {
            LOGGER.error("Error replacing resource cache: {}", ex.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public Stream<Quad> stream() {
        LOGGER.trace("Streaming quads for {}", identifier);
        final File file = new File(directory, RESOURCE_QUADS);
        if (file.exists()) {
            try {
                // TODO -- JDK9 shortcut Optional::stream and flatMap
                return lines(file.toPath()).map(line -> stringToQuad(rdf, line)).filter(Optional::isPresent)
                    .map(Optional::get);
            } catch (final IOException ex) {
                LOGGER.warn("Could not read file at {}: {}", file, ex.getMessage());
            }
        }
        return empty();
    }

    private static void moveIntoPlace(final File from, final File to) throws IOException {
        try {
            move(from.toPath(), to.toPath(), ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException ex) {
            move(from.toPath(), to.toPath(), REPLACE_EXISTING);
        } finally {
            deleteIfExists(from.toPath());
        }
    }

    private static String random(final Integer length) {
        return generator.generate(length);
    }
}
