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

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.time.Instant.parse;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jena.riot.tokens.TokenizerFactory.makeTokenizerString;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.FileUtils.stringToQuad;
import static org.trellisldp.vocabulary.RDF.type;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.tokens.Tokenizer;
import org.slf4j.Logger;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;
import org.trellisldp.vocabulary.XSD;

/**
 * @author acoburn
 */
final class RDFPatch {

    private static final Logger LOGGER = getLogger(RDFPatch.class);

    private static final String ADD = "A ";
    private static final String DELETE = "D ";
    private static final String TX = "TX .";
    private static final String TX_COMMIT = "TC .";
    private static final String MODIFIED_HEADER = "H modified ";

    /**
     * Read the triples from the journal that existed up to (and including) the specified time
     * @param rdf the rdf object
     * @param file the file
     * @param identifier the identifier
     * @param time the time
     * @return a stream of RDF triples
     */
    public static Stream<Quad> asStream(final RDF rdf, final File file, final IRI identifier, final Instant time) {
        LOGGER.debug("Reading Journal for {} as quads", identifier);
        final StreamReader reader = new StreamReader(rdf, file, identifier, time);
        return stream(spliteratorUnknownSize(reader, IMMUTABLE | NONNULL | ORDERED), false).onClose(reader::close);
    }

    /**
     * Retrieve time values for the history of the resource
     * @param file the file
     * @return a list of VersionRange objects
     */
    public static List<VersionRange> asTimeMap(final File file) {
        LOGGER.debug("Reading Journal for TimeMap data");
        final List<VersionRange> ranges = new ArrayList<>();
        try (final TimeMapReader reader = new TimeMapReader(file)) {
            reader.forEachRemaining(ranges::add);
        }
        return unmodifiableList(ranges);
    }

    /**
     * Write RDF Patch statements to the specified file
     * @param file the file
     * @param delete the quads to delete
     * @param add the quads to add
     * @param time the time
     * @return true if the write succeeds; false otherwise
     */
    public static Boolean write(final File file, final Stream<? extends Quad> delete, final Stream<? extends Quad> add,
            final Instant time) {
        LOGGER.debug("Writing Journal at {}", file.getPath());
        try (final BufferedWriter writer = newBufferedWriter(file.toPath(), UTF_8, CREATE, APPEND)) {
            writer.write(MODIFIED_HEADER + "\"" + time.truncatedTo(MILLIS) + "\"^^" + XSD.dateTimeStamp + " ." +
                    lineSeparator());
            writer.write(TX + lineSeparator());
            final Iterator<String> delIter = delete.map(quadToString).iterator();
            while (delIter.hasNext()) {
                writer.write(DELETE + delIter.next() + lineSeparator());
            }
            final Iterator<String> addIter = add.map(quadToString).iterator();
            while (addIter.hasNext()) {
                writer.write(ADD + addIter.next() + lineSeparator());
            }
            writer.write(TX_COMMIT + lineSeparator());
        } catch (final IOException ex) {
            LOGGER.error("Error writing data to resource {}: {}", file, ex.getMessage());
            return false;
        }
        return true;
    }

    public static final Function<Quad, String> quadToString = quad ->
        join(" ",
                quad.getSubject().ntriplesString(), quad.getPredicate().ntriplesString(),
                quad.getObject().ntriplesString(),
                quad.getGraphName().orElse(Trellis.PreferUserManaged).ntriplesString(), ".");

    /**
     * Convert a "modified" header field into an Instant
     * @param line the line
     * @return the instant
     */
    private static Instant modifiedToInstant(final String line) {
        final Tokenizer tokenizer = makeTokenizerString(line);
        try {
            tokenizer.next(); // H
            tokenizer.next(); // modified
            if (tokenizer.hasNext()) {
                final Node n = tokenizer.next().asNode();
                if (nonNull(n) && n.isLiteral()) {
                    return parse(n.getLiteralLexicalForm());
                }
            }
        } finally {
            tokenizer.close();
        }
        return null;
    }

    /**
     * A class for reading an RDF Patch file into a VersionRange Iterator
     */
    static class TimeMapReader implements Iterator<VersionRange>, AutoCloseable {
        private final Stream<String> lineStream;
        private final Iterator<String> allLines;
        private Instant from = null;
        private Boolean hasUserTriples = false;
        private VersionRange buffer = null;

        /**
         * Create a time map reader
         * @param file the file
         */
        public TimeMapReader(final File file) {
            try {
                lineStream = lines(file.toPath());
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            allLines = lineStream.iterator();
            tryAdvance();
        }

        @Override
        public boolean hasNext() {
            return nonNull(buffer);
        }

        @Override
        public VersionRange next() {
            final VersionRange range = buffer;
            tryAdvance();
            if (nonNull(range)) {
                return range;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            LOGGER.trace("Closing Journal from Timemap");
            lineStream.close();
        }

        private static Boolean isUserTripleQuad(final String line) {
            return line.endsWith(Trellis.PreferUserManaged + " .") ||
                        line.endsWith(Trellis.PreferServerManaged + " .");
        }

        private void tryAdvance() {
            Instant time = null;
            while (allLines.hasNext()) {
                final String line = allLines.next();
                if (line.startsWith(MODIFIED_HEADER)) {
                    time = modifiedToInstant(line);
                    hasUserTriples = false;
                } else if (isUserTripleQuad(line)) {
                    hasUserTriples = true;
                } else if (line.startsWith(TX_COMMIT) && hasUserTriples && nonNull(time)) {
                    if (nonNull(from)) {
                        if (time.isAfter(from.truncatedTo(MILLIS))) {
                            buffer = new VersionRange(from, time);
                            from = time;
                        }
                        return;
                    }
                    from = time;
                }
            }
            buffer = null;
        }
    }

    /**
     * A class for reading an RDFPatch file into a Quad Iterator.
     */
    static class StreamReader implements Iterator<Quad>, AutoCloseable {

        private final Set<Quad> deleted = new HashSet<>();

        private final Set<Quad> patchDeleted = new HashSet<>();
        private final Set<Quad> patchAdded = new HashSet<>();

        private final ReversedLinesFileReader reader;
        private final Instant time;
        private final RDF rdf;
        private final IRI identifier;

        private Boolean hasModified = false;
        private Boolean hasModificationQuads = false;
        private Boolean hasContainerModificationQuads = false;

        private Iterator<Quad> bufferIter = null;

        private String line = null;
        private IRI interactionModel = null;
        private Instant momentIfContainer = null;
        private Instant momentIfNotContainer = null;

        /**
         * Create an iterator that reads a file line-by-line in reverse
         * @param rdf the RDF object
         * @param file the file
         * @param identifier the identifier
         * @param time the time
         */
        public StreamReader(final RDF rdf, final File file, final IRI identifier, final Instant time) {
            this.rdf = rdf;
            this.time = time;
            this.identifier = identifier;
            try {
                this.reader = new ReversedLinesFileReader(file, UTF_8);
                this.line = reader.readLine();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
            bufferIter = readPatch();
        }

        @Override
        public boolean hasNext() {
            return bufferIter.hasNext();
        }

        @Override
        public Quad next() {
            final Quad quad = bufferIter.next();
            if (!bufferIter.hasNext()) {
                bufferIter = readPatch();
            }
            return quad;
        }

        @Override
        public void close() {
            LOGGER.trace("Closing stream reader");
            try {
                reader.close();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        private static Boolean isDataLine(final String line) {
            return line.startsWith(ADD) || line.startsWith(DELETE);
        }

        private Instant getModifiedIfInRange(final String line) {
            final Instant modified = modifiedToInstant(line);
            if (nonNull(modified) && !time.isBefore(modified.truncatedTo(MILLIS))) {
                return modified;
            }
            return null;
        }

        private Iterator<Quad> readPatch() {
            Boolean complete = false;
            while (nonNull(line) && !complete) {
                if (line.startsWith(MODIFIED_HEADER)) {
                    final Instant modified = getModifiedIfInRange(line);
                    if (nonNull(modified)) {
                        deleted.addAll(patchDeleted);
                        maybeEmitModifiedQuad(modified);
                        complete = true;
                    }
                } else if (line.startsWith(TX_COMMIT)) {
                    // reset
                    patchDeleted.clear();
                    patchAdded.clear();
                } else if (isDataLine(line)) {
                    final String[] parts = line.split(" ", 2);
                    stringToQuad(rdf, parts[1]).ifPresent(quadHandler(parts[0]));
                }
                try {
                    line = reader.readLine();
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            if (complete && !patchAdded.isEmpty()) {
                return patchAdded.iterator();
            } else if (isNull(line)) {
                return emptyIterator();
            } else {
                return readPatch();
            }
        }

        private Consumer<Quad> quadHandler(final String prefix) {
            return quad -> {
                if (quad.getGraphName().equals(of(LDP.PreferContainment)) ||
                        quad.getGraphName().equals(of(LDP.PreferMembership))) {
                    hasContainerModificationQuads = true;
                } else {
                    hasModificationQuads = true;
                }
                if (prefix.equals("D")) {
                    patchDeleted.add(quad);
                } else if (prefix.equals("A") && !deleted.contains(quad)) {
                    if (quad.getGraphName().filter(Trellis.PreferServerManaged::equals).isPresent() &&
                            quad.getPredicate().equals(type)) {
                        interactionModel = (IRI) quad.getObject();
                    }
                    patchAdded.add(quad);
                }
            };
        }

        private Boolean shouldSetModificationForContainers() {
            return (hasContainerModificationQuads || hasModificationQuads) && isNull(momentIfContainer);
        }

        private Boolean shouldSetModificationForNonContainers() {
            return hasModificationQuads && isNull(momentIfNotContainer);
        }

        private void maybeEmitModifiedQuad(final Instant moment) {
            if (!hasModified && !time.isBefore(moment.truncatedTo(MILLIS))) {
                if (shouldSetModificationForContainers()) {
                    momentIfContainer = moment;
                }
                if (shouldSetModificationForNonContainers()) {
                    momentIfNotContainer = moment;
                }
                if (LDP.RDFSource.equals(interactionModel) || LDP.NonRDFSource.equals(interactionModel)) {
                    if (nonNull(momentIfNotContainer)) {
                        patchAdded.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                                rdf.createLiteral(momentIfNotContainer.toString(), XSD.dateTime)));
                        hasModified = true;
                    }
                } else if (nonNull(interactionModel) && nonNull(momentIfContainer)) {
                    patchAdded.add(rdf.createQuad(Trellis.PreferServerManaged, identifier, DC.modified,
                            rdf.createLiteral(momentIfContainer.toString(), XSD.dateTime)));
                    hasModified = true;
                }
            }
        }
    }

    private RDFPatch() {
        // prevent instantiation
    }
}
