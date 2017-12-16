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

import static java.time.Instant.MAX;
import static java.time.Instant.now;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Optional;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.trellisldp.api.Resource;
import org.trellisldp.rosid.common.ResourceData;
import org.trellisldp.vocabulary.RDFS;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class VersionedResourceTest {

    private static final RDF rdf = new JenaRDF();

    private File directory;

    @BeforeEach
    public void setUp() throws Exception {
        directory = new File(getClass().getResource("/versionable").toURI());
    }

    @Test
    public void testWriteData() {
        final Optional<Resource> res = VersionedResource.find(directory, "trellis:repository/versioned", MAX);
        assertTrue(res.isPresent());
        assertEquals(7L, res.get().stream().count());
        assertTrue(VersionedResource.write(directory, empty(), of(rdf.createQuad(Trellis.PreferUserManaged,
                            rdf.createIRI("trellis:repository/versioned"), RDFS.label,
                            rdf.createLiteral("Another label"))), now()));
        assertEquals(8L, res.get().stream().count());
    }

    @Test
    public void testReadData() {
        final Optional<ResourceData> data = VersionedResource.read(directory, "trellis:repository/versioned", MAX);
        assertTrue(data.isPresent());
        data.ifPresent(d -> {
            assertEquals("trellis:repository/versioned", d.getId());
            assertEquals("http://www.w3.org/ns/ldp#BasicContainer", d.getLdpType());
            assertEquals("http://example.org/receiver/inbox", d.getInbox());
            assertTrue(d.getUserTypes().contains("http://example.org/types/Foo"));
            assertTrue(d.getUserTypes().contains("http://example.org/types/Bar"));
        });
    }
}
