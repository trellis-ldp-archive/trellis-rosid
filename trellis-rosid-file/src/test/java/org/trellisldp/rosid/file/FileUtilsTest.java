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

import static java.io.File.separator;
import static java.lang.String.join;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class FileUtilsTest {

    private static final RDF rdf = new JenaRDF();

    @Test
    public void testPartition1() {
        assertEquals(join(separator, "d3", "68", "a8", "81b7173229c1e865e941211f249ec1b6"),
                FileUtils.partition("trellis:repository/resource"));
        assertEquals(join(separator, "d3", "68", "a8", "81b7173229c1e865e941211f249ec1b6"),
                FileUtils.partition(rdf.createIRI("trellis:repository/resource")));
    }

    @Test
    public void testPartition3() {
        assertEquals(join(separator, "2a", "79", "8c", "70a37cae7da1c312e0d052297e9921aa"),
                FileUtils.partition("trellis:repository/other"));
        assertEquals(join(separator, "2a", "79", "8c", "70a37cae7da1c312e0d052297e9921aa"),
                FileUtils.partition(rdf.createIRI("trellis:repository/other")));
    }

    @Test
    public void testNullResourceDirectory() {
        assertNull(FileUtils.resourceDirectory(null, "trellis:repo/file"));
    }

    @Test
    public void testResourceDirectory() throws Exception {
        final String path = getClass().getResource("/res3").toURI().toString();
        assertTrue(FileUtils.resourceDirectory(path.substring("file:".length()), "trellis:repo/testing").exists());
    }
}
