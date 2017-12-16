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

import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

import java.util.function.Predicate;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.jena.JenaRDF;

/**
 * @author acoburn
 */
class TestUtils {

    public static final RDF rdf = new JenaRDF();

    public static final Predicate<Quad> isUserManaged = quad ->
        quad.getGraphName().filter(Trellis.PreferUserManaged::equals).isPresent();

    public static final Predicate<Quad> isServerManaged = quad ->
        quad.getGraphName().filter(Trellis.PreferServerManaged::equals).isPresent();

    public static final Predicate<Quad> isContainment = quad ->
        quad.getGraphName().filter(LDP.PreferContainment::equals).isPresent();

    public static final Predicate<Quad> isMembership = quad ->
        quad.getGraphName().filter(LDP.PreferMembership::equals).isPresent();

    private TestUtils() {
        // Prevent instantiation
    }
}
