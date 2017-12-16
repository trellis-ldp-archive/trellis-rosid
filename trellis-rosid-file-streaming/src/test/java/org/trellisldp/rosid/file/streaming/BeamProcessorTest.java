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
package org.trellisldp.rosid.file.streaming;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

import java.io.File;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.trellisldp.vocabulary.LDP;

/**
 * @author acoburn
 */
public class BeamProcessorTest {

    private final static String MEMBER_DATASET = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferMembership> .";

    private final static String CONTAINER_DATASET = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferContainment> .";

    private final static KV<String, String> MEMBER_KV = KV.of("trellis:repository/resource", MEMBER_DATASET);
    private final static KV<String, String> CONTAINER_KV = KV.of("trellis:repository/resource", CONTAINER_DATASET);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testAddMemberPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(MEMBER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferMembership.getIRIString(), true)));

        PAssert.that(pCollection).containsInAnyOrder(asList(MEMBER_KV));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRemoveMemberPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(MEMBER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferMembership.getIRIString(), false)));

        PAssert.that(pCollection).containsInAnyOrder(asList(MEMBER_KV));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testAddContainerPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(CONTAINER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), true)));

        PAssert.that(pCollection).containsInAnyOrder(asList(CONTAINER_KV));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRemoveContainerPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(CONTAINER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), false)));

        PAssert.that(pCollection).containsInAnyOrder(asList(CONTAINER_KV));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testUnwritableRemoveContainerPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory2").toURI().toString();

        final File root = new File(getClass().getResource("/dataDirectory2").toURI());

        assumeTrue(root.setReadOnly());

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(CONTAINER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), false)));

        PAssert.that(pCollection).empty();

        pipeline.run();
        root.setWritable(true);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testUnwritableAddContainerPipeline() throws Exception {

        final String dataConfiguration = getClass().getResource("/dataDirectory2").toURI().toString();

        final File root = new File(getClass().getResource("/dataDirectory2").toURI());

        assumeTrue(root.setReadOnly());

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(CONTAINER_KV))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), true)));

        PAssert.that(pCollection).empty();

        pipeline.run();
        root.setWritable(true);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testInvalidDataPipeline() throws Exception {

        final String dataset = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferConta";
        final KV<String, String> kv = KV.of("trellis:repository/resource", dataset);

        final String dataConfiguration = getClass().getResource("/dataDirectory").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new BeamProcessor(dataConfiguration, LDP.PreferContainment.getIRIString(), false)));

        PAssert.that(pCollection).empty();

        pipeline.run();
    }
}
