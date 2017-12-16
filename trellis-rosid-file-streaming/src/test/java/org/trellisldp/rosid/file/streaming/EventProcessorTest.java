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

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author acoburn
 */
public class EventProcessorTest {

    private final static String MEMBER_DATASET = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferMembership> .";

    private final static String CONTAINER_DATASET = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferContainment> .";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    private static class VerifyEvent implements SerializableFunction<Iterable<KV<String, String>>, Void> {
        private final String url;

        public VerifyEvent(final String url) {
            this.url = url;
        }

        @Override
        public Void apply(final Iterable<KV<String, String>> data) {
            int i = 0;
            for (final KV<String, String> d : data) {
                assertTrue(d.getValue().contains(this.url));
                i += 1;
            }
            assertTrue(1 == i);
            return null;
        }
    }

    @Test
    @Category(NeedsRunner.class)
    public void testAddMemberPipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", MEMBER_DATASET);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).satisfies(new VerifyEvent("http://localhost/repository/resource"));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRemoveMemberPipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", MEMBER_DATASET);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).satisfies(new VerifyEvent("http://localhost/repository/resource"));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testAddContainerPipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", CONTAINER_DATASET);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).satisfies(new VerifyEvent("http://localhost/repository/resource"));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRemoveContainerPipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", CONTAINER_DATASET);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).satisfies(new VerifyEvent("http://localhost/repository/resource"));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testNoDerefTermPipeline() throws Exception {

        final KV<String, String> kv = KV.of("foo:repository/resource", CONTAINER_DATASET);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).satisfies(new VerifyEvent("foo:repository/resource"));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testInvalidDataPipeline() throws Exception {

        final String dataset = "<trellis:repository/resource> " +
            "<http://purl.org/dc/terms/subject> <trellis:repository/resource/member> " +
            "<http://www.w3.org/ns/ldp#PreferConta";
        final KV<String, String> kv = KV.of("trellis:repository/resource", dataset);

        final String dataConfiguration = "http://localhost/";

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new EventProcessor(dataConfiguration)));

        PAssert.that(pCollection).empty();

        pipeline.run();
    }
}
