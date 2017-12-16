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

/**
 * @author acoburn
 */
public class CacheWriterTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testCachePipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", null);

        final String dataConfiguration = getClass().getResource("/root").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new CacheWriter(dataConfiguration)));

        PAssert.that(pCollection).containsInAnyOrder(asList(kv));

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testUnableToCachePipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/some-other-resource", null);

        final String dataConfiguration = getClass().getResource("/root").toURI().toString();

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new CacheWriter(dataConfiguration)));

        PAssert.that(pCollection).empty();

        pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testUnwritablePipeline() throws Exception {

        final KV<String, String> kv = KV.of("trellis:repository/resource", null);

        final String dataConfiguration = getClass().getResource("/dataDirectory2").toURI().toString();

        final File root = new File(getClass().getResource("/dataDirectory2").toURI());

        assumeTrue(root.setReadOnly());

        final PCollection<KV<String, String>> pCollection = pipeline
            .apply("Create", Create.of(kv))
            .apply(ParDo.of(new CacheWriter(dataConfiguration)));

        PAssert.that(pCollection).empty();

        pipeline.run();
        root.setWritable(true);
    }
}
