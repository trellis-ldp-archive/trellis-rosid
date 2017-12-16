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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.junit.Test;

/**
 * @author acoburn
 */
public class FileProcessingPipelineTest {

    @Test
    public void testConfigLoader() throws Exception {
        final File file = new File(getClass().getResource("/test.config").toURI());
        final Properties config = FileProcessingPipeline.loadConfiguration(file.getPath());
        assertEquals("/tmp/data/trellis/resources",
                config.getProperty("trellis.data"));
    }

    @Test(expected = IOException.class)
    public void testNoConfiguration() throws Exception {
        final File parent = new File(getClass().getResource("/test.config").toURI()).getParentFile();
        final File file = new File(parent, "non-existant.config");
        FileProcessingPipeline.loadConfiguration(file.getPath());
    }

    @Test
    public void testPiplelineLoader() throws Exception {
        final File file = new File(getClass().getResource("/test.config").toURI());
        final Pipeline pipeline = FileProcessingPipeline.loadPipeline(new String[]{file.getPath()});
        assertNotNull(pipeline);
    }

    @Test(expected = IOException.class)
    public void testPipelineLoaderNoConfig() throws IOException {
        FileProcessingPipeline.loadPipeline(new String[]{});
    }

    @Test
    public void testPiplelineNoAggregateLoader() throws Exception {
        final File file = new File(getClass().getResource("/testNoAggregate.config").toURI());
        final Pipeline pipeline = FileProcessingPipeline.loadPipeline(new String[]{file.getPath()});
        assertNotNull(pipeline);
    }
}
