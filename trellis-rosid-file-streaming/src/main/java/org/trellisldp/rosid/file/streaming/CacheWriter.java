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

import static java.util.Objects.isNull;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.rosid.file.FileUtils.resourceDirectory;

import java.io.File;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.trellisldp.rosid.file.CachedResource;

/**
 * @author acoburn
 */
public class CacheWriter extends DoFn<KV<String, String>, KV<String, String>> {

    private static final Logger LOGGER = getLogger(CacheWriter.class);

    private final String dataLocation;

    /**
     * Create a new cache writer processor
     * @param dataLocation the dataLocation configuration
     */
    public CacheWriter(final String dataLocation) {
        super();
        LOGGER.debug("Building CacheWriter with dataLocation: {}", dataLocation);
        this.dataLocation = dataLocation;
    }

    /**
     * A method for processing each element
     * @param c the context
     */
    @ProcessElement
    public void processElement(final ProcessContext c) {
        final KV<String, String> element = c.element();
        final String key = element.getKey();
        final File dir = resourceDirectory(dataLocation, key);
        if (!isNull(dir)) {
            LOGGER.debug("Writing cache for: {}", key);
            if (CachedResource.write(dir, key)) {
                c.output(element);
            } else {
                LOGGER.error("Error writing cached resource for {}", key);
            }
        } else {
            LOGGER.error("Error accessing cached resource location for {}", key);
        }
    }
}
