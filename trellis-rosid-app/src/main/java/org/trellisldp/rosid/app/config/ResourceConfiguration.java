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
package org.trellisldp.rosid.app.config;

import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author acoburn
 */
public class ResourceConfiguration {

    @NotNull
    private String path;

    /**
     * Get the underlying path for file-based resources
     * @return the path
     */
    @JsonProperty
    public String getPath() {
        return path;
    }

    /**
     * Set the underlying path for file-based resources
     * @param path the path
     */
    @JsonProperty
    public void setPath(final String path) {
        this.path = path;
    }
}


