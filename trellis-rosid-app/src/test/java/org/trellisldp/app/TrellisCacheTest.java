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
package org.trellisldp.rosid.app;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.cache.Cache;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class TrellisCacheTest {

    @Mock
    private Cache<String, String> mockCache;

    @BeforeEach
    public void setUp() throws ExecutionException {
        initMocks(this);
        when(mockCache.get(any(), any())).thenThrow(ExecutionException.class);
    }

    @Test
    public void testCache() {
        final TrellisCache<String, String> cache = new TrellisCache<>(newBuilder().maximumSize(5).build());
        assertEquals("longer", cache.get("long", x -> x + "er"));
    }

    @Test
    public void testCacheException() throws Exception {
        final TrellisCache<String, String> cache = new TrellisCache<>(mockCache);
        assertNull(cache.get("long", x -> x + "er"));
    }
}
