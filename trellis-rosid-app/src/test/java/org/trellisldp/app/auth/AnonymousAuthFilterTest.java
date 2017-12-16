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
package org.trellisldp.rosid.app.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import io.dropwizard.auth.AuthFilter;

import java.io.IOException;
import java.security.Principal;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.trellisldp.vocabulary.Trellis;

/**
 * @author acoburn
 */
@RunWith(JUnitPlatform.class)
public class AnonymousAuthFilterTest {

    @Mock
    private ContainerRequestContext mockContext;

    @Mock
    private SecurityContext mockSecurityContext;

    @Captor
    private ArgumentCaptor<SecurityContext> securityCaptor;

    @BeforeEach
    public void setupTests() {
        initMocks(this);
    }

    @Test
    public void testAuthFilter() throws IOException {
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

        when(mockContext.getHeaders()).thenReturn(headers);

        final AuthFilter<String, Principal> filter = new AnonymousAuthFilter.Builder()
            .setAuthenticator(new AnonymousAuthenticator()).buildAuthFilter();
        filter.filter(mockContext);

        verify(mockContext).setSecurityContext(securityCaptor.capture());

        assertEquals(Trellis.AnonymousUser.getIRIString(), securityCaptor.getValue().getUserPrincipal().getName());
        assertFalse(securityCaptor.getValue().isUserInRole("role"));
        assertFalse(securityCaptor.getValue().isSecure());
        assertEquals("NONE", securityCaptor.getValue().getAuthenticationScheme());
    }

    @Test
    public void testAuthFilterSecure() throws IOException {
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

        when(mockContext.getHeaders()).thenReturn(headers);
        when(mockContext.getSecurityContext()).thenReturn(mockSecurityContext);
        when(mockSecurityContext.isSecure()).thenReturn(true);

        final AuthFilter<String, Principal> filter = new AnonymousAuthFilter.Builder()
            .setAuthenticator(new AnonymousAuthenticator()).buildAuthFilter();
        filter.filter(mockContext);

        verify(mockContext).setSecurityContext(securityCaptor.capture());

        assertEquals(Trellis.AnonymousUser.getIRIString(), securityCaptor.getValue().getUserPrincipal().getName());
        assertFalse(securityCaptor.getValue().isUserInRole("role"));
        assertTrue(securityCaptor.getValue().isSecure());
        assertEquals("NONE", securityCaptor.getValue().getAuthenticationScheme());
    }

    @Test
    public void testAuthFilterNotSecure() throws IOException {
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

        when(mockContext.getHeaders()).thenReturn(headers);
        when(mockContext.getSecurityContext()).thenReturn(mockSecurityContext);
        when(mockSecurityContext.isSecure()).thenReturn(false);

        final AuthFilter<String, Principal> filter = new AnonymousAuthFilter.Builder()
            .setAuthenticator(new AnonymousAuthenticator()).buildAuthFilter();
        filter.filter(mockContext);

        verify(mockContext).setSecurityContext(securityCaptor.capture());

        assertEquals(Trellis.AnonymousUser.getIRIString(), securityCaptor.getValue().getUserPrincipal().getName());
        assertFalse(securityCaptor.getValue().isUserInRole("role"));
        assertFalse(securityCaptor.getValue().isSecure());
        assertEquals("NONE", securityCaptor.getValue().getAuthenticationScheme());
    }

    @Test
    public void testUnauthorized() throws IOException {
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer blahblahblah");

        when(mockContext.getHeaders()).thenReturn(headers);

        final AuthFilter<String, Principal> filter = new AnonymousAuthFilter.Builder()
            .setAuthenticator(new AnonymousAuthenticator()).buildAuthFilter();

        assertThrows(WebApplicationException.class, () -> filter.filter(mockContext));
    }
}
