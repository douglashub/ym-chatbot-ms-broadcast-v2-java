package com.ymchatbot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FacebookServiceTest {

    @Mock
    private CloseableHttpAsyncClient httpClient;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private FacebookService facebookService;

    @Captor
    private ArgumentCaptor<HttpPost> httpPostCaptor;
    
    @Captor
    private ArgumentCaptor<FutureCallback<HttpResponse>> callbackCaptor;

    @BeforeEach
    void setUp() {
        // In test mode we pass null for tokenManager and rateProperties to trigger fake behavior.
        facebookService = new FacebookService(httpClient, objectMapper, null, null);
    }

    @Nested
    @DisplayName("When sending message with fake token")
    class FakeTokenTests {
        // This value should match the fake token configured in your application-test.yml.
        private static final String FAKE_TOKEN = "FAKE_FACEBOOK_TOKEN_FOR_TEST";
        private static final String PAYLOAD = "{\"dummy\": \"data\"}";

        @Test
        @DisplayName("Should return test message ID immediately")
        void shouldReturnTestMessageId() throws Exception {
            CompletableFuture<ObjectNode> future = facebookService.sendMessage(FAKE_TOKEN, PAYLOAD);
            ObjectNode response = future.get();
            
            assertEquals("fb_test_msg_id", response.get("message_id").asText());
            verifyNoInteractions(httpClient);
        }
    }

    @Nested
    @DisplayName("When sending message with real token")
    class RealTokenTests {
        // This value is used to simulate a real token.
        private static final String REAL_TOKEN = "VALID_FACEBOOK_TOKEN";
        private static final String PAYLOAD = "{\"message\": \"Hello\"}";
        private static final String EXPECTED_URL = "https://graph.facebook.com/v22.0/me/messages?access_token=" + REAL_TOKEN;

        @Test
        @DisplayName("Should successfully send message and return message ID")
        void shouldSendMessageSuccessfully() throws Exception {
            HttpResponse httpResponse = mock(HttpResponse.class);
            HttpEntity entity = new StringEntity("{\"message_id\": \"fb_actual_msg_id\"}", StandardCharsets.UTF_8);
            when(httpResponse.getEntity()).thenReturn(entity);

            CompletableFuture<ObjectNode> future = facebookService.sendMessage(REAL_TOKEN, PAYLOAD);
            
            verify(httpClient).execute(httpPostCaptor.capture(), callbackCaptor.capture());
            
            HttpPost capturedPost = httpPostCaptor.getValue();
            assertEquals(EXPECTED_URL, capturedPost.getURI().toString());
            assertEquals("application/json", capturedPost.getFirstHeader("Content-Type").getValue());
            
            callbackCaptor.getValue().completed(httpResponse);
            
            ObjectNode result = future.get();
            assertEquals("fb_actual_msg_id", result.get("message_id").asText());
        }

        @Test
        @DisplayName("Should handle HTTP request failure")
        void shouldHandleRequestFailure() {
            CompletableFuture<ObjectNode> future = facebookService.sendMessage(REAL_TOKEN, PAYLOAD);
            
            verify(httpClient).execute(httpPostCaptor.capture(), callbackCaptor.capture());
            
            RuntimeException simulatedError = new RuntimeException("Simulated failure");
            callbackCaptor.getValue().failed(simulatedError);
            
            ExecutionException exception = assertThrows(ExecutionException.class, future::get);
            assertEquals(simulatedError, exception.getCause());
        }

        @Test
        @DisplayName("Should handle request cancellation")
        void shouldHandleRequestCancellation() {
            CompletableFuture<ObjectNode> future = facebookService.sendMessage(REAL_TOKEN, PAYLOAD);
            
            verify(httpClient).execute(httpPostCaptor.capture(), callbackCaptor.capture());
            
            callbackCaptor.getValue().cancelled();
            
            ExecutionException exception = assertThrows(ExecutionException.class, future::get);
            assertEquals("Request cancelled", exception.getCause().getMessage());
        }

        @Test
        @DisplayName("Should handle response processing error")
        void shouldHandleResponseProcessingError() throws Exception {
            HttpResponse httpResponse = mock(HttpResponse.class);
            HttpEntity entity = new StringEntity("invalid json", StandardCharsets.UTF_8);
            when(httpResponse.getEntity()).thenReturn(entity);

            CompletableFuture<ObjectNode> future = facebookService.sendMessage(REAL_TOKEN, PAYLOAD);
            
            verify(httpClient).execute(httpPostCaptor.capture(), callbackCaptor.capture());
            
            callbackCaptor.getValue().completed(httpResponse);
            
            ExecutionException exception = assertThrows(ExecutionException.class, future::get);
            assertTrue(exception.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException);
        }
    }

    @Test
    @DisplayName("Should handle HTTP client initialization error")
    void shouldHandleInitializationError() {
        RuntimeException initializationError = new RuntimeException("Initialization error");
        doThrow(initializationError).when(httpClient).execute(any(HttpPost.class), any(FutureCallback.class));

        // Because the exception is thrown synchronously by the execute() call, we expect sendMessage() itself to throw.
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            facebookService.sendMessage("ANY_TOKEN", "{}");
        });
        assertEquals(initializationError, thrown);
    }
}
