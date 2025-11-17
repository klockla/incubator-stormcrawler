/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.pii;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.*;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link PiiBolt}. */
class PiiBoltTest {

    private PiiBolt bolt;
    private OutputCollector collector;
    private TopologyContext context;
    
    private static final String MOCK_REDACTER_CLASS = "org.apache.stormcrawler.pii.MockPiiRedacter";

    @BeforeEach
    void setUp() {
        // Mock Storm infrastructure
        collector = mock(OutputCollector.class);
        context = mock(TopologyContext.class);

        // Prepare a minimal configuration
        Map<String, Object> conf = new HashMap<>();
        conf.put(PiiBolt.PII_REDACTER_CLASS_PARAM, MOCK_REDACTER_CLASS);
        conf.put(PiiBolt.PII_ENABLE_FIELD, false);
        conf.put(PiiBolt.PII_DETECT_LANGUAGE_PARAM, false);

        bolt = new PiiBolt();
        bolt.prepare(conf, context, collector);
    }

    /** A simple redacter used only for the test – it replaces “secret” with “*****”. */
    public static class MockPiiRedacter implements PiiRedacter {
        @Override public void init(Map<String, Object> conf) { /* no‑op */ }
        @Override public String redact(String content) { return redact(content, null); }
        @Override public String redact(String content, String language) {
            return content.replaceAll("secret", "*****");
        }
    }

    @Test
    void testRedactionAndMetadata() {

	    Map<String, Object> conf = new HashMap<>();
        conf.put(PiiBolt.PII_REDACTER_CLASS_PARAM, MOCK_REDACTER_CLASS);
	    conf.put(PiiBolt.PII_ENABLE_FIELD, true);
	    bolt.prepare(conf, context, collector);

        // Input tuple
        String url = "http://example.com";
        String html = "<html><body>this is a secret page</body></html>";
        byte[] contentBytes = html.getBytes(StandardCharsets.UTF_8);
        Metadata md = new Metadata();
        md.addValue("some", "value");
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(url);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        when(input.getValueByField("metadata")).thenReturn(md);
        when(input.getStringByField("text")).thenReturn("this is a secret page");


        // Execute bolt
        bolt.execute(input);

        // Capture emitted tuple
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        Values emitted = valuesCaptor.getValue();

        // Verify URL unchanged
        assertEquals(url, emitted.get(0));

        // Verify HTML binary content has been replaced by dummy content
        byte[] emittedBytes = (byte[]) emitted.get(1);
        String emittedContent = new String(emittedBytes, StandardCharsets.UTF_8);
        assertEquals(PiiBolt.REDACTED_HTML, emittedContent);
        
        String emittedText = (String)emitted.get(3);
        
        // Verify text has been properly redacted
        assertTrue(emittedText.contains("*****"));
        assertFalse(emittedText.contains("secret"));

        // Verify metadata flag
        Metadata outMd = (Metadata) emitted.get(2);
        assertEquals("true", outMd.getFirstValue("pii.processed"));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testDisabledRedaction() {
        // Re‑prepare bolt with disabling flag set
        Map<String, Object> conf = new HashMap<>();
        conf.put(PiiBolt.PII_REDACTER_CLASS_PARAM, MOCK_REDACTER_CLASS);
        conf.put(PiiBolt.PII_ENABLE_FIELD, false);
        bolt.prepare(conf, context, collector);

        String url = "http://example.com";
        byte[] contentBytes = "<html>secret</html>".getBytes(StandardCharsets.UTF_8);
        Metadata md = new Metadata();
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(url);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        when(input.getValueByField("metadata")).thenReturn(md);
        when(input.getStringByField("text")).thenReturn("irrelevant");

        bolt.execute(input);

        // Should emit original bytes unchanged
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        Values emitted = valuesCaptor.getValue();
        assertArrayEquals(contentBytes, (byte[]) emitted.get(1));

        // No pii.processed flag added
        Metadata outMd = (Metadata) emitted.get(2);
        assertNull(outMd.getFirstValue("pii.processed"));

        verify(collector).ack(input);
    }
    
    
    @Test
    void testRedactionEmptyText() {

	    Map<String, Object> conf = new HashMap<>();
        conf.put(PiiBolt.PII_REDACTER_CLASS_PARAM, MOCK_REDACTER_CLASS);
	    conf.put(PiiBolt.PII_ENABLE_FIELD, true);
	    bolt.prepare(conf, context, collector);

        // Input tuple
        String url = "http://example.com";
        String html = "<html><body>this is a secret page</body></html>";
        byte[] contentBytes = html.getBytes(StandardCharsets.UTF_8);
        Metadata md = new Metadata();
        md.addValue("some", "value");
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(url);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        when(input.getValueByField("metadata")).thenReturn(md);
        when(input.getStringByField("text")).thenReturn(null);


        // Execute bolt
        bolt.execute(input);

        // Capture emitted tuple
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        Values emitted = valuesCaptor.getValue();

        // Verify URL unchanged
        assertEquals(url, emitted.get(0));

        // Verify HTML binary content has been replaced by dummy content
        byte[] emittedBytes = (byte[]) emitted.get(1);
        String emittedContent = new String(emittedBytes, StandardCharsets.UTF_8);
        assertEquals(PiiBolt.REDACTED_HTML, emittedContent);
        
        String emittedText = (String)emitted.get(3);
        
        // Verify text is empty string
        assertEquals("", emittedText);

        // Verify metadata flag
        Metadata outMd = (Metadata) emitted.get(2);
        assertEquals("false", outMd.getFirstValue("pii.processed"));

        // Verify ack
        verify(collector).ack(input);
    }
}
