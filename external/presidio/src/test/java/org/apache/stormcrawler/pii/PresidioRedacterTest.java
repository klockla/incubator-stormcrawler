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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import okhttp3.ResponseBody;

@ExtendWith(MockitoExtension.class)
class PresidioRedacterTest extends PresidioRedacter {

    @Mock
    private OkHttpClient mockHttpClient;
        
    @Mock
    private Call mockCall;
    
    private PresidioRedacter fakeRedacter;

    private String analyzerEndpoint = "http://localhost:5002/analyze";
    		
    private String anonymizerEndpoint = "http://localhost:5001/anonymize";
    
    private static String input = "My name is John Doe (SSN: 489-36-8350) and I live in New York, US.";
    
    private static String analyzerResult =  "[{\"analysis_explanation\": null, \"end\": 24, \"entity_type\": \"PERSON\", \"recognition_metadata\": {\"recognizer_identifier\": \"SpacyRecognizer_140697834419632\", \"recognizer_name\": \"SpacyRecognizer\"}, \"score\": 0.85, \"start\": 11}, {\"analysis_explanation\": null, \"end\": 29, \"entity_type\": \"ORGANIZATION\", \"recognition_metadata\": {\"recognizer_identifier\": \"SpacyRecognizer_140697834419632\", \"recognizer_name\": \"SpacyRecognizer\"}, \"score\": 0.85, \"start\": 26}, {\"analysis_explanation\": null, \"end\": 42, \"entity_type\": \"US_SSN\", \"recognition_metadata\": {\"recognizer_identifier\": \"UsSsnRecognizer_140698153812224\", \"recognizer_name\": \"UsSsnRecognizer\"}, \"score\": 0.85, \"start\": 31}, {\"analysis_explanation\": null, \"end\": 66, \"entity_type\": \"LOCATION\", \"recognition_metadata\": {\"recognizer_identifier\": \"SpacyRecognizer_140697834419632\", \"recognizer_name\": \"SpacyRecognizer\"}, \"score\": 0.85, \"start\": 58}, {\"analysis_explanation\": null, \"end\": 70, \"entity_type\": \"LOCATION\", \"recognition_metadata\": {\"recognizer_identifier\": \"SpacyRecognizer_140697834419632\", \"recognizer_name\": \"SpacyRecognizer\"}, \"score\": 0.85, \"start\": 68}]\r\n"
    		+ "";
    
    private static String expected = "My name is <PERSON> (<ORGANIZATION>: <US_SSN>) and I live in <LOCATION>, <LOCATION>.";
    
    @BeforeEach
    void setUp() {

        fakeRedacter = new PresidioRedacter(analyzerEndpoint, anonymizerEndpoint, mockHttpClient);  
    }

    @Test
    void testInitOk() {
    	PresidioRedacter redacter = new PresidioRedacter();      
    	
    	Map<String, Object> conf = new HashMap<>();
    	conf.put("presidio.analyzer.endpoint", "http://analyzer.org:10000");
    	conf.put("presidio.anonymizer.endpoint",  "http://anonymizer.endpoint:20000");
    	Assertions.assertDoesNotThrow(() -> redacter.init(conf));
    }
    
    @Test
    void testInitfail() {
    	PresidioRedacter redacter = new PresidioRedacter();      
    	
    	Map<String, Object> conf = new HashMap<>();
    	conf.put("presidio.analyzer.endpoint", "http://analyzer.org:10000");
    	Assertions.assertThrows(MalformedURLException.class, () -> redacter.init(conf));
    }
    
    @Test
    void testMockedAnalyzer() throws Exception {
        String language = "en";
        
        Request.Builder reqBuilder = new Builder();
        Request req1 = reqBuilder
        		.url(analyzerEndpoint)
        		.build();

        ResponseBody respBody = ResponseBody.create(JSON, analyzerResult);

        Response.Builder builder = new Response.Builder();
        Response response = builder
        		.request(req1)
        		.body(respBody)
        		.protocol(Protocol.HTTP_1_0)
        		.message("Fake response")
        		.code(200)
        		.build();
        
        
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(response);
        
        String result = fakeRedacter.analyze(input, language);
        assertNotNull(result);
        assertEquals(analyzerResult, result);
    }

    @Test
    void testMockedAnonymizer() throws Exception {
        String expectedBody = "{\"text\": \"My name is <PERSON> (<ORGANIZATION>: <US_SSN>) and I live in <LOCATION>, <LOCATION>.\", \"items\": [{\"start\": 73, \"end\": 83, \"entity_type\": \"LOCATION\", \"text\": \"<LOCATION>\", \"operator\": \"replace\"}, {\"start\": 61, \"end\": 71, \"entity_type\": \"LOCATION\", \"text\": \"<LOCATION>\", \"operator\": \"replace\"}, {\"start\": 37, \"end\": 45, \"entity_type\": \"US_SSN\", \"text\": \"<US_SSN>\", \"operator\": \"replace\"}, {\"start\": 21, \"end\": 35, \"entity_type\": \"ORGANIZATION\", \"text\": \"<ORGANIZATION>\", \"operator\": \"replace\"}, {\"start\": 11, \"end\": 19, \"entity_type\": \"PERSON\", \"text\": \"<PERSON>\", \"operator\": \"replace\"}]}";
        
        Request.Builder reqBuilder = new Builder();
        Request req1 = reqBuilder
        		.url(anonymizerEndpoint)
        		.build();

        ResponseBody respBody = ResponseBody.create(JSON, expectedBody);

        Response.Builder builder = new Response.Builder();
        Response response = builder
        		.request(req1)
        		.body(respBody)
        		.protocol(Protocol.HTTP_1_0)
        		.message("Fake response")
        		.code(200)
        		.build();
        
        
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(response);
        
        String result = fakeRedacter.anonymize(input, analyzerResult);
        assertNotNull(result);
        assertEquals(expected, result);
    }
    
    @Test
    @Disabled("For local tests only")
    void testLocalAnalyzer() {
    	
        PresidioRedacter redacter = new PresidioRedacter(analyzerEndpoint, anonymizerEndpoint);         
    	String result = redacter.analyze(input, "en");
    	assertNotNull(result);
    	assertTrue(result.contains("analysis_explanation"));
    }
    
    @Test
    @Disabled("For local tests only")
    void testLocalAnonymizer() {
    	
        PresidioRedacter redacter = new PresidioRedacter(analyzerEndpoint, anonymizerEndpoint);         
    	String result = redacter.analyze(input, "en");
    	String redacted = redacter.anonymize(input, result);
    	
    	assertNotNull(redacted);
    	assertEquals(expected, redacted);
    }
    
    @Test
    @Disabled("For local tests only")
    void testLocalRedact() {
    	PresidioRedacter redacter = new PresidioRedacter(analyzerEndpoint, anonymizerEndpoint);      
    	
    	String redacted = redacter.redact(input, "en");
    	assertNotNull(redacted);
    	assertEquals(expected, redacted);
    }

    @Test
    void testMockedAnonymizerWithWrongJson() throws Exception {
        // Create a bad object that ObjectMapper might fail to serialize as JSON
        // string
        String badObject = "{ \"key\": { \"value\": [1, 2, 3, \"nestedKey\": { \"nestedValue\": null } } }";

        // Ensure the method returs null
        String result = fakeRedacter.anonymize(input, badObject);
        assertNull(result);
    }
}