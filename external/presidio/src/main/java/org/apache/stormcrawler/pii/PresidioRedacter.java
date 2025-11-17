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

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import org.apache.stormcrawler.util.ConfUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * PII Redacter implementation for Microsoft Presidio
 */
public class PresidioRedacter implements PiiRedacter {
	
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PresidioRedacter.class);
	
    private static final String PRESIDIO_ANONYMIZER_ENDPOINT = "presidio.anonymizer.endpoint";

	private static final String PRESIDIO_ANALYZER_ENDPOINT = "presidio.analyzer.endpoint";

	private static final String PRESIDIO_ANALYZER_ENTITIES = "presidio.analyzer.entities";
	
	private static final String PRESIDIO_SUPPORTED_LANGUAGES = "presidio.supported.languages";
	
	private String analyzerEndpoint = "https://your-presidio-endpoint/analyze";

	private String anonymizerEndpoint = "https://your-presidio-endpoint/anonymize"; ;

	private List<String> analyzerEntities = null;
	
	private List<String> supportedLanguages = Arrays.asList("en", "fr", "de", "xx");
	
	private OkHttpClient httpClient = new OkHttpClient();

	public static final MediaType JSON = MediaType.get("application/json");

	public PresidioRedacter() {
		LOG.info("Created PresidioRedactor instance");	
	}
	
	// Dependency with Jdk8Module should be removed when Storm dependency will use a version > 2.20
	private ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module());
	
	static class AnalyzerPayload {
		
		public String text;
		public String language;
		public Optional<List<String>> entities;
		
		AnalyzerPayload(String input, String lang, Optional<List<String>> entities) {
			this.text = input;
			this.language = lang;
			this.entities = entities;
		}
	}
		
	static class AnonymizerPayload {
		
		public String text;
		public JsonNode analyzer_results;
		
		AnonymizerPayload(String input, JsonNode results) {
			this.text = input;
			this.analyzer_results = results;
		}
	}
	
	public PresidioRedacter(String analyzerEndpoint, String anonymizerEndpoint) {
		this.analyzerEndpoint = analyzerEndpoint;
		this.anonymizerEndpoint = anonymizerEndpoint;

		LOG.info("Created PresidioRedactor instance  with:");
		LOG.info("Analyzer endpoint:   {}", analyzerEndpoint);
		LOG.info("Anonymizer endpoint: {}", anonymizerEndpoint);		
	}	

	protected PresidioRedacter(String analyzerEndpoint, String anonymizerEndpoint, OkHttpClient client) {
		this(analyzerEndpoint, anonymizerEndpoint);
		this.httpClient = client;
	}	

	/**
	 * Retrieve the endpoints from the topology configuration
	 */
	@Override
	public void init(Map<String, Object> topologyConf) throws MalformedURLException {
		LOG.info("Initializating PresidioRedacter...");
		
		this.analyzerEndpoint = ConfUtils.getString(topologyConf, PRESIDIO_ANALYZER_ENDPOINT);
		this.anonymizerEndpoint = ConfUtils.getString(topologyConf, PRESIDIO_ANONYMIZER_ENDPOINT);
		
		String entitiesString = ConfUtils.getString(topologyConf, PRESIDIO_ANALYZER_ENTITIES);
		
		if (entitiesString != null && StringUtils.isNotBlank(entitiesString)) {
			List<String> entities = Arrays.asList(entitiesString.split(",", -1));
			if (!entities.isEmpty()) {
				this.analyzerEntities = entities;
				LOG.info("Analyzer entities: {}", entitiesString);
			}
		}
		
		String supLangs = ConfUtils.getString(topologyConf, PRESIDIO_SUPPORTED_LANGUAGES);
		if (supLangs != null && StringUtils.isNotBlank(supLangs)) {
			List<String> langs = Arrays.asList(supLangs.split(",", -1));
			if (!langs.isEmpty()) {
				this.supportedLanguages = langs;
			}
		}
		LOG.info("Analyzer supported languages: {}", this.supportedLanguages);

		
		if (StringUtils.isBlank(analyzerEndpoint) || StringUtils.isBlank(anonymizerEndpoint)) {
			String msg = "Presidio Analyzer and anonymizer endpoints can not be null or empty !";
			LOG.error(msg);
			throw new MalformedURLException(msg);
		}
		
		
		LOG.info("Analyzer endpoint:   {}", analyzerEndpoint);
		LOG.info("Anonymizer endpoint: {}", anonymizerEndpoint);
		
	}

	/**
	 * Calls the analyzer with the multi lingual model
	 * and then the anonymizer
	 */
	@Override
	public String redact(String input) {
        String analyzerResult = analyze(input, "xx");
		String anonymizerResult = null;

		if (analyzerResult != null) {
			anonymizerResult = anonymize(input, analyzerResult);
		}

		return anonymizerResult;
	}
	
	/**
	 * Calls the analyzer in a specific language 
	 * and then the anonymizer
	 */
	@Override
	public String redact(String input, String language) {
		String analyzerResult = null;
		String anonymizerResult = null;

		if (supportedLanguages.contains(language)) {
			analyzerResult = analyze(input, language);
		} else {
			LOG.warn("Language {} not supported by PresidioRedactor. Falling back to multi-lingual model (xx)", language);
			analyzerResult = analyze(input, "xx");
		}

		if (analyzerResult != null) {
			anonymizerResult = anonymize(input, analyzerResult);
		}

		return anonymizerResult;
	}
	
	public List<String> getSupportedLanguages() {
		return supportedLanguages;
	}

	/**
	 * Calls the Presidio analyzer endpoint
	 * @param input
	 * @param language
	 * @return
	 */
	protected String analyze(String input, String language) {
		String analyzerResult = null;

		LOG.info("Calling presidio for analyzing text in language {}:", language);

		AnalyzerPayload objPayload = new AnalyzerPayload(input, language, Optional.ofNullable(analyzerEntities));
		String payload;
		try {
			payload = objectMapper.writeValueAsString(objPayload);
		} catch (JsonProcessingException e) {
			LOG.error(e.getMessage(), e);
			LOG.error("Input was:");
			LOG.error(input);
			return null;
		}

		RequestBody body = RequestBody.create(payload, JSON);
		Request request = new Request.Builder()
			.url(analyzerEndpoint)
			.post(body)
			.build();
		
		Instant start = Instant.now();

		try (Response response = httpClient.newCall(request).execute()) {
			if (response.isSuccessful()) {
				analyzerResult = response.body().string();
			} else {
				LOG.error("Response not successfull: {}", response.code());
				String errbody = response.body().string();
				LOG.error(errbody);
				LOG.error("Input was:");
				LOG.error(input);
			}
		} catch (IOException e) {
			LOG.error("Error calling Presidio analyzer:");
			LOG.error(e.getMessage(), e);
		}
		
		Instant finish = Instant.now();
		long elapsed = Duration.between(start, finish).toMillis();
		LOG.info("Analyzer request took {} ms", elapsed);

		return analyzerResult;
	}

	/**
	 * Calls the Presidio anonymizer endpoint
	 * @param input
	 * @param analyzerResults
	 * @return
	 */
	protected String anonymize(String input, String analyzerResults) {
		String anonymizedText = null;

		LOG.info("Calling presidio anonymizer");
		
		String payload;
		try {
			JsonNode tree = objectMapper.readTree(analyzerResults);
			AnonymizerPayload objPayload = new AnonymizerPayload(input, tree);
			payload = objectMapper.writeValueAsString(objPayload);
		} catch (JsonProcessingException e) {
			LOG.error(e.getMessage(), e);
			LOG.error("Analyzer results was:");
			LOG.error(analyzerResults);
			return null;
		}
		
		RequestBody body = RequestBody.create(payload, JSON);
		Request request = new Request.Builder()
			.url(anonymizerEndpoint)
			.post(body)
			.build();
		
		Instant start = Instant.now();

		String responseBody = null;
		try (Response response = httpClient.newCall(request).execute()) {
			if (response.isSuccessful()) {
				responseBody = response.body().string();
			} else {
				LOG.error("Response not successfull: {}", response.code());
				String errbody = response.body().string();
				LOG.error(errbody);
			}
		} catch (IOException e) {
			LOG.error("Error calling Presidio analyzer:");
			LOG.error(e.getMessage(), e);
		}
		
		Instant finish = Instant.now();
		long elapsed = Duration.between(start, finish).toMillis();
		LOG.info("Anonymizer request took {} ms", elapsed);

		if (Objects.nonNull(responseBody)) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				JsonNode root = mapper.readTree(responseBody);
				anonymizedText = root.path("text").asText();
			} catch (JsonProcessingException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return anonymizedText;
	}
}
