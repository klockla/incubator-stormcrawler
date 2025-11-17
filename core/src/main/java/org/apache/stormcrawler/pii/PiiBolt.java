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

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * StormCrawler bolt that performs PII redaction on the content of web pages
 * before they are passed to the indexing or persistence bolt.<br>
 * If enabled, the HTML content will be overwritten with a dummy HTML page (containing just "REDACTED")<br><br>
 * <b>pii.redacter.class</b> is the name of the class implementing the PiiInterface interface (e.g. org.apache.stormcrawler.pii.PresidioRedacter)<br>
 * <b>pii.language.field</b>, if set, allows to set the name of a Metadata field that contains the language to be passed to the PII redacter instance
 * 
 */
@SuppressWarnings("serial")
public class PiiBolt extends BaseRichBolt {

	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PiiBolt.class);

	/*
	 *  Name of config field defining the PII Redacter class
	 * (This class must implement the PiiRedacter interface
	 */
	public static final String PII_REDACTER_CLASS_PARAM = "pii.redacter.class";

	/*
	 * Name of the field for configurating language detection
	 */
	public static final String PII_DETECT_LANGUAGE_PARAM = "pii.detect.language";

	/*
	 * Name of the field for defining Metadata field containing language
	 */
	public static final String PII_LANGUAGE_FIELD = "pii.language.field";

	/*
	 * Name of the field for disabling PII removal
	 */
	public static final String PII_ENABLE_FIELD = "pii.removal.enable";

	private static final String FIELD_URL = "url";
	private static final String FIELD_CONTENT = "content";
	private static final String FIELD_METADATA = "metadata";
	private static final String FIELD_TEXT = "text";


	// Default value for language metadata field
	private String languageFieldName = "parse.lang";

	protected OutputCollector collector;

	protected PiiRedacter piiRedacter;

	private boolean piiEnabled = false;
	
	public static final String REDACTED_HTML = "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><title>REDACTED</title></head><body>REDACTED</body></html>";
	
	public static final byte[] REDACTED_BYTES = REDACTED_HTML.getBytes(StandardCharsets.UTF_8);

	/**
	 * Returns a Scheduler instance based on the configuration *
	 */
	public static PiiRedacter getInstance(Map<String, Object> stormConf) {
		PiiRedacter redacter;

		String className = ConfUtils.getString(stormConf, PII_REDACTER_CLASS_PARAM);
		if (className == null || className.isEmpty()) {
			throw new RuntimeException("PiiRedacter class name must be defined in the configuration (pii.redacter.class)");
		}

		LOG.info("Loading PII Redacter class, name={}", className);
		try {
			redacter = InitialisationUtil.initializeFromQualifiedName(className, PiiRedacter.class);
		} catch (Exception e) {
			throw new RuntimeException("Can't instantiate " + className, e);
		}

		LOG.info("Initializing PII Redacter instance");
		try {
			redacter.init(stormConf);
		} catch (Exception e) {
			LOG.error("Error while initializing PII Redacter", e);
		}

		return redacter;
	}

	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		// Uncomment if extending StatusEmitterBolt
		//super.prepare(topoConf, context, collector);

		this.collector = collector;

		this.piiRedacter = getInstance(topoConf);
		LOG.info("Initialized PiiRedacter instance");

		// Get language metadata field name
		String confLanguageField = ConfUtils.getString(topoConf, "pii.language.field");
		if (confLanguageField != null && !confLanguageField.isEmpty()) {
			languageFieldName = confLanguageField;
		}
		LOG.info("PII language field: {}", languageFieldName);

		piiEnabled = ConfUtils.getBoolean(topoConf, PII_ENABLE_FIELD, false);
		LOG.info("PII enabled: {}", piiEnabled);

	}

	@Override
	public void execute(Tuple input) {

		if (!piiEnabled) {
			this.collector.emit(input, input.getValues());
			this.collector.ack(input);
			return;
		}
		
		String url = input.getStringByField(FIELD_URL);
		LOG.info("Processing URL for PII redaction: {}", url);

		Metadata metadata = (Metadata) input.getValueByField(FIELD_METADATA);
		String text = input.getStringByField(FIELD_TEXT);
		byte[] originalBytes = input.getBinaryByField(FIELD_CONTENT);
		
		if (StringUtils.isBlank(text)) {
			LOG.info("No text to process for URL: {}", url);
			metadata.addValue("pii.processed", "false");
			// Force the binary content to a dummy content
			emitTuple(input, url, REDACTED_BYTES, metadata, "");
			this.collector.ack(input);
			return;
		}

		try {
			String language = metadata.getFirstValue(languageFieldName);
			String redacted = (language != null) ?
					piiRedacter.redact(text, language) :
					piiRedacter.redact(text);

			if (redacted == null) {
				throw new Exception("PII Redacter returned null");
			}

			metadata.addValue("pii.processed", "true");
			
			// Force the binary content to a dummy content
			emitTuple(input, url, REDACTED_BYTES, metadata, redacted);
		} catch (Exception e) {
			LOG.error("Error during PII redaction for URL {}: {}", url, e.getMessage());
			metadata.addValue("pii.error", e.getMessage());
			
			// How to handle the content in case of error ?
			emitTuple(input, url, originalBytes, metadata, text);
		}

		this.collector.ack(input);
	}

	private void emitTuple(Tuple input, String url, byte[] content, Metadata metadata, String text) {
		this.collector.emit(input, new Values(url, content, metadata, text));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_URL, FIELD_CONTENT, FIELD_METADATA, FIELD_TEXT));
	}
}
