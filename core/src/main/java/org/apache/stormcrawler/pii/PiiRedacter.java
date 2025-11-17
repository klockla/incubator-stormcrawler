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

import java.util.Map;

/**
 * An interface for bolts implementing PII redaction
 */
public interface PiiRedacter {
	void init(Map<String, Object> topologyConf) throws Exception;
	
	/**
	 * Redacts PII from the input string using default language settings
	 * (e.g. no language or a default language configured at initialization)
	 * 
	 * @param input the input string possibly containing PII
	 * @return the input string with PII redacted
	 */
	String redact(String input);
	
	/**
	 * Redacts PII from the input string using the specified language
	 * @param input	 the input string possibly containing PII
	 * @param language the language to use for PII redaction
	 * @return
	 */
	String redact(String input, String language);
}
