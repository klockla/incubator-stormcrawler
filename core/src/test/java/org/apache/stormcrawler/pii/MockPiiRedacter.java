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
 * Mock PII Redacter implementation for testing purposes.
 * This class simulates redaction by replacing occurrences of the word"secret"
 * with "*****".
 */

public class MockPiiRedacter implements PiiRedacter {

    @Override public void init(Map<String, Object> conf) {}

    @Override public String redact(String content) {
        return redact(content, null);
    }

    @Override public String redact(String content, String language) {
        // simple redaction logic for the test
        return content.replaceAll("secret", "*****");
    }
}