
## PII Redaction with Microsoft Presidio

StormCrawler provides a bolt (`PiiBolt`) that can redact personally‑identifiable information (PII) from page content before it reaches the indexing or persistence stages.  
The actual redaction work is delegated to a **PresidioRedacter** implementation that talks to Microsoft Presidio’s *analyzer* and *anonymizer* services.

### How it works
1. `PiiBolt` reads the page text (field **text**) and optional language metadata.  
2. It creates an instance of the class configured via `pii.redacter.class` (default is `org.apache.stormcrawler.pii.PresidioRedacter`).  
3. The redacter:
   * Sends the text to the **Presidio Analyzer** (`presidio.analyzer.endpoint`) – optionally with a language code.  
   * Sends the analyzer results to the **Presidio Anonymizer** (`presidio.anonymizer.endpoint`).  
   * Returns the anonymised text, which `PiiBolt` stores in a dummy HTML payload (`REDACTED_BYTES`) while preserving the original metadata.

### Required configuration

| Property | Description | Example |
|----------|-------------|---------|
| `pii.redacter.class` | Fully‑qualified class name of the PII redacter. Must implement `PiiRedacter`. | `org.apache.stormcrawler.pii.PresidioRedacter` |
| `pii.detect.language` | Set to `true` if you want `PiiBolt` to look for a language field in the metadata. | `true` |
| `pii.language.field` | Name of the metadata field that contains the language code (e.g. `parse.lang`). | `parse.lang` |
| `pii.removal.enable` | Enable/disable the whole redaction step. | `true` |
| `presidio.analyzer.endpoint` | URL of the Presidio Analyzer service. | `https://my-presidio.example.com/analyze` |
| `presidio.anonymizer.endpoint` | URL of the Presidio Anonymizer service. | `https://my-presidio.example.com/anonymize` |
| `presidio.analyzer.entities` *(optional)* | Comma‑separated list of entity types to request from the analyzer. | `PERSON,EMAIL,PHONE_NUMBER` |
| `presidio.supported.languages` *(optional)* | Comma‑separated list of ISO language codes supported by your Presidio deployment. If a language is not listed, the redacter falls back to the multi‑lingual model (`xx`). | `en,fr,de,es` |

### Minimal topology example

```java
// add the bolt to your topology
builder.setBolt("piiRedactor", new PiiBolt())
       .shuffleGrouping("fetcher");

// make sure the required config entries are present
Map<String, Object> conf = new HashMap<>();
conf.put(PiiBolt.PII_REDACTER_CLASS_PARAM, "org.apache.stormcrawler.pii.PresidioRedacter");
conf.put(PiiBolt.PII_ENABLE_FIELD, true);
conf.put("presidio.analyzer.endpoint", "https://presidio.mycorp.com/analyze");
conf.put("presidio.anonymizer.endpoint", "https://presidio.mycorp.com/anonymize");
// optional: language field
conf.put("pii.language.field", "parse.lang");
