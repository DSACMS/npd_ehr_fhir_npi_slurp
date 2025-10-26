# Meta Tag Analysis Summary

## What This Analysis Does
This analysis examines the "meta" tag structure in FHIR JSON files. It tracks the 
presence of meta tags and their expected subfields (versionId, lastUpdated, source) 
while also identifying any unknown keys that appear beyond the expected ones.

- **Expected Fields:** versionId, lastUpdated, source
- **Categories:** Has meta, no meta, individual subfield presence
- **Special Feature:** Reports unknown meta keys beyond expected ones
- **Examples:** Longest/shortest/random by filename length

## Processing Results
**Files Processed:** 22
**Files Failed:** 0
**Total Meta Categories:** 4

## Meta Tag Distribution

| Meta Category | Count | Longest Example | Shortest Example | Random Example |
|---------------|-------|-----------------|------------------|----------------|
| No Meta Tag | 21 | [entry_Organization_7e36faab-1280-5bb8-b82e-db83b8fdb34d.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/zoobook_systems_llc_fae725c74837e2bd855d0b2710740bbc/organization/entry_Organization_7e36faab-1280-5bb8-b82e-db83b8fdb34d.json) | [entry_Endpoint_endpoint-ac-1672.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/caretracker_inc_fb608e9a6189e731e15d57f8357e46b7/endpoint/entry_Endpoint_endpoint-ac-1672.json) | [entry_Endpoint_id.gWvnv5E15R4Ru7GLX63sA.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/drchrono_inc_c0aaacb243932a2f36f36a7152b14262/endpoint/entry_Endpoint_id.gWvnv5E15R4Ru7GLX63sA.json) |
| Has lastUpdated | 1 | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) |
| Has Meta Tag | 1 | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) |
| Has versionId | 1 | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/webedoctor_inc_7de0125b8b367c0084351e846798cc6d/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) |

**Total Files Analyzed:** 22

## Unknown Meta Keys Found

The following meta keys were found beyond versionId, lastUpdated, and source:

* `profile`
