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
**Files Processed:** 362222
**Files Failed:** 0
**Total Meta Categories:** 5

## Meta Tag Distribution

| Meta Category | Count | Longest Example | Shortest Example | Random Example |
|---------------|-------|-----------------|------------------|----------------|
| No Meta Tag | 274733 | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/abeo_solutions_inc_87133ed24a4073af176beaf74cd27a1e/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Endpoint_test.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/adaptamed_llc_47d439f4130f7692caea161fa0b4d2bd/endpoint/entry_Endpoint_test.json) | [entry_Endpoint_test.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/adaptamed_llc_47d439f4130f7692caea161fa0b4d2bd/endpoint/entry_Endpoint_test.json) |
| Has Meta Tag | 87489 | [entry_Endpoint_142519.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_142519.json) | [entry_Endpoint_135243.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_135243.json) | [entry_Endpoint_142082.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_142082.json) |
| Has lastUpdated | 81663 | [entry_Endpoint_142519.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_142519.json) | [entry_Endpoint_135243.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_135243.json) | [entry_Endpoint_141374.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_431baf9cf4a84b1f9865df363dcd5e35/endpoint/entry_Endpoint_141374.json) |
| Has versionId | 1217 | [entry_Endpoint_143bc7c9-010a-4864-aedf-ecc31a53cff4.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_ab5783b7bda76708800d03e01f6405c3/endpoint/entry_Endpoint_143bc7c9-010a-4864-aedf-ecc31a53cff4.json) | [entry_Endpoint_6295a912-4865-4d03-a903-9763e02910c5.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_ab5783b7bda76708800d03e01f6405c3/endpoint/entry_Endpoint_6295a912-4865-4d03-a903-9763e02910c5.json) | [entry_Endpoint_4de199fb-c748-4478-9bc3-4f5c08a57143.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/azalea_health_ab5783b7bda76708800d03e01f6405c3/endpoint/entry_Endpoint_4de199fb-c748-4478-9bc3-4f5c08a57143.json) |
| Has source | 457 | [entry_Endpoint_1.100028.21.2.191.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/eyefinity_inc_7f49a3ffd9754854012a649ba6df4325/endpoint/entry_Endpoint_1.100028.21.2.191.json) | [entry_Endpoint_1.101114.21.2.28.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/eyefinity_inc_7f49a3ffd9754854012a649ba6df4325/endpoint/entry_Endpoint_1.101114.21.2.28.json) | [entry_Endpoint_1.101157.21.2.71.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/eyefinity_inc_7f49a3ffd9754854012a649ba6df4325/endpoint/entry_Endpoint_1.101157.21.2.71.json) |

**Total Files Analyzed:** 362222

## Unknown Meta Keys Found

The following meta keys were found beyond versionId, lastUpdated, and source:

* `profile`
* `tag`
