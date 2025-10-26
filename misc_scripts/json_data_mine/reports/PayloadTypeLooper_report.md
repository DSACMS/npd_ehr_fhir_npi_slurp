# Payload Type Analysis Summary

## What This Analysis Does
This analysis examines payloadType fields in FHIR JSON files, including their coding 
subkeys and address classification. It analyzes system/code values under coding subkeys, 
classifies address subkey contents using regex patterns, and tracks header subkey presence.

- **Coding Analysis:** System and code statistics from coding subkey
- **Address Classification:** Uses regex classification on address subkey contents
- **Header Detection:** Tracks presence of header subkey
- **Categories:** Has payloadType, coding, address, header presence
- **Examples:** Longest/shortest/random by filename length

## Processing Results
**Files Processed:** 28
**Files Failed:** 0

## Payload Type Presence

| Category | Count | Longest Example | Shortest Example | Random Example |
|----------|-------|-----------------|------------------|----------------|
| Has Payload Type | 16 | [entry_Endpoint_cb8483a1-d182-48bd-b480-a575e49a2863.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/tenzing_medical_llc_97c2c7bbb0aaacdcad3d54cdc6b4234c/endpoint/entry_Endpoint_cb8483a1-d182-48bd-b480-a575e49a2863.json) | [entry_Endpoint_2.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/pcis_gold_36399432dd7c7ed79eaf66a939c1dce7/endpoint/entry_Endpoint_2.json) | [entry_Endpoint_cb8483a1-d182-48bd-b480-a575e49a2863.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/tenzing_medical_llc_97c2c7bbb0aaacdcad3d54cdc6b4234c/endpoint/entry_Endpoint_cb8483a1-d182-48bd-b480-a575e49a2863.json) |
| No Payload Type | 12 | [entry_Organization_34c0cdef-5e71-459e-bf29-5f15faa2ff94.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/tenzing_medical_llc_97c2c7bbb0aaacdcad3d54cdc6b4234c/organization/entry_Organization_34c0cdef-5e71-459e-bf29-5f15faa2ff94.json) | [entry_Organization_3836.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/mdland_895e255ebcfd4ad4c250fa3440774d6d/organization/entry_Organization_3836.json) | [entry_Organization_no_id_267.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/nextech_f46efa02c6cbb395f24e1dde45a736b8/organization/entry_Organization_no_id_267.json) |
| Has Coding | 16 | N/A | N/A | N/A |

## Coding System Values

| System | Count |
|---------|-------|
| `http://ihe.net/fhir/ihe.formatcode.fhir/CodeSystem/formatcode` | 14 |
| `http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/EndpointPayloadTypeCS` | 2 |

## Coding Code Values

| Code | Count |
|------|-------|
| `urn:hl7-org:sdwg:ccda-structuredBody:1.1` | 12 |
| `NA` | 2 |
| `urn:ihe:pcc:xphr:2007` | 2 |
