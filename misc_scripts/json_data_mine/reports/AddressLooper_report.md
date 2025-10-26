# Address Component Analysis Summary

## What This Analysis Does
This analysis examines address entries in FHIR JSON files and dynamically discovers all 
address subcomponents present in the data. It tracks the presence of address fields and 
calculates percentages for each subcomponent found (such as line, city, state, postalCode, country, etc.).

- **Discovery Method:** Dynamic scanning of all address dictionary keys
- **Components Tracked:** All subfields found in address objects (not just predefined ones)
- **Percentage Calculations:** Based on files that have address fields
- **Examples:** Longest/shortest/random by filename length

## Processing Results
**Files Processed:** 31
**Files Failed:** 0

## Address Field Presence

| Category | Count | Percentage | Longest Example | Shortest Example | Random Example |
|----------|-------|------------|-----------------|------------------|----------------|
| Has Address | 8 | 25.8% | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/us_monitoring_inc_d75e6c341117b5972d941f3b71378082/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_H9NEO6LkO7QNfslW.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/the_echo_group_bf7be8370b8034dbc1b396c6f7811c91/organization/entry_Organization_H9NEO6LkO7QNfslW.json) | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/us_monitoring_inc_d75e6c341117b5972d941f3b71378082/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) |
| No Address | 23 | 74.2% | [entry_Endpoint_IndianHealthservice-FourDirectionsHubBulkFHIR.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/indian_health_service_bd021e6fcfa80410228633c73fbc9fbf/endpoint/entry_Endpoint_IndianHealthservice-FourDirectionsHubBulkFHIR.json) | [entry_Endpoint_FRA93850.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/eyemd_emr_healthcare_systems_inc_dbb2c800b0c9f414238366c576603535/endpoint/entry_Endpoint_FRA93850.json) | [entry_Endpoint_IndianHealthservice-FourDirectionsHubFHIR.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/indian_health_service_bd021e6fcfa80410228633c73fbc9fbf/endpoint/entry_Endpoint_IndianHealthservice-FourDirectionsHubFHIR.json) |

## Address Component Breakdown
*(Percentages are of files that have address fields)*

| Component | Count | Percentage |
|-----------|-------|------------|
| City | 8 | 100.0% |
| Country | 8 | 100.0% |
| Line | 8 | 100.0% |
| Postalcode | 8 | 100.0% |
| State | 8 | 100.0% |
