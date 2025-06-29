#!/bin/bash
python Step10_extract_list_source_from_lantern_csv.py --input_file prod_data/fhir_endpoints.csv --output_file prod_data/list_sources_summary.csv
python Step20_download_list_source_json.py --input_file ./prod_data/list_sources_summary.csv --output_dir ./data/service_json/
python Step30_parse_source_bundle.py --input_dir ./data/service_json/

