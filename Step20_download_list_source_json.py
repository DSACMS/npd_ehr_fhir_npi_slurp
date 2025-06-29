#!/usr/bin/env python3
"""
This file accepts the csv file output by Step10_extract_list_source_from_lantern_csv.py
And then downloads all of the source list files (which are usually json files) and puts them into 
./data/source_list_json/

The structure of the input file (which is listed in the first line of the CSV) is: 

list_source,certified_api_developer_name,distinct_url_count

This script should create a good filename for each download file, by creating a 'safe_file_name_string' from the certified_api_developer_name column. 
To do this replace all special characters with spaces. Then convert all groups of spaces into underscores. and then convert all of the letters to lower-case.

Then rename the downloaded json file to ./data/source_list_json/new_safe_ehr_vendor_name.json 

Do this for every list_source in the --input_file
"""

