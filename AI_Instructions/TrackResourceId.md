Track Source Resource ID
=================

Each json file that is processed by Step40_extract_csv_data.py has a "id" field under "resource".

Also the file is named for this identifier. I would like to have two columns exported in every relevant output file alongside the EHR vendor.

I want the id of the resource added as a resource_id column. This comes from the id of the resource entry so json_obj.resource.id in object speak.
And I want the relative file location of the json file that was processed included as url with the prefix of

https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/

This would make the file that is currently stored in ../npd_ehr_scrape_cache/cehrt_fhir_json/zoobook_systems_llc/entry_7e36faab-1280-5bb8-b82e-db83b8fdb34d.json

By the time they reach the sub-directory level, the json files like this have one and only one resource listed. You can consider every json file processed by Step40 as having a single resource_id. 

Specifically: 

../npd_ehr_scrape_cache/ -> https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/

Appear as:

https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/zoobook_systems_llc/entry_7e36faab-1280-5bb8-b82e-db83b8fdb34d.json

Which will allow for manual confirmation that the ETL is working by reviewing the cache of the scrape online!

Add this entry to every CSV output in step 40 and step 50 that is not just a linking table.
