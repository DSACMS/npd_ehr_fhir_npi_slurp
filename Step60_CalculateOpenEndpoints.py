"""
DO NOT ATTEMPT TO READ THE CSV files directly!! this currently crashes the cline API. 

Now that step50 is implemented we have a list of NPIs their associated urls. That CSV file structure looks like this

org_fhir_url,npi
https://oauth.ipatientcare.net/api/Organization/e167a65d-9b75-11ef-91a9-0a34c90c8dcf,1346280260
https://oauth.ipatientcare.net/api/Organization/e2cb19e7-9b75-11ef-91a9-0a34c90c8dcf,1245545789
https://oauth.ipatientcare.net/api/Organization/e1fd6353-9b75-11ef-91a9-0a34c90c8dcf,1437225067
https://provider.myhelo.com/fhir/Organization/74,1336252451
https://fhir.phemr.co:9443/fhir-server/api/v4/Organization/1938b437712-b3b975bd-8c23-4bc1-bdbb-78417b3df344,1407071210
https://fhir.phemr.co:9443/fhir-server/api/v4/Organization/1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c,1376083733
https://revolutionehr.dynamicfhir.com/fhir/r4/Organization/idzMNL0pFJ5pjpyAmQqHJw.g,1326492190
https://revolutionehr.dynamicfhir.com/fhir/r4/Organization/idTDBzm3uhgmhCI0kIaACSag,1821091943
https://fhir.novoclinical.com/fhir/DEFAULT/Organization/444593,2222222222
https://interface.relimedsolutions.com/fhir/r4/Organization/2460037878,2460037878
https://interface.relimedsolutions.com/fhir/r4/Organization/2460037878,1154719797

We know that there is a server listening because of the logic in Step50_simple_clean_output.py (though sometimes we get weird server headers)
Please ignore the weird server headers in an otherwise working server endpoint, similar to the way it is handled in Step50. 
Use similar timeouts to Step50

Now we need to calculate the always-available-well-known-and-open endpoints that should be available without authentication.
However, it is not clear what sub-directory on a given url will have these files. For 

https://example.com/which/dir/level/is/the/fhir/at/Organization/123abc

The well known urls could be prefixed with 

https://example.com/
or 
https://example.com/which/
or
https://example.com/which/dir/

You need to check every directory, starting at the domain name until you find a level that is responsive to your specific sub-url requests. 

We are looking for several specific types of urls: 

* Capability Statement - should be valid XML or JSON at /metadata
* Well-known Smart Config -  Oauth endpoints available at /.well-known/smart-configuration
* OpenAPI - at /api-docs
* OpenAPi json - at /openapi.json
* Swagger - at /swagger
* Swagger json - at /swagger.json

At each level of the URL directory structure, starting at the top, check to see if these exist. 


then create an enriched csv output with the following headers for each row of data in the origial file

org_fhir_url,npi,capability_url,smart_url,openapi_docs_url,openapi_json_url,swagger_url

In the event that it is not possible to find a responsive url for a given url type at an endpoint, in the place of a valid url this row data should say "Error - failed to find openapi_docs url" etc etc. 

There should be no defaults to the --input_csv_file and --output_csv_file arguments. 


"""