EHR FHIR Entity Slurp
=========

It does that.

So the general steps here are: 

* Download the FHIR endpoint data file from Lantern
* Extract all of the service links.. which the EHR vendors publish as Bulk FHIR json files that list all of the EHR endpoints, and also the ONPIs associated with them
* SPlit these huge services files into smaller files for further processing. 
* Loop over the direct EHR Endpoints to see if they are properly exposing the limited Endpoint and Organization endpoints that they should be. 
* Generate a dashboard that shows how each EHR vendor is doing on compliance with the HTI-2 requirements
* extract the data into several CSV linked files with a normalized data structure, including a file for Organizations, Addresses, Vendors, and Endpoints. 




