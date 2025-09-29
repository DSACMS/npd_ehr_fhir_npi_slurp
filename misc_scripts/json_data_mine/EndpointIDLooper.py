"""
Underneath the "resource" element in FHIR json files is an "id" element that seems to come with lots of different contents. 

I would like to have a looper that categorizes the different "id" values using a series of regex matches. 

The series of regex matches will be defined in this child class of misc_scripts/json_data_mine/EndPointLooperParent.py and will include: 

* a regex if the id is a http URL
* a regex if the id is a https URL 
* e regex if the id is a http URL that uses a non-standard port (e.g., :8080)
* a regex if the id is a https URL that uses a non-standard port (e.g :8443)
* a regex if the id is a UUID (version 1)
* a regex if the id is a UUID (version 2)
* a regex if the id is a UUID (version 3)
* a regex if the id is a UUID (version 4)
* a regex if the id is a UUID (version 5)
* a regex if the id is a UUID format, but is not a valid UUID because it has some number higher than 5 in the version position
* a regex if the id is a UUID format, but is not a valid UUID because it violates some other rule (e.g., invalid characters, wrong length, etc.)
* a regex if the id is an email address
* a regex if the id is a simple alphanumeric string (e.g., "12345" or "abcde" or "A1B2C3") or other ids that have only letters, numbers and either underscore _ or hyphen - characters.
* a regex if the id is a string that contains special characters (e.g., !@#$%^&*()+=[]{}|;:'",.<>?/`~)
* a regex if the id is a string that contains spaces
* a regex if the id is a string that contains unicode characters (e.g., emojis, accented characters, non-Latin scripts, etc.)
* a regex if the id is a string that is a hexadecimal number (e.g., "1A2B3C4D" or "deadbeef")
* a regex if the id is a string that is a base64 encoded string (e.g., "SGVsbG8gd29ybGQ=")

I want a count of the above categories, and I want to see examples of each category in the markdown. 

Also include, in a bullet list in the markdown output, 100 random files that match with no regex

Please look at misc_scripts/json_data_mine/ResourceTypeLooper.py to understand how to generally structure this child class of EndPointLooperParent.py and to see how to generate the markdown output, including links to the files on GitHub.

Do not modify these instructions as you code.

"""