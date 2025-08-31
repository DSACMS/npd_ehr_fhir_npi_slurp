Migrate To Seperate Cache Repo
===========================

The current design of this repo is that there is a cache of calulated csv files and json files that represent intermediate steps in a data processing
pipeline. These are valuable to store in a git repo, but using git as a text database for the history of slightly shifting json
files is a good approach to being able to browse this history of the changing data, it makes pull requests and code review unmanagable.

The solution? One repo has all of the code, and a seperate repo exists to be the text database: ../npd_ehr_scrape_cache/

However, the current series of steps in this process, do not uniformly use arguments to determine where they output their data. 
Specifically, Step40 does not take an arguement about which directory to process, and needs to be modified to do this. 

Also, there is a series of csv files, one being the output of one program and the input to another. 
The file names here need to be changed to include the step that generated them. So enriched_endpoints.csv needs to become step60_enriched_endpoints.csv etc.
This will make it much easier to follow what is happening. 

Step89 and Step90 should accept arguments for what their data sources are, and what the file names for the markdown reports they make. 

Please ask any clarifying questions about how to make these changes
