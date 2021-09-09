-- Instructions to run the scripts

python3 invidx_cons.py [COLLPATH] [INDEXFILE] [STOPWORDFILE] [COMPRESSION_SCHEME] [XML_TAGS_INFO]

This will generate [INDEXFILE].idx and [INDEXFILE].dict in the present working directory.

python3 boolsearch.p [QUERYFILE] [RESULTFILE] [INDEXFILE] [DICTFILE]

This will generate the [RESULTFILE].txt in the present working directory.