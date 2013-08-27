REGISTER ./tutorial.jar;
/*****************************************************************************************
The records are read from the file excite-small.log
The file is first cleaned in two steps:
STEP 1 : Remove records with null query
STEP 2 : Convert query to lower case to keep the case consistent and ensure that matching occurs properly later on

FINAL clean relation query_records_c2 is used for all parts
*****************************************************************************************
CLEANUP PROCESS
****************************************************************************************/

-- Loading file excite-small.log

query_records = LOAD 'excite-small.log' USING PigStorage('\t') AS (user,time,query);

-- Removing records that have null query

query_records_c1 = FILTER query_records BY org.apache.pig.tutorial.NonURLDetector(query); 

-- Converting query to lower case to keep the case consistent and ensure that matching occurs properly later on

query_records_c2 = FOREACH query_records_c1 GENERATE user, time, org.apache.pig.tutorial.ToLower(query) as query;

-- We know have a clean relation in query_records_c2  I now use this relation in all later -- steps  of part 2

/****************************************************************************************
 Part 4a - Queries with references to US zip codes
***************************************************************************************/

-- Applying MATCH to match phrases that have 5 continous digits. The digits should be independent and not clubbed with any word.

zip = FILTER query_records_c2 BY query MATCHES '.*(\\b\\d{5}\\b).*';

-- Store results in file

STORE zip INTO './4a';


/****************************************************************************************
 Part 4b - Find queries with references to place names
/****************************************************************************************/


/*
Assumptions are:
1) Only places names unto two words long will match. This is because ngram function has been used to separate out words from the query and by default this ngram is a 2-grams
e.g. if the query is 'I am in New York'
creating ngrams of this creates
(I)(am)(in)(new)(york)(i am)(am in)(in new)(new york)

Here the place names will be captured as
a) New York
b) York (this is a place name defined in the master list)

2) There might be ambiguous place name .e.g. the master file show 'Of' as a name of a place hence wherever in the query 'of' is used it will appear as a place name
*/


-- Loading the master file that contains all place name

raw = LOAD 'dataen.txt' USING PigStorage('\t');

-- Clean the relation obtained above by removing blank rows

clean1 = FILTER  raw BY org.apache.pig.tutorial.NonURLDetector($1); -- Change the place names to lower case to ease the mapping/matching later on
clean2 = FOREACH raw GENERATE org.apache.pig.tutorial.ToLower($1) AS placename;

-- in the clean relation of our original excite-small.log file , create n-grams go the query. This has been done to ensure that single word and two word place names are found
 
ngramed1 = FOREACH query_records_c2 GENERATE user, query, flatten(org.apache.pig.tutorial.NGramGenerator(query)) as ngram;

-- Join the above relation with the relation formed from the master file of place name , using the place name column

query_with_places = JOIN clean2 BY placename , ngramed1 BY ngram;  

-- Fetch user, query and the place name in the query and put it in relation result1

result1 = FOREACH query_with_places GENERATE user,query,placename;

-- Finding distinct results

result = DISTINCT result1;

-- Store result in a file

STORE result INTO './4b';
