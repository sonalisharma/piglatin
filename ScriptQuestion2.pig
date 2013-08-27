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
 Part 2a - Counting the total number of query records 
***************************************************************************************/

-- Grouping the relation by ALL to get all the records in a single line to facilitate counting

query_records_grp = GROUP query_records_c2 ALL;

-- Counting the records in the row using COUNT function , in this case there will be only one row containing all records

total_query_records = FOREACH query_records_grp GENERATE COUNT(query_records_c2) as total;

-- Store the result in a file using STORE

STORE total_query_records INTO './2a';



/****************************************************************************************
 Part 2b - Maximum query length in words
/****************************************************************************************/

-- split the query into tokens using TOKENIZE function. This function separates out the
-- query into tuples with each tuple containing a word.
-- e.g. if query is "this is good" TOKENIZE converts it into ((this),(is),(good))

query_words =FOREACH query_records_c2 GENERATE user,time,query,TOKENIZE(query) as tokens; 

-- Use count function to count the number of tuple , in the example above this returns 
-- ((this),(is),(good)) , 3
query_num_words = FOREACH query_words GENERATE user, time, query, tokens AS tokens , COUNT($3) as numwords;

-- Order the relation based on the number of words in the query in descending order

order_query_num_words = ORDER query_num_words BY numwords DESC;
result = FOREACH order_query_num_words GENERATE query,numwords;

-- The first row of the order relation gives the maximum length query along with the number of words it has
max_query_length = LIMIT result 1;

-- Storing result in a file
STORE max_query_length INTO './2b';/****************************************************************************************
 Part 2c - Average length of the query in words
***************************************************************************************/

/*
In the previous part we already had the query and its length in words , here we use that relation and apply AVG function to it to get the average length in words
e.g.
in the above part we had relation in the form:
AOOOOOO455677,120304154523, {((this),(is),(good)), 3}
AOOOOOO455677,120304154523, {((this),(is),(not),(good)), 4}
where 3 ,4 refer to the number of words in the query.
in the stmt below we group this relation by ALL , to get all records in a single row 
*/

query_grp = GROUP query_num_words ALL;

-- Apply AVG on the number of words to get the average 
avg_query_length = FOREACH query_grp GENERATE AVG(query_num_words.numwords);

-- Storing result in a file

STORE avg_query_length INTO './2c';

/****************************************************************************************
 Part 2d -Find the total distinct users
****************************************************************************************/

-- Fetch the total users from the clean relation

distinct_users = FOREACH query_records_c2 GENERATE user;

-- Apply DISTINCT to remove duplicate users
distinct_users2 = DISTINCT distinct_users;

-- GROUP the relation by ALL to get all users in a singe row
distinct_users_grp = GROUP distinct_users2 ALL;

-- Apply COUNT to count all the users
num_dictinct_user = FOREACH distinct_users_grp GENERATE COUNT($1);

-- Storing result in a file
STORE num_dictinct_user INTO './2d';/****************************************************************************************
 Part 2e -Average query records per user
****************************************************************************************/

/*
In part b we had already obtained a relation containing users , query , number of words in  the query , this relation was called query_num_words , here we use the same relation and group it by user
*/

query_record_byuser = GROUP query_num_words BY  user;

-- COUNT the records for each row grouped by user
cnt_query_record_byuse = FOREACH query_record_byuser GENERATE group, COUNT($1) as num; 

-- From the above stmt we have the number of query records per user, we group them by ALL to get all records in one row
cnt_query_record_byuse_grp = GROUP cnt_query_record_byuse  ALL; 

-- Apply AVG function to get the average
avg = FOREACH cnt_query_record_byuse_grp GENERATE AVG(cnt_query_record_byuse.num);

-- Storing result in a file

STORE avg INTO './2e';

/***************************************************************************************
 Part 2f -Percentage of the query containing boolean operators like and or not
***************************************************************************************/

-- Using REGEX_EXTRACT_ALL get all records which have and ,or , not as words in the query,
-- the assumption is these words exists as separate words one or more times in a query

match = FOREACH query_records_c2 GENERATE query, REGEX_EXTRACT_ALL(query,'(.*(\\band\\b|\\bor\\b|\\bnot\\b).*)') as match_query; 

-- First find out the total number of queries,this will be the denominator to calculate percentage.
-- To find the  total number of query in the relation we do the following
-- Grouping the relation by ALL to get all the records in a single line to facilitate counting

query_records_grp = GROUP query_records_c2 ALL;

-- Counting the records in the row using COUNT function , in this case there will be only one row containing all records

total_query_records = FOREACH query_records_grp GENERATE COUNT(query_records_c2) as total;


/*
Here we do the following in a single stmt:
1) In the above relation 'match', if the match is not found then a null is returned as match_query.
2) We filter out such rows from the relation 'match'
3) Group the result by ALL , to get all matching queries in a single row
4) Count the number of queries and divide them by total queries found above 
*/
tmp = FOREACH (GROUP(FILTER match BY match_query is not NULL) ALL) GENERATE ((double)COUNT($1)/(double)total_query_records.total)*100 as percentage ;

-- Storing result in a file
STORE tmp INTO './2f';/***************************************************************************************
 Part 2g -10 longest distinct queries in words
***************************************************************************************/

/*
In part b we had already obtained an ordered relation containing users , query , number of words in  the query , this relation was called order_query_num_words , here we use the same relation and extract query and the number of words in the query.
*/

distinct_queries = FOREACH order_query_num_words GENERATE query,numwords;  

-- In case there are duplicates, those are removeddistinct_query_c1 = DISTINCT distinct_queries;

-- Order the queries by number of words againorder_distinct_query_c1 = ORDER distinct_query_c1 BY numwords DESC;

-- Pick the top ten queries using LIMITtopten_distinct_query_c1 = LIMIT order_distinct_query_c1 10;-- Storing result in a fileSTORE topten_distinct_query_c1 INTO './2g';/***************************************************************************************
 Part 2h - 10 most frequently occurring queries
***************************************************************************************/

/* 
Here most frequently occurring phrases have been calculated by using grams.
e.g. if the query is "this is good" then grams have been first calculated
as:
(this)
(is)
(good)
(this is)
(is good)
and then the most frequently occurring phrases have been found
*/

-- Finding n gramsngramed1 = FOREACH query_records_c2  GENERATE query, flatten(org.apache.pig.tutorial.NGramGenerator(query)) as ngram;

-- Take DISTINCT ngramsngramed2 = DISTINCT ngramed1;

-- Group relation by gramsfrequency1 = GROUP ngramed2 BY ngram;-- Counting the number of ngrams in the relationfrequency2 = FOREACH frequency1 GENERATE $0 as ngram, COUNT($1) as freq;-- The relation containing the number of norams ordered in descending orderorder_frequency2 = ORDER frequency2 BY freq DESC;-- Top 10 results takenlimit_frequency2 = LIMIT order_frequency2 10;

-- Results stored in another relationresult_frequency = FOREACH limit_frequency2 GENERATE ngram , freq;

-- Storing result in a fileSTORE result_frequency INTO './2h';
