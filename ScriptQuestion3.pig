REGISTER ./tutorial.jar;
REGISTER ./SessionAnalysis.jar
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
 Part 3a - Foreach user how many query sessions does the user have.
***************************************************************************************/

-- Foreach record in the clean relation query_records_c2 we fetch the user and the time in seconds. The time in the input files given in the format of
-- YYMMDDHHMMSS , I extract HHMMSS and convert it into seconds and store it as time

timestamped = FOREACH query_records_c2 GENERATE user, (int)SUBSTRING(time,6,8)*3600+(int)SUBSTRING(time,8,10)*60+(int)SUBSTRING(time,10,12) AS time;

-- Group the relation by users, to get timestamps grouped each user

grpd = GROUP timestamped BY user;  

-- Order timestamps for each user based on the time. e.g. if the relation has (A0000000000,{(A0000000000,20),(A0000000000,14),(A0000000000,05),(A0000000000,560)}) AS timestamps
-- the query below arranges timestamp in descending order (A0000000000,{((A0000000000,05),(A0000000000,14(,(A0000000000,20),(A0000000000,56))})

ordersessions = FOREACH grpd { F = ORDER timestamped BY time DESC; GENERATE group,F;}

-- Passing the timestamps to the udf which counts the sessions , the assumption taken is that a session expires after "10 minutes" of inactivity
-- note here $1 represents the data bag containing tuples as shown above 
-- < {((A0000000000,05),(A0000000000,14(,(A0000000000,20),(A0000000000,56))} > is the data bag for user A0000000000

count = FOREACH ordersessions GENERATE group, SessionAnalysis.SessionCount($1) as sessioncount;

-- Store results in file

STORE count INTO './3a';


/****************************************************************************************
 Part 3b - Maximum query Session Length
/****************************************************************************************/

/*
Assumption:
Max session length is only defined in those cases where the user has made at least 2 queries within a gap of 10 minutes (which we have assumed to be the time interval of 1 session). For eg., if a user makes 5 queries but all the queries are more than 10 minutes apart pair-wise, then the max session length for this user is still 0. However, if a user makes only 2 queries but they are less than 10 minutes apart then their time difference is the max session length.
*/

-- We already have a relation containing timestamped based on the group user 
-- (A0000000000,{(20,14,05,56)})  , this is stored in ordersessions as described above
-- here we pass $1 which is the bag of timestamps as describes above and get the maximum session length for each user
-- session length is fetched in minutes by dividing the max session length by 60
max = FOREACH ordersessions GENERATE group, SessionAnalysis.MaxSessionLength($1)/60 AS sessionlength; 

-- Order the relation above based on the session lengths

ordmax = ORDER max BY sessionlength DESC;

-- Pick the top most row to get maximum session length

maxsession = LIMIT ordmax 1;

-- Store results in file

STORE maxsession INTO './3b';



