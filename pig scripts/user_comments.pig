--Users who posted maximum nember of comments.

commentsdata = LOAD 's3://tagsproject1/comments.csv' USING PigStorage(','); 
select1 = FOREACH commentsdata GENERATE REPLACE($0, '\\"', '') as (Id:int), REPLACE($5, '\\"', '') as (UserTagName:chararray), REPLACE($6, '\\"', '') as (UserId:int);
group1 = GROUP select1 by UserId;
count1 = FOREACH group1 GENERATE UserTagName AS User, couunt(ID) AS Total_Comments;
result = LIMIT count1 30;
STORE result1 into 's3://tagsproject1/output/3s';

