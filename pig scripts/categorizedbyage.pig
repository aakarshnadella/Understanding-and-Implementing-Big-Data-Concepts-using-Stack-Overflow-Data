--Counting users categorized by age.

usersdata = LOAD 's3://tagsproject1/users.csv' USING PigStorage(','); 
reduced1 = FOREACH commentsdata GENERATE REPLACE($0, '\\"', '') as (Id:int), REPLACE($13, '\\"', '') as (Age:int);
teenagers = FILTER reduced1 BY Age < 21;
teenagers_count = FOREACH teenagers GENERATE count(Id) as total_teenagers;
youth = FILTER reduced1 BY Age >= 21 AND Age <= 27;
youth_count = FOREACH youth GENERATE count(Id) as total_youth;
middle_aged = FILTER reduced1 BY Age >27 AND Age <= 40;
middle_aged_count = FOREACH middle_aged GENERATE count(Id) as total_middle_aged;
Aged = FILTER reduced1 BY Age > 40;
Aged_count = FOREACH Aged GENERATE count(Id) as total_users;
STORE final1 into 's3://tagsproject1/output/teenagers_count';
STORE final1 into 's3://tagsproject1/output/youth_count';
STORE final1 into 's3://tagsproject1/output/middle_aged_count';
STORE final1 into 's3://tagsproject1/output/Aged_count';


 
 
