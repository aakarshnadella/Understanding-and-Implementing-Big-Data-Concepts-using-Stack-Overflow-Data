--Most used tags

tags = LOAD 's3://tagsproject/tags-1.csv' USING PigStorage(',') AS (Id :int, TagName : chararray,	Count:int,	ExcerptPostId: int,	WikiPostId:int);
generate1 = FOREACH tags GENERATE REPLACE (TagName,'\\"', '') AS TagName,Id,Count;
order1 = ORDER generate1 BY Count DESC;
results = LIMIT order1 50;
DUMP results; 


