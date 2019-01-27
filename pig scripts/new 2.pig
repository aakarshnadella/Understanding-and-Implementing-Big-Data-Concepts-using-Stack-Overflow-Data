-- Most Used Tags last 3 years.

loadTagm = LOAD 's3://tagsproject1/posttag.csv' USING PigStorage(','); 
posttagm = FOREACH loadTagm GENERATE REPLACE($0, '\\"', '') as (PostId:int), REPLACE($1, '\\"', '') as (TagId:int);
tags1m = LOAD 's3://tagsproject1/tags-1.csv' USING PigStorage(',');
tagsm = FOREACH tags1m GENERATE REPLACE($0, '\\"', '') as (Id :int), REPLACE($1, '\\"', '') as (TagName :chararray);
tags = LOAD 's3://tagsproject1/tags-1.csv' USING PigStorage(',') AS (Id :int, TagName : chararray,	Count:int,	ExcerptPostId: int,	WikiPostId:int);
posthistory1m = LOAD 's3://tagsproject1/post history.csv' USING PigStorage(',');
posthistorym = FOREACH posthistory1m GENERATE REPLACE($1, '\\"', '') as (PostId :int), REPLACE($3, '\\"', '') as (CreationDate :chararray);
join1m = JOIN tagsm BY Id, posttagm BY TagId;
join2m = JOIN join1m BY posttagm::PostId,  posthistorym BY PostId;
group_by_tagm = GROUP join2m BY join1m::tagsm::Id;
count_by_tag = FOREACH group_by_tagm GENERATE group, COUNT(join2m.posthistorym::CreationDate) AS c1;
order_count_by_tag = ORDER count_by_tag BY c1 DESC;
result = JOIN order_count_by_tag BY group, tags BY Id;
final = FOREACH result GENERATE order_count_by_tag::group, tags::TagName, order_count_by_tag::c1 AS c2;
final1 = ORDER final BY c2 DESC;
STORE final1 into 's3://tagsproject1/output/2s';
