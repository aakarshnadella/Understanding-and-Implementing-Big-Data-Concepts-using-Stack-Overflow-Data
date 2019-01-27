from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import csv
import StringIO
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession \
		.builder \
		.appName("xml to csv conversion") \
		.getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)


def loadRecord(line):
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames=['Id','PostTypeId', 'AcceptedAnswerId','ParentId','CreationDate','DeletionDate','Score',\
	'ViewCount','Body','OwnerUserId','OwnerDisplayName','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate',\
	'Title','Tags','AnswerCount','CommentCount','FavoriteCount','ClosedDate','CommunityOwnedDate'])
	return reader.next()

def xml_to_csv_convertion(each_line,header):
	offset=0
	result_csv = ""
	for i in header:
		csv_convert = ""
		pattern_check = i + "="
		index = each_line.find(pattern_check,offset)
		if(index == -1):
			result_csv+=','
			continue
		index+=(len(i) + 2)
		csv_convert += '\"'
		while(each_line[index]!='\"'):
			csv_convert += each_line[index].encode('utf8')
			index += 1
		csv_convert += '\"'
		result_csv += csv_convert + ','
		offset = index
	return result_csv[:-1]
		
input_posts = sc.textFile('/data2/AnalysisMY/Posts.xml')
#input_posts = sc.textFile('/home/students/yashkuru/BigDataProject/Datadumps/Posts.xml')

header = ['Id','PostTypeId', 'AcceptedAnswerId','ParentId','CreationDate','DeletionDate','Score',\
	'ViewCount','Body','OwnerUserId','OwnerDisplayName','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate',\
	'Title','Tags','AnswerCount','CommentCount','FavoriteCount','ClosedDate','CommunityOwnedDate']

posts_csv_rdd = input_posts.map(lambda x:xml_to_csv_convertion(x,header))
delete_rows = posts_csv_rdd.take(2)
output_data = posts_csv_rdd.filter(lambda x: x not in delete_rows)
# change xml to csv
output_data=output_data.map(loadRecord)
print output_data.take(10)

#convert to spark data frame
inputCSV = output_data.toDF()
inputCSV.createOrReplaceTempView("postsData")


getCount=spark.sql("select count(*) from postsData")
getCount.show()

#Count of number of questions and answers
question_type_count = spark.sql("select 'Question' as Posttype,count(*) as No_of_Questions from postsData where PostTypeId = 1 UNION select 'Answer' as Posttype,count(*) as No_of_Questions from postsData where PostTypeId = 2")

#Count of no of unanswered questions
unanswered_questions = spark.sql("Select count(*) as No_of_unanswered_Questions from postsData where AnswerCount = 0 and PostTypeId = 1")
unanswered_questions.show()

#get top 30 tags
topTags=spark.sql("select tags,count(*) as TagsCount from postsData group by tags order by TagsCount desc limit 30")
topTags.show(30,False)

#get least 30 tags
topTags=spark.sql("select tags,count(*) as TagsCount from postsData group by tags order by TagsCount limit 30")
topTags.show(30,False)


#Count of closed questions
closed_Questions = spark.sql("select count(*) from postsData where ClosedDate != '' and PostTypeId = 1")

#Posts with no activity for more than 2 years
No_activity = spark.sql("select count(*) from postsData where LastActivityDate < '2014-12-01' ")
No_activity.show()

#Favourite count for unanswered questions tags
favorite_unansweredcount = spark.sql("select sum(PHP) as PHP, sum(PYTHON) as PYTHON, sum(ANDROID) as ANDROID, sum(JAVASCRIPT) as JAVASCRIPT,sum(IOS) as IOS,sum(JQUERY) as JQUERY, sum(HTML) as HTML, sum(JAVA) as JAVA,sum(CSS) as CSS,sum(ANGULARJS) as ANGULARJS, sum(AJAX) as AJAX , sum(DATABASES) as DATABASES , sum(SQL) as SQL, sum(JSON) as JSON, sum(LINUX) as LINUX from (select case when tags LIKE '%php%' then FavoriteCount ELSE 0 end as PHP, case when tags LIKE '%python%' then FavoriteCount ELSE 0 end as PYTHON, case when tags LIKE '%android%' then FavoriteCount ElSE 0 end as ANDROID, case when tags LIKE '%javascript%' then FavoriteCount ElSE 0 end as JAVASCRIPT, case when tags LIKE '%ios%' then FavoriteCount ElSE 0 end as IOS, case when tags LIKE '%jquery%' then FavoriteCount ElSE 0 end as JQUERY, case when tags LIKE '%html%' then FavoriteCount ElSE 0 end as HTML, case when tags LIKE '%java%' then FavoriteCount ElSE 0 end as JAVA, case when tags LIKE '%css%' then FavoriteCount ElSE 0 end as CSS, case when tags LIKE '%angularjs%' then FavoriteCount ElSE 0 end as ANGULARJS, case when tags LIKE '%ajax%' then FavoriteCount ElSE 0 end as AJAX, case when tags LIKE '%database%' then FavoriteCount ElSE 0 end as DATABASES, case when tags LIKE '%sql%' then FavoriteCount ElSE 0 end as SQL, case when tags LIKE '%json%' then FavoriteCount ElSE 0 end as JSON, case when tags LIKE '%linux%' then FavoriteCount ElSE 0 end as LINUX from postsData where answercount = 0) as result")

favorite_unansweredcount.show(100,False)

# Top 10 questions
top10Question = spark.sql("select Title, ViewCount, Score from postsData order by viewCount Desc LIMIT 10")

#output_data.saveAsTextFile('/home/students/yashkuru/BigDataProject/Datadumps/badgesOutput12')
