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

def eachLineComments(line):
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames=['Id','PostId', 'Score','Text','CreationDate','UserDisplayName','UserId'])
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
		
inputComments = sc.textFile('/data2/AnalysisMY/Comments.xml')

headerComments = ['Id','PostId', 'Score','Text','CreationDate','UserDisplayName','UserId']

commentsCSVrdd = inputComments.map(lambda x:xml_to_csv_convertion(x,headerComments))
delete_rows = commentsCSVrdd.take(2)
output_data = commentsCSVrdd.filter(lambda x: x not in delete_rows)

output_data=output_data.map(eachLineComments)
print output_data.take(10)

inputCSV = output_data.toDF()
inputCSV.createOrReplaceTempView("commentsData")

#####	Posts data		######
# def eachLinePosts(line):
	# input = StringIO.StringIO(line)
	# reader = csv.DictReader(input, fieldnames=['Id','PostTypeId', 'AcceptedAnswerId','ParentId','CreationDate','DeletionDate','Score',\
	# 'ViewCount','Body','OwnerUserId','OwnerDisplayName','LastEditorUserId','LastEditorDisplayName','LastEditDate','LastActivityDate',\
	# 'Title','Tags','AnswerCount','CommentCount','FavoriteCount','ClosedDate','CommunityOwnedDate'])
	# return reader.next()

# readPostsCSV = sc.textFile("/data2/AnalysisMY/QwithHighComments.csv").map(eachLinePosts)
# readPostsCSV=spark.createDataFrame(readPostsCSV)
# readPostsCSV.createOrReplaceTempView("postsData1")

inputCSV = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/data2/AnalysisMY/QwithHighComments.csv')
inputCSV.createOrReplaceTempView("postsData1")
# getCountP=spark.sql("select count(*) from postsData1")
# getCountP.show()

####		Querying		####
getCount=spark.sql("select count(*) from commentsData")
getCount.show()


#	Posts with highest no of comments
highestNoOfComm=spark.sql("select PostId,count(*) as No_Of_Comments from commentsData group by PostId order by No_Of_Comments desc limit 30")
highestNoOfComm.show(30,False)

#	For a post(question), which user’s comment has high score
highScore=spark.sql("select UserId,PostId,Title,Text,commentsData.Score from commentsData,postsData1 where PostTypeId=1 and postsData1.Id=commentsData.PostId \
group by UserId,PostId,Title,Text,commentsData.Score order by commentsData.Score desc limit 20")
highScore.show(20)









