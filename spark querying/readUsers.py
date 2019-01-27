from pyspark.sql.functions import unix_timestamp
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import csv
import StringIO
import sys
import pandas

reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession \
		.builder \
		.appName("xml to csv conversion") \
		.getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

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
		
######################### Users ##################################		
def loadRecordUsers(line):
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames=['Id','Reputation', 'CreationDate','DisplayName','LastAccessDate','WebsiteUrl','Location',\
	'AboutMe','Views','UpVotes','DownVotes','ProfileImageUrl','EmailHash','Age','AccountId'])
	return reader.next()


input_posts = sc.textFile('/data2/AnalysisMY/Users.xml')
header_users = ['Id','Reputation', 'CreationDate','DisplayName','LastAccessDate','WebsiteUrl','Location',\
	'AboutMe','Views','UpVotes','DownVotes','ProfileImageUrl','EmailHash','Age','AccountId']
	
users_csv_rdd = input_posts.map(lambda x:xml_to_csv_convertion(x,header_users))
delete_rows = users_csv_rdd.take(2)
output_data_users = users_csv_rdd.filter(lambda x: x not in delete_rows)

output_data_users = output_data_users.map(loadRecordUsers)
print output_data_users.take(10)

inputCSV_users = output_data_users.toDF()
inputCSV_users.createOrReplaceTempView("usersData")
sqlContext.cacheTable("usersData")
#inputCSV.persist()
######################### Users end #################################

######################### Badges ##################################	
def loadRecordBadges(line):
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames=['Id','UserId', 'Name','Date','Class','TagBased'])
	return reader.next()
	
input_badges = sc.textFile('/data2/AnalysisMY/Badges.xml')
header_badges = ['Id','UserId', 'Name','Date','Class','TagBased']
	
badges_csv_rdd = input_badges.map(lambda x: xml_to_csv_convertion(x,header_badges))
delete_rows = badges_csv_rdd.take(2)
output_data_badges = badges_csv_rdd.filter(lambda x: x not in delete_rows)

output_data_badges = output_data_badges.map(loadRecordBadges)
print output_data_badges.take(10)

inputCSV_badges = output_data_badges.toDF()
inputCSV_badges.createOrReplaceTempView("BadgesData")
sqlContext.cacheTable("BadgesData")
#inputCSV.persist()
######################### Badges end #################################


# getCount=spark.sql("select count(*) from usersData")
# getCount.show()


# 1 Users with top views
max_views_user = spark.sql(" select Id, DisplayName,int(views) as No_of_views from usersData Order by No_of_views DESC ")
max_views_user.show()
# pandas_df = max_views_user.toPandas()
# pandas_df.to_csv("max_views_user.csv", header=True, index=False, encoding='utf-8')

# 2 top reputed users
Top_reputation = spark.sql("select Id, DisplayName, Location ,Reputation from usersData order by Int(Reputation) Desc LIMIT 100")
Top_reputation.show(100,False)
# pandas_df = Top_reputation.toPandas()
# pandas_df.to_csv("Top_reputation.csv", header=True, index=False, encoding='utf-8')

# 3. Users with most positive profile 
positive_profiles = spark.sql("select Id, DisplayName, Reputation, UpVotes , DownVotes, (int(UpVotes) - int(DownVotes)) as Vote_diff from UsersData where int(upVotes) > 10000 order by Vote_diff Desc LIMIT 100")
positive_profiles.show(100,False)
# pandas_df = positive_profiles.toPandas()
# pandas_df.to_csv("positive_profiles.csv", header=True, index=False, encoding='utf-8')

# 4. Number of account created every year
users_per_year = spark.sql("select Year_data.CreateYear, count(*) No_of_accounts_created from (select id, year(creationDate) as CreateYear from UsersData) as year_data group by Year_data.CreateYear order by Year_data.CreateYear")
users_per_year.show(10)
# pandas_df = users_per_year.toPandas()
# pandas_df.to_csv("users_per_year.csv", header=True, index=False, encoding='utf-8')

# 5. Number of account that are inactive for the last one to nine years
inactive_users = spark.sql("Select * from (select 'one year inactivity' as User_Inactivity,count(*) count_inactive_usersData from UsersData where lastAccessDate < '2016-12-01' \
UNION \
select 'two year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2015-12-01' \
UNION \
select 'three year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2014-12-01' \
UNION \
select 'four year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2013-12-01' \
UNION \
select 'five year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2012-12-01' \
UNION \
select 'six year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2011-12-01' \
UNION \
select 'seven year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2010-12-01' \
UNION \
select 'eight year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2009-12-01' \
UNION \
select 'nine year inactivity' as User_Inactivity,count(*) count_inactive_users from UsersData where lastAccessDate < '2008-12-01') order by User_Inactivity ")
inactive_users.show(100,False)
# pandas_df = inactive_users.toPandas()
# pandas_df.to_csv("inactive_users.csv", header=True, index=False, encoding='utf-8')


# Top badges assigned
top_badges = spark.sql("select name, count(*) as badges_count from BadgesData  GROUP BY Name Order by badges_count Desc LIMIT 100")
top_badges.show(100,False)
# pandas_df = top_badges.toPandas()
# pandas_df.to_csv("top_badges.csv", header=True, index=False, encoding='utf-8')

#Top 50 users with each type of badges 
top_users_badges = spark.sql("select Result.id,Result.DisplayName, Sum(Gold) as No_of_gold,Sum(Silver) as No_of_silver ,Sum(Bronze) as No_of_bronze , Count(*) as Total_Number_of_medals from (select U.Id, U.DisplayName, Case WHEN B.Class = 1 then 1 else 0 end as Gold, Case WHEN B.Class = 2 then 1 else 0 end as Silver, Case WHEN B.Class = 3 then 1 else 0 end as Bronze FROM usersData U JOIN BadgesData B ON B.userId = U.Id) as Result group by Result.Id, Result.DisplayName Order by Total_Number_of_medals Desc Limit 50")
top_users_badges.show(50,False)
# pandas_df = top_users_badges.toPandas()
# pandas_df.to_csv("top_users_badges.csv", header=True, index=False, encoding='utf-8')

# top users with different types of badges
user_diff_badges = spark.sql("select UserId, count(distinct Name) as Count_of_different_badge_names From BadgesData group by userid order by Count_of_different_badge_names Desc limit 50")
user_diff_badges.show(50,False)
# pandas_df = user_diff_badges.toPandas()
# pandas_df.to_csv("user_diff_badges.csv", header=True, index=False, encoding='utf-8')
