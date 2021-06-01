import tweepy
import json
from tweepy.parsers import JSONParser
#import boto3
import decimal
from dateutil.parser import parse
from pytz import timezone
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import time
import subprocess
from pushover import init, Client
import sys
import boto3

def unescape(s):
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    # this has to be last:
    s = s.replace("&amp;", "&")
    return s


# pull RDS details from SSM parameter store
ssm = boto3.client('ssm')
rdsHost = ssm.get_parameter(Name='/jtweets/rds-instance-host', WithDecryption=False).get("Parameter").get("Value")
rdsInstance = ssm.get_parameter(Name='/jtweets/rds-instance-name', WithDecryption=False).get("Parameter").get("Value")
rdsUsername = ssm.get_parameter(Name='/jtweets/rds-instance-user', WithDecryption=False).get("Parameter").get("Value")
rdsPassword = ssm.get_parameter(Name='/jtweets/rds-instance-pass', WithDecryption=True).get("Parameter").get("Value")
consumer_key = ssm.get_parameter(Name='/jtweets/tweepy/consumer_key', WithDecryption=True).get("Parameter").get("Value")
consumer_secret = ssm.get_parameter(Name='/jtweets/tweepy/consumer_secret', WithDecryption=True).get("Parameter").get("Value")
access_token = ssm.get_parameter(Name='/jtweets/tweepy/access_token', WithDecryption=True).get("Parameter").get("Value")
access_token_secret = ssm.get_parameter(Name='/jtweets/tweepy/access_token_secret', WithDecryption=True).get("Parameter").get("Value")
pushover_api_key = ssm.get_parameter(Name='/jtweets/pushover/api_key', WithDecryption=True).get("Parameter").get("Value")
pushover_app_id = ssm.get_parameter(Name='/jtweets/pushover/app_id', WithDecryption=False).get("Parameter").get("Value")

# set up return JSON
outJSON = { "artist": "", "song": "", "error": 0, "errorMessage": "", "tweetText": "" }

brisbaneTimezone = timezone('Australia/Brisbane')
dateFormat = '%Y-%m-%d %H:%M'

# login
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


# set up tweepy
#api = tweepy.API(auth, parser=JSONParser())
api = tweepy.API(auth)

# set up pushover
init(pushover_api_key)

# start up RDS instance
#rdsClient = boto3.client('rds')


import botocore.session
session = botocore.session.get_session()
rdsClient = session.create_client('rds')

try:
	response = rdsClient.start_db_instance(
		DBInstanceIdentifier = rdsInstance
	)
	time.sleep(600)
	
except Exception, e:
	if str(e).find("is not stopped, cannot be started.") > 0 :
		# its not already powered on
		Client(pushover_app_id).send_message("RDS instance already started - check out - $$$?" + str(e), title="getjplays.py")
	else:
		# other exceptions
		Client(pushover_app_id).send_message("RDS instance could not be started - quitting" + str(e), title="getjplays.py")
		exit()

# set up mysql
try:
	db = MySQLdb.connect(rdsHost, rdsUsername, rdsPassword, rdsInstance, connect_timeout=5)
except Exception, e:
	Client(pushover_app_id).send_message("RDS is apparently up but couldn't connect to MySQL - quitting" + str(e), title="getjplays.py")
	exit()

cursor = db.cursor()

# get the latest row
cursor.execute("SELECT MAX(tweetid) AS maximum FROM jtweets")

result = cursor.fetchall()

for i in result:
    if str(type(i[0])) == "<type 'NoneType'>":
		lastTweet=1
    else:
		lastTweet=i[0]

print "Last tweet ID: " + str(lastTweet)

# get the most recent triplejplays tweet
# OLD NON-CURSOR WAY search_results = api.user_timeline(screen_name="triplejplays", count=200, since_id=lastTweet)
search_results = tweepy.Cursor(api.user_timeline, screen_name="triplejplays", count=10000, since_id=lastTweet).items()

# insert into MySQL DB
while True:
    # as long as I still have a tweet to grab
	try:
		data = search_results.next()
	except StopIteration:
		break
		
	# convert from Python dict-like structure to JSON format
	jsoned_data = json.dumps(data._json)
	tweet = json.loads(jsoned_data)
	
	try:
		tweet["id"]
	except NameError:
		print "No ID!"
	else:
		insertID=str(tweet["id"])
	
	try:
		tweet["text"]
	except:
		print "No text!"
	else:
		insertText=tweet["text"].encode('utf-8')

	try:
		tweet["favourite_count"]
	except:
		insertFavourite_Count = "0"
	else:
		insertFavourite_Count = str(tweet["favorite_count"])

	try:
		tweet["entities"]["user_mentions"][0]["id"]
	except:
		insertUser_Mentions = "null"
	else:
		insertUser_Mentions = str(tweet["entities"]["user_mentions"][0]["id"])

	try:
		tweet["retweet_count"]
	except:
		insertRetweet_Count = "0"
	else:
		insertRetweet_Count = str(tweet["retweet_count"])

	try:
		tweet["created_at"]
	except:
		print "no date!"
	else:
		brisTime = parse(tweet["created_at"]).astimezone(brisbaneTimezone)
		insertDate = brisTime.strftime('%Y-%m-%d')
		insertTime = brisTime.strftime('%H:%M:00')

	
	sql = "insert into jtweets (tweetid, text, favourite_count, band_id, retweet_count, play_time, play_date) values(%s, %s, %s, %s, %s, %s, %s)"
	cursor.execute(sql, (insertID, insertText, insertFavourite_Count, insertUser_Mentions, insertRetweet_Count, insertTime, insertDate))


cursor.execute("select day(play_date), count(*) from jtweets where play_date > date(now()) - interval 15 day group by day(play_date)")

result = cursor.fetchall()

lastResults=chr(10)
for i in result:
	lastResults = lastResults + str(i[0]) + ": " + str(i[1]) + " tweets" + chr(10)

db.commit()
cursor.close()
db.close()

nohup=False
for i in sys.argv:
	if i == "--nohup" :
		nohup=True

if nohup==False:
	try:
		response = rdsClient.stop_db_instance(
			DBInstanceIdentifier = rdsInstance
		)
		Client(pushover_app_id).send_message("Successfully loaded jtweets and shut down RDS" + lastResults, title="getjplays.py")
		
	except Exception, e:
		if str(e).find(" is not in available state.") > 0 :
			# can't turn it off, already being turned off maybe?
			Client(pushover_app_id).send_message("Couldn't stop RDS instance - says its not in running state.  Investigate - $$$?" + chr(10) + lastResults + chr(10) + str(e), title="getjplays.py")
			
		else:
			# can't turn it off for some other reason
			Client(pushover_app_id).send_message("Couldn't stop RDS instance, not sure why" + chr(10) + lastResults + chr(10) + str(e), title="getjplays.py")
else:
	print "--nohup is True - not shutting down RDS.\n"
