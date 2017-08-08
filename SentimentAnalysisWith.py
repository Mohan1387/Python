from tweepy import OAuthHandler
from textblob import TextBlob
import tweepy,re

access_token = "90891917-XSEZhgvMTeYICCoz5Msld3MVdHHCboY3fOBB3Xd9F"
access_token_secret = "wiis10HorfjPecHcJtgsASBN6p2WDbxWszlkTKD8sRdax"
consumer_key = "3U10m5wXhF9WsAPNh67HiVeK5"
consumer_secret = "2W2qyKDkmZhXtoBlyzeTV427iVT28D2DF3iWoZ6QIZluejFZxU"

auth = OAuthHandler(consumer_key, consumer_secret)     
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

public_tweets = api.home_timeline()

positive = 0
negative = 0
neutral = 0
total = 0

def clean_tweet(tweet):
	return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

tweets = []

parsed_tweet = {}

for tweet in tweepy.Cursor(api.search, q="thirumurugan", rpp=100, count=20, result_type="recent", include_entities=True, lang="en").items(50):
 parsed_tweet['text'] = tweet.text
 analysis = TextBlob(clean_tweet(tweet.text))
 print tweet.text+"   "+str(analysis.sentiment.polarity)
 # set sentiment
 if analysis.sentiment.polarity > 0.0:
  parsed_tweet['sentiment'] = 'positive'
 elif analysis.sentiment.polarity == 0.0:
  parsed_tweet['sentiment'] = 'neutral'
 else:
  parsed_tweet['sentiment'] = 'negative'

 if tweet.retweet_count > 0:
  # if tweet has retweets, ensure that it is appended only once
  if parsed_tweet not in tweets:
   tweets.append(parsed_tweet)
 else:
     tweets.append(parsed_tweet)

ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
nutweets = [tweet for tweet in tweets if tweet['sentiment'] == 'neutral']


print '---------------------------------------------'
i = 0
for tweet in tweets: 
 print tweet
 
print "Positive tweets: %d" % len(ptweets)
print "negative tweets: %d" % len(ntweets)
print "neutral tweets: %d" % len(nutweets)
print "total tweets: %d" % len(tweets)
