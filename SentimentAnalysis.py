from tweepy import OAuthHandler
from textblob import TextBlob
import tweepy,re

def get_sentiment(name):
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

 for tweet in tweepy.Cursor(api.search, q=name, rpp=100, count=20, result_type="recent", include_entities=True, lang="en").items(500):
  total += 1
  #print tweet.created_at, tweet.text
  analysis = TextBlob(clean_tweet(tweet.text))
  # set sentiment
  if analysis.sentiment.polarity > 0:
   #print 'positive'
   positive += 1
  elif analysis.sentiment.polarity == 0:
   #print 'neutral'
   neutral += 1
  else:
   negative += 1
   #print 'negative'

 print '----------------- %s ----------------------------' % name
 print "Total %d" % total
 print "Positive %d" % positive
 print "Negative %d" % negative
 print "Neutral %d" % neutral


def main():
    name1 = "Ubisoft"
    name2 = "EA"
    name3 = "XBOX"
    name4 = "Playstation"
    get_sentiment(name1)
    get_sentiment(name2)
    get_sentiment(name3)
    get_sentiment(name4)
    
if __name__ == "__main__": main()