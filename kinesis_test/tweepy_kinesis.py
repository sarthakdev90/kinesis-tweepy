from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto
import json
import time
from credentials import (aws_access_key, aws_access_secret, consumer_key,
                    consumer_token, access_token, access_secret)

#Kinesis stream
stream_name = 'tweet_test'

#List of keywords
list_of_keywords = ['modi', 'tendulkar']

class TweetListener(StreamListener):

    def on_status(self, tweet):
        _tweet = tweet.__dict__['_json']
        print "Tweet: ", _tweet
        #self.send_tweet_to_stream(_tweet)

    def send_tweet_to_stream(self, tweet):
        conn = boto.connect_kinesis(aws_access_key, aws_access_secret)

        r = conn.describe_stream(stream_name)
        description = r.get('StreamDescription')
        status= description.get('StreamStatus')

        try:
            if status == 'DELETING':
                print 'The stream: {s} is being deleted, please rerun the script.'.format(s=stream_name)
                #sys.exit(1)
            elif status == 'ACTIVE':
                time.sleep(10)
        except:
            # We'll assume the stream didn't exist so we will try to create it with just one shard
            conn.create_stream(stream_name, 1)
            time.sleep(10)

        conn.put_record(stream_name, json.dumps(tweet), str(int(time.time())))


if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    l = TweetListener()
    streamer = Stream(auth, listener=l)
    streamer.filter(track = list_of_keywords)
