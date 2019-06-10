#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Script para capturar tweets en tiempo real y guardarlos tanto en MongoDB como en un archivo .json

import pymongo
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime
import numpy as np

# BBDD: TwitterStream
connection = pymongo.MongoClient('localhost', 27017)
db = connection.TwitterStream
db.A3media.ensure_index("id", unique=True, dropDups=True)
collection = db.A3media

#Términos a buscar:
keywords = ['#eldebate','#ElDebateDecisivo','#DebateAtresmedia','#DebateDecisivo','#DebateAtresmediaESP'] #<- AQUÍ LO QUE SE QUIERE LEER

consumer_key = '......'
consumer_secret = '.......'
access_token = '.........'
access_token_secret = '........'

class StdOutListener(StreamListener):

    def on_data(self, data):
        try:
            t = json.loads(data)

            with open('A3media.json','a', encoding="utf8") as f:
                f.write(data)

            tweet_id = t['id_str'] 
            username = t['user']['screen_name']  
            followers = t['user']['followers_count']  
            text = t['text'] 
            hashtags = t['entities']['hashtags']  
            dt = t['created_at'] 
            language = t['lang']
            location = t['user']['location']
            try:
                fulltext = t['extended_tweet']['full_text']
            except:
                fulltext = text

            created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

            tweet = {'id':tweet_id, 'username':username, 'followers':followers, 'text':text, 'hashtags':hashtags, 'language':language, 'created':created, 'fulltext':fulltext, 'location':location}

            collection.save(tweet)

        except:
            pass
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)

    stream.filter(track=keywords)


