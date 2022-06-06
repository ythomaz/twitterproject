from twitter_authentication import bearer_token, consumer_key, consumer_secret, access_token, access_token_secret
from mongo_authentication import password
from pymongo import MongoClient
import tweepy

#configurando tweepy
tweepy_client = tweepy.Client(bearer_token=bearer_token, consumer_key=consumer_key,
	consumer_secret=consumer_secret, access_token=access_token, access_token_secret=access_token_secret)

#configurando pymongo e obtendo nome das coleções
mongo_client = MongoClient('mongodb+srv://ythomaz:'+password+'@cluster0.30s3m.mongodb.net/?retryWrites=true&w=majority')
db=mongo_client.tweets
db.list_collection_names()

#obtendo tweets e armazenando no MongoDB
query_lula = 'lula -RT'
query_bolsonaro = 'bolsonaro -RT'
def get_tweets(query, collection_name):
    tweet_pages = tweepy.Paginator(tweepy_client.search_recent_tweets, query=query,
                                   tweet_fields=['text', 'author_id','created_at'], max_results=100, limit=80)
    tweets = []
    for page in tweet_pages:
        tweets += page.data
        df = pd.DataFrame(tweets, columns=['text', 'author_id','created_at'])
    df_dict = df.to_dict(orient='records')
    return db[collection_name].insert_many(df_dict)

get_tweets(query_lula,'lula_tweets')

get_tweets(query_bolsonaro,'bolsonaro_tweets')
