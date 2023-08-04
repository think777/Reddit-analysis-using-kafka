from kafka import KafkaProducer
import json
import praw
import pandas as pd




# Define Kafka producer configuration
bootstrap_servers = ['localhost:9092']

# Create Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

# Define search parameters
q = 'bitcoin'
sub = 'travel'
sort = 'top'
limit = 1000

# Get top posts from subreddit
top_posts = r.subreddit(sub)

# Iterate over the top posts and send them to Kafka
for post in top_posts.new(limit = limit):
    title = post.title
    description = post.selftext
    score = post.score
    num_comments = post.num_comments
    publish_date = post.created
    link = 'https://www.reddit.com' + post.permalink
    data = {
        'title': title,
	    'description':description,
        'score': score,
        'num_comments': num_comments,
        'publish_date': publish_date,
        'link': link
    }
    # Send the data to the Kafka topic for the subreddit
    topic = sub  # Use the subreddit name as the Kafka topic
    producer.send(topic, value=data)
    
# Flush the Kafka producer to ensure all messages are sent
producer.flush()
