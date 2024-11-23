import websocket
import json
import pandas as pd
from threading import Timer

url = 'wss://api.benzinga.com/api/v1/earnings-call-transcripts/stream?token=YOUR API KEY'

data = []

# WebSocket event handlers
def on_message(ws, message):
    parsed_data = json.loads(message)
    content = parsed_data['data']['content']
    transcript_chunk = content['transcript_chunk']
    security = content['security']
    
    # Extract relevant fields and append to data list
    data.append({
        'call_id': content['call_id'],
        'transcript_chunk': transcript_chunk,
        'ticker': security['ticker'],
        'company_name': security['company_name'],
        'time': content['time']
    })
    
    print(f"Received transcript chunk: {transcript_chunk}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

def on_open(ws):
    print("WebSocket connection opened, receiving data...")

# Function to stop the WebSocket connection after 30 seconds
def stop_websocket():
    print("Stopping WebSocket connection after 30 seconds...")
    ws.close()

ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
ws.on_open = on_open

websocket_thread = Timer(0, ws.run_forever)
websocket_thread.start()
stop_timer = Timer(30, stop_websocket)
stop_timer.start()

# Convert data to DataFrame
df = pd.DataFrame(data)
df.head()

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download VADER lexicon
nltk.download('vader_lexicon')

# Initialize VADER Sentiment Intensity Analyzer
sid = SentimentIntensityAnalyzer()

# Add sentiment analysis to each transcript chunk
df['sentiment'] = df['transcript_chunk'].apply(lambda x: sid.polarity_scores(x)['compound'])

# Display sentiment scores
df[['transcript_chunk', 'sentiment']].head()

# Group by ticker and calculate the average sentiment score
average_sentiment = df.groupby('ticker')['sentiment'].mean().reset_index()
average_sentiment = average_sentiment.sort_values(by='sentiment', ascending=False)

# Display the average sentiment scores by ticker
print("Average Sentiment by Ticker:")
print(average_sentiment)

# Find the top 5 most positive statements
most_positive = df.nlargest(5, 'sentiment')[['ticker', 'company_name', 'transcript_chunk', 'sentiment']]
print("Most Positive Statements:")
display(most_positive)

# Find the top 5 most negative statements
most_negative = df.nsmallest(5, 'sentiment')[['ticker', 'company_name', 'transcript_chunk', 'sentiment']]
print("Most Negative Statements:")
display(most_negative)
