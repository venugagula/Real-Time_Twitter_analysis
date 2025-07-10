# scripts/sentiment_utils.py
from textblob import TextBlob

def analyze_sentiment(text):
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    sentiment = (
        "positive" if polarity > 0
        else "negative" if polarity < 0
        else "neutral"
    )
    return sentiment, polarity
