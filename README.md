# Real-Time Twitter Data Pipeline with Analytics

![Pipeline Architecture](https://github.com/user-attachments/assets/a7c1800f-a4f1-426a-a1bc-ad733b1f5caa)

## 📌 Overview
A production-grade streaming pipeline that processes **5,000+ tweets/minute** with:
- Real-time ingestion from Twitter API
- Kafka/Event Hubs for fault-tolerant buffering
- Spark Structured Streaming for transformations
- MongoDB/Cosmos DB for flexible storage
- Sentiment analysis and Power BI visualization

## 🛠 Tech Stack
| Component           | Options                          |
|---------------------|----------------------------------|
| **Streaming**       | Kafka / [Azure Event Hubs](https://azure.microsoft.com/en-us/products/event-hubs/) |
| **Processing**      | [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) (Local or [Azure Databricks](https://azure.microsoft.com/en-us/products/databricks/)) |
| **Storage**         | [MongoDB](https://www.mongodb.com/) / [Azure Cosmos DB](https://azure.microsoft.com/en-us/products/cosmos-db/) |
| **Analytics**       | [TextBlob](https://textblob.readthedocs.io/) / [NLTK](https://www.nltk.org/) |
| **Visualization**   | [Power BI](https://powerbi.microsoft.com/) / [MongoDB Charts](https://www.mongodb.com/products/charts) |

## ✨ Key Features
- **Real-time Processing**  
  - Filters tweets by keywords/language
  - Handles 5,000+ tweets/minute with auto-scaling

- **Sentiment Analysis**  
  ```python
  from textblob import TextBlob
  def analyze(text):
      analysis = TextBlob(text)
      return {
          'polarity': analysis.sentiment.polarity,
          'sentiment': 'positive' if analysis.sentiment.polarity > 0 else 'negative' if analysis.sentiment.polarity < 0 else 'neutral'
      }
## Author
Venugopal Reddy Gangula
