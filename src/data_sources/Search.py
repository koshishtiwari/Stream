"""Online Search Data Source using Gemini"""
import os
import json
import logging
from typing import Dict, List, Any, Generator, Optional
from datetime import datetime
import time

# Import Gemini
from google import genai

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeminiSearchDataSource:
    """Data source for online search using Google's Gemini models"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.0-flash"):
        """
        Initialize Gemini search data source
        
        Args:
            api_key: Google API key (will use env var GOOGLE_API_KEY if not provided)
            model: Gemini model to use
        """
        self.api_key = api_key or os.environ.get('GOOGLE_API_KEY')
        
        if not self.api_key:
            raise ValueError("Google API key not found. Set GOOGLE_API_KEY environment variable or provide it as a parameter.")
        
        # Configure genai with the API key
        genai.configure(api_key=self.api_key)
        
        self.model = model
        self.client = genai.Client()
        logger.info(f"Gemini search data source initialized with model: {model}")
    
    def search_news(self, topic: str, max_results: int = 5) -> List[Dict[str, Any]]:
        """
        Search for news on a specific topic
        
        Args:
            topic: News topic to search for
            max_results: Maximum number of results to return
            
        Returns:
            List of news articles with metadata
        """
        prompt = f"What's the latest news about {topic}? Please provide {max_results} recent articles."
        
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config={"tools": [{"google_search": {}}]}
            )
            
            # Extract search metadata
            search_query = response.candidates[0].grounding_metadata.web_search_queries
            sources = [site.web.title for site in response.candidates[0].grounding_metadata.grounding_chunks]
            
            # Log search details
            logger.info(f"Search query: {search_query}")
            logger.info(f"Sources used: {sources}")
            
            # Process the response text to extract news articles
            # This is simplified - in a real application, we'd parse this more carefully
            news_items = []
            
            # Extract article metadata if available
            if hasattr(response.candidates[0].grounding_metadata, 'grounding_chunks'):
                for i, chunk in enumerate(response.candidates[0].grounding_metadata.grounding_chunks):
                    if hasattr(chunk, 'web') and i < max_results:
                        news_items.append({
                            "title": chunk.web.title,
                            "url": chunk.web.url,
                            "snippet": chunk.web.snippet,
                            "source": "gemini_search",
                            "timestamp": datetime.now().isoformat()
                        })
            
            # If we couldn't extract structured data, use the text response
            if not news_items:
                news_items.append({
                    "title": f"News on {topic}",
                    "content": response.text,
                    "source": "gemini_search",
                    "timestamp": datetime.now().isoformat()
                })
                
            logger.info(f"Retrieved {len(news_items)} news items about '{topic}'")
            return news_items
            
        except Exception as e:
            logger.error(f"Error searching for news: {e}")
            return []
    
    def search_information(self, query: str) -> Dict[str, Any]:
        """
        Search for general information on a topic
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with search results and metadata
        """
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=query,
                config={"tools": [{"google_search": {}}]}
            )
            
            # Extract search metadata
            search_query = response.candidates[0].grounding_metadata.web_search_queries
            sources = []
            
            if hasattr(response.candidates[0].grounding_metadata, 'grounding_chunks'):
                sources = [
                    {
                        "title": site.web.title, 
                        "url": site.web.url
                    } 
                    for site in response.candidates[0].grounding_metadata.grounding_chunks 
                    if hasattr(site, 'web')
                ]
            
            result = {
                "query": query,
                "search_query_used": search_query,
                "response": response.text,
                "sources": sources,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Retrieved information for query '{query}'")
            return result
            
        except Exception as e:
            logger.error(f"Error searching for information: {e}")
            return {
                "query": query,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def monitor_topic(self, topic: str, interval: int = 3600, max_results: int = 3) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Monitor a topic for new information at regular intervals
        
        Args:
            topic: Topic to monitor
            interval: Time between checks in seconds
            max_results: Maximum number of results per check
            
        Yields:
            List of news items per check
        """
        logger.info(f"Starting monitoring for topic: '{topic}' at {interval}s intervals")
        
        try:
            while True:
                news_items = self.search_news(topic, max_results)
                yield news_items
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info(f"Stopped monitoring topic: '{topic}'")
            return
        except Exception as e:
            logger.error(f"Error monitoring topic: {e}")
            return
    
    def format_for_kafka(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format search data for Kafka ingestion
        
        Args:
            data: Raw search data
            
        Returns:
            Formatted data for Kafka
        """
        return {
            "source": "gemini_search",
            "data_type": "news",
            "timestamp": datetime.now().isoformat(),
            "payload": data
        }

# Example usage
if __name__ == "__main__":
    # You would need to set the GOOGLE_API_KEY environment variable
    try:
        search = GeminiSearchDataSource()
        
        # Search for news on a topic
        news = search.search_news("artificial intelligence")
        print(f"Found {len(news)} news items")
        
        for item in news:
            print(f"Title: {item.get('title')}")
            print(f"URL: {item.get('url')}")
            print("---")
        
        # Get information on a specific query
        info = search.search_information("What are the latest developments in quantum computing?")
        print(f"Search Query: {info.get('search_query_used')}")
        print(f"Response: {info.get('response')}")
        print(f"Sources: {len(info.get('sources', []))}")
        
        # Monitor a topic (uncomment to run)
        # This will continuously yield new information
        # print("Starting monitoring for 'climate change'...")
        # for updates in search.monitor_topic("climate change", interval=10, max_results=2):
        #     print(f"Update: {len(updates)} new items")
        #     time.sleep(5)  # Just for the example
        
    except Exception as e:
        logger.error(f"Error in example: {e}")
