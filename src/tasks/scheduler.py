import os
from .celery_app import celery_app
from .worker_tasks import scrape_subreddit_task, scrape_twitter_task

def queue_subreddits():
    """Read subreddits from file and queue scraping tasks."""
    subreddits_file = os.getenv("SUBREDDITS_FILE_PATH", "subreddits.txt")
    
    if not os.path.exists(subreddits_file):
        print(f"Error: {subreddits_file} not found.")
        return

    with open(subreddits_file, 'r') as f:
        subreddits = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    print(f"Queueing tasks for {len(subreddits)} subreddits...")
    for subreddit in subreddits:
        scrape_subreddit_task.delay(subreddit)

def queue_twitter_queries():
    """Read twitter queries from file and queue tasks."""
    queries_file = "twitter_queries.txt"
    
    if not os.path.exists(queries_file):
        print(f"Info: {queries_file} not found. Skipping Twitter tasks.")
        return

    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    print(f"Queueing tasks for {len(queries)} Twitter queries...")
    for query in queries:
        print(f"Queueing Twitter task for: {query}")
        scrape_twitter_task.delay(query, max_results=50)

if __name__ == "__main__":
    print("--- Starting Lead Harvest Scheduler ---")
    queue_subreddits()
    queue_twitter_queries()
    print("--- All platforms queued successfully ---")
