import os
from .celery_app import celery_app
from .worker_tasks import scrape_subreddit_task

def queue_subreddits():
    """Read subreddits from file and queue scraping tasks."""
    subreddits_file = os.getenv("SUBREDDITS_FILE_PATH", "subreddits.txt")
    
    if not os.path.exists(subreddits_file):
        print(f"Error: {subreddits_file} not found.")
        return

    with open(subreddits_file, 'r') as f:
        # Filter comments and empty lines
        subreddits = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    print(f"Queueing tasks for {len(subreddits)} subreddits...")
    
    for subreddit in subreddits:
        print(f"Queueing task for: {subreddit}")
        # Add a delay between queueing to avoid sudden burst in broker
        scrape_subreddit_task.delay(subreddit)

    print("All tasks queued successfully.")

if __name__ == "__main__":
    queue_subreddits()
