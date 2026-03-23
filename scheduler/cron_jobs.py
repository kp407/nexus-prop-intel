from apscheduler.schedulers.blocking import BlockingScheduler
from main import run_pipeline

scheduler = BlockingScheduler(timezone="Asia/Kolkata")

@scheduler.scheduled_job("cron", hour="0,6,12,18", id="news_rss_crawl")
def news_rss_job():
    print("[Scheduler] Running news + RSS crawl...")
    run_pipeline()

@scheduler.scheduled_job("cron", hour=2, minute=0, id="full_crawl")
def full_crawl_job():
    print("[Scheduler] Running full crawl (PDFs + job boards)...")
    run_pipeline()

if __name__ == "__main__":
    print("[Scheduler] Starting NEXUS ASIA PROP INTEL scheduler...")
    scheduler.start()
