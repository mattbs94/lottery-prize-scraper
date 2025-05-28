from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', minutes=1)
def scheduled_job():
    logger.info('Running prize scraper job...')
    try:
        subprocess.call(['python', 'update_live_prizes.py'])
        logger.info('Job completed successfully!')
    except Exception as e:
        logger.error(f'Error running job: {e}')

if __name__ == '__main__':
    logger.info('Starting scheduler...')
    scheduler.start()
