from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import logging
from config import settings
from database import get_db_session
from sync_service import SyncService

logger = logging.getLogger(__name__)


class DataSyncScheduler:
    """Scheduler for periodic data synchronization from Mendix."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.is_running = False
    
    def sync_job(self):
        """Job function that performs data synchronization."""
        logger.info(f"Starting scheduled sync job at {datetime.utcnow()}")
        
        db = get_db_session()
        try:
            sync_service = SyncService(db)
            results = sync_service.sync_all()
            logger.info(f"Scheduled sync completed successfully: {results}")
        except Exception as e:
            logger.error(f"Error in scheduled sync job: {str(e)}")
        finally:
            db.close()
    
    def start(self):
        """Start the scheduler."""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        try:
            # Add job with interval trigger
            self.scheduler.add_job(
                func=self.sync_job,
                trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
                id='mendix_sync_job',
                name='Sync Mendix Employee Data',
                replace_existing=True,
                max_instances=1  # Prevent concurrent runs
            )
            
            self.scheduler.start()
            self.is_running = True
            logger.info(f"Scheduler started - sync interval: {settings.sync_interval_minutes} minutes")
            
            # Run initial sync immediately
            logger.info("Running initial sync...")
            self.sync_job()
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            raise
    
    def stop(self):
        """Stop the scheduler."""
        if not self.is_running:
            return
        
        try:
            self.scheduler.shutdown(wait=False)
            self.is_running = False
            logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {str(e)}")
    
    def get_status(self):
        """Get scheduler status."""
        return {
            "running": self.is_running,
            "sync_interval_minutes": settings.sync_interval_minutes,
            "jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None
                }
                for job in self.scheduler.get_jobs()
            ] if self.is_running else []
        }


# Global scheduler instance
scheduler = DataSyncScheduler()