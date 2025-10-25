from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging
from datetime import datetime

from config import settings
from database import get_db, init_db
from ai_service import AIQueryService
from sync_service import SyncService
from scheduler import scheduler
from models import SyncLog

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Mendix Employee Intelligence API",
    description="AI-powered API for querying employee data synced from Mendix",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models for request/response
class QuestionRequest(BaseModel):
    question: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "How many employees work as Mendix Developers?"
            }
        }


class QuestionResponse(BaseModel):
    success: bool
    question: str
    answer: str
    query_type: Optional[str] = None
    data_points: Optional[int] = None
    raw_data: Optional[Any] = None
    error: Optional[str] = None


class SyncResponse(BaseModel):
    success: bool
    message: str
    results: Optional[Dict[str, int]] = None
    timestamp: datetime


class HealthResponse(BaseModel):
    status: str
    database: str
    scheduler: Dict[str, Any]
    timestamp: datetime


@app.on_event("startup")
async def startup_event():
    """Initialize database and start scheduler on application startup."""
    logger.info("Starting application...")
    
    try:
        # Initialize database
        init_db()
        logger.info("Database initialized successfully")
        
        # Start scheduler
        scheduler.start()
        logger.info("Scheduler started successfully")
        
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown."""
    logger.info("Shutting down application...")
    
    try:
        scheduler.stop()
        logger.info("Scheduler stopped successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Mendix Employee Intelligence API",
        "version": "1.0.0",
        "description": "AI-powered API for querying employee data",
        "endpoints": {
            "ask": "POST /api/ask - Ask questions about employee data",
            "sync": "POST /api/sync - Manually trigger data synchronization",
            "health": "GET /api/health - Check API health status",
            "stats": "GET /api/stats - Get database statistics",
            "scheduler": "GET /api/scheduler - Get scheduler status",
            "sync-history": "GET /api/sync-history - Get sync operation history"
        }
    }


@app.post("/api/ask", response_model=QuestionResponse, tags=["AI Query"])
async def ask_question(request: QuestionRequest, db: Session = Depends(get_db)):
    """
    Ask a question about employee data using natural language.
    
    This endpoint uses OpenAI to understand your question and query the database
    to provide intelligent answers about employees, goals, projects, and skills.
    
    Example questions:
    - "How many employees work as Mendix Developers?"
    - "Who are all the Senior Delivery Managers?"
    - "What are the pending goals for employee LCL16110165?"
    - "List all active projects"
    - "Which employees report to manager LCL16110001?"
    - "Show me employees with blocked accounts"
    """
    try:
        logger.info(f"Received question: {request.question}")
        
        ai_service = AIQueryService(db)
        result = ai_service.answer_question(request.question)
        
        return QuestionResponse(**result)
        
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing question: {str(e)}"
        )


@app.post("/api/sync", response_model=SyncResponse, tags=["Synchronization"])
async def manual_sync(db: Session = Depends(get_db)):
    """
    Manually trigger data synchronization from Mendix API.
    
    This will fetch the latest data for:
    - Employees
    - Departments
    - Goals
    - Projects
    - Skills
    """
    try:
        logger.info("Manual sync triggered")
        
        sync_service = SyncService(db)
        results = sync_service.sync_all()
        
        return SyncResponse(
            success=True,
            message="Data synchronization completed successfully",
            results=results,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Error during manual sync: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during synchronization: {str(e)}"
        )


@app.get("/api/health", response_model=HealthResponse, tags=["Monitoring"])
async def health_check(db: Session = Depends(get_db)):
    """Check the health status of the API and its components."""
    try:
        # Test database connection
        db.execute("SELECT 1")
        db_status = "connected"
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        db_status = f"error: {str(e)}"
    
    return HealthResponse(
        status="healthy" if db_status == "connected" else "unhealthy",
        database=db_status,
        scheduler=scheduler.get_status(),
        timestamp=datetime.utcnow()
    )


@app.get("/api/stats", tags=["Monitoring"])
async def get_statistics(db: Session = Depends(get_db)):
    """Get database statistics and record counts."""
    try:
        from models import Employee, Department, Goal, Project, Skill
        
        stats = {
            "employees": {
                "total": db.query(Employee).count(),
                "active": db.query(Employee).filter(Employee.active == True).count(),
                "blocked": db.query(Employee).filter(Employee.blocked == True).count()
            },
            "departments": db.query(Department).count(),
            "goals": {
                "total": db.query(Goal).count(),
                "pending": db.query(Goal).filter(Goal.status == "Pending").count(),
                "in_progress": db.query(Goal).filter(Goal.status == "In Progress").count(),
                "completed": db.query(Goal).filter(Goal.status == "Completed").count()
            },
            "projects": {
                "total": db.query(Project).count(),
                "active": db.query(Project).filter(Project.status == "Active").count()
            },
            "skills": db.query(Skill).count(),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving statistics: {str(e)}"
        )


@app.get("/api/scheduler", tags=["Monitoring"])
async def get_scheduler_status():
    """Get the status of the background scheduler."""
    return scheduler.get_status()


@app.get("/api/sync-history", tags=["Monitoring"])
async def get_sync_history(limit: int = 20, db: Session = Depends(get_db)):
    """Get the history of sync operations."""
    try:
        logs = db.query(SyncLog).order_by(
            SyncLog.sync_started_at.desc()
        ).limit(limit).all()
        
        return {
            "total_logs": len(logs),
            "logs": [
                {
                    "id": log.id,
                    "sync_type": log.sync_type,
                    "status": log.status,
                    "records_synced": log.records_synced,
                    "error_message": log.error_message,
                    "sync_started_at": log.sync_started_at.isoformat() if log.sync_started_at else None,
                    "sync_completed_at": log.sync_completed_at.isoformat() if log.sync_completed_at else None
                }
                for log in logs
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting sync history: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving sync history: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )