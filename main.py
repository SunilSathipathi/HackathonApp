from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime

from config import settings
from database import get_db, init_db, engine
from ai_service import AIQueryService, DynamicAIQueryService
from sync_service import SyncService
from scheduler import scheduler
from models import SyncLog, Employee, Department, Goal, Project, EmployeeProject

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
    context: Optional[Dict[str, Any]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "How many employees work as Mendix Developers?"
            }
        }

@app.get("/dbview", response_class=HTMLResponse)
async def db_view_home():
    """Home page for database viewer"""
    # Get all table names from the database
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    
    # Create a simple HTML page with links to each table
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Database Viewer</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }
            h1 { color: #333; }
            .table-list { margin-bottom: 20px; }
            .table-list a { 
                display: inline-block; 
                margin-right: 10px; 
                margin-bottom: 10px;
                padding: 8px 15px; 
                background-color: #f0f0f0; 
                color: #333; 
                text-decoration: none; 
                border-radius: 4px; 
            }
            .table-list a:hover { background-color: #ddd; }
        </style>
    </head>
    <body>
        <h1>Database Tables</h1>
        <div class="table-list">
    """
    
    # Add links for each table
    for table in table_names:
        html_content += f'<a href="/dbview/{table}">{table}</a>'
    
    # Add custom UI link for Goal-Employee relationships
    html_content += '<a href="/goals-employees">employee-goal</a>'
    # Add custom UI link for Employee-Department relationships
    html_content += '<a href="/employees-departments">employee_department</a>'
    # Add AI Query UI link
    html_content += '<a href="/ask">ask-ai</a>'

    html_content += """
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/ask", response_class=HTMLResponse, tags=["AI Query UI"])
async def ask_ui_page():
    """Simple UI to enter a question and see AI answer via /query."""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Ask AI</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 24px; }
            h1 { color: #333; }
            .container { max-width: 860px; }
            .row { display: flex; gap: 10px; margin-bottom: 14px; }
            input[type=text] { flex: 1; padding: 10px; font-size: 14px; }
            button { padding: 10px 16px; font-size: 14px; cursor: pointer; }
            .meta { margin-top: 10px; color: #555; }
            .answer { white-space: pre-wrap; background: #f9f9f9; padding: 12px; border-radius: 6px; }
            table { border-collapse: collapse; margin-top: 12px; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; font-size: 13px; }
            th { background: #f0f0f0; text-align: left; }
            .error { color: #b00; margin-top: 10px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Ask AI</h1>
            <div class="row">
                <input id="question" type="text" placeholder="Type your question..." />
                <button id="send">Send</button>
            </div>
            <div id="status" class="meta"></div>
            <div id="meta" class="meta"></div>
            <div id="answer" class="answer"></div>
            <div id="preview"></div>
            <div id="error" class="error"></div>
        </div>
        <script>
        const qEl = document.getElementById('question');
        const sendBtn = document.getElementById('send');
        const statusEl = document.getElementById('status');
        const metaEl = document.getElementById('meta');
        const answerEl = document.getElementById('answer');
        const previewEl = document.getElementById('preview');
        const errorEl = document.getElementById('error');

        async function ask() {
            const question = (qEl.value || '').trim();
            errorEl.textContent = '';
            answerEl.textContent = '';
            previewEl.innerHTML = '';
            metaEl.textContent = '';
            statusEl.textContent = 'Sending...';
            if (!question) { statusEl.textContent = ''; errorEl.textContent = 'Please enter a question.'; return; }
            try {
                const res = await fetch('/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question })
                });
                const data = await res.json();
                statusEl.textContent = '';
                if (!data.success) {
                    errorEl.textContent = data.error || 'Query failed.';
                    return;
                }
                answerEl.textContent = data.answer || '';
                metaEl.textContent = `Type: ${data.query_type || ''}` + (data.query_used ? ` | Query: ${data.query_used}` : '');
                const preview = data.data_preview;
                if (Array.isArray(preview) && preview.length > 0) {
                    const cols = Object.keys(preview[0]);
                    let html = '<table><thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead><tbody>';
                    for (const row of preview) {
                        html += '<tr>' + cols.map(c => `<td>${typeof row[c] === 'object' ? JSON.stringify(row[c]) : row[c]}</td>`).join('') + '</tr>';
                    }
                    html += '</tbody></table>';
                    previewEl.innerHTML = html;
                } else if (preview && typeof preview === 'object') {
                    previewEl.innerHTML = '<pre>' + JSON.stringify(preview, null, 2) + '</pre>';
                } else {
                    previewEl.innerHTML = '';
                }
            } catch (e) {
                statusEl.textContent = '';
                errorEl.textContent = 'Error: ' + (e && e.message ? e.message : e);
            }
        }

        sendBtn.addEventListener('click', ask);
        qEl.addEventListener('keydown', (e) => { if (e.key === 'Enter') ask(); });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

@app.get("/dbview/{table_name}", response_class=HTMLResponse)
async def db_view_table(table_name: str, db: Session = Depends(get_db)):
    """View data in a specific table"""
    # Check if table exists
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return HTMLResponse(content=f"<h1>Table {table_name} not found</h1>")
    
    # Get table columns
    columns = [column['name'] for column in inspector.get_columns(table_name)]
    
    # Query data from the table (SQLAlchemy 2.0 requires text())
    result = db.execute(text(f"SELECT * FROM {table_name}")).fetchall()
    
    # Create HTML table
    table_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Table: {table_name}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
            h1 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .back-link {{ margin-bottom: 20px; }}
            .back-link a {{ 
                display: inline-block;
                padding: 8px 15px;
                background-color: #f0f0f0;
                color: #333;
                text-decoration: none;
                border-radius: 4px;
            }}
            .back-link a:hover {{ background-color: #ddd; }}
        </style>
    </head>
    <body>
        <div class="back-link">
            <a href="/dbview">Back to Tables</a>
        </div>
        <h1>Table: {table_name}</h1>
        <table>
            <tr>
    """
    
    # Add table headers
    for column in columns:
        table_html += f"<th>{column}</th>"
    
    table_html += "</tr>"
    
    # Add table rows
    for row in result:
        table_html += "<tr>"
        for cell in row:
            table_html += f"<td>{str(cell)}</td>"
        table_html += "</tr>"
    
    table_html += """
        </table>
    </body>
    </html>
    """
    return HTMLResponse(content=table_html)


class QuestionResponse(BaseModel):
    success: bool
    question: str
    answer: str
    query_type: Optional[str] = None
    data_points: Optional[int] = None
    raw_data: Optional[Any] = None
    error: Optional[str] = None


class QueryResponse(BaseModel):
    success: bool
    question: str
    answer: str
    query_type: Optional[str] = None
    query_used: Optional[str] = None
    data_preview: Optional[Any] = None
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


@app.post("/query", response_model=QueryResponse, tags=["AI Query"])
async def dynamic_query(request: QuestionRequest, db: Session = Depends(get_db)):
    """
    Fully dynamic AI query endpoint.
    - Introspects schema
    - Routes to SQL and/or semantic search
    - Generates parameterized SQL via GPT and executes
    - Composes answer and returns preview and query used
    """
    try:
        logger.info(f"Dynamic query received: {request.question}")
        ai = DynamicAIQueryService(db)
        result = ai.answer(request.question)
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"Dynamic query error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing dynamic query: {str(e)}"
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
        from models import Employee, Department, Goal, Project, Skill, Form, Task
        
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
            "forms": db.query(Form).count(),
            "tasks": db.query(Task).count(),
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


@app.get("/goals-employees", response_class=HTMLResponse, tags=["UI"])
async def goals_employees_view(db: Session = Depends(get_db)):
    """Display Goal-Employee relationships in a nice UI format."""
    try:
        # Query goals with their associated employees
        goals_with_employees = db.query(Goal).join(Employee, Goal.employee_id == Employee.employee_id).all()
        
        # Create HTML content
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Goal-Employee Relationships</title>
            <style>
                body { 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 20px; 
                    line-height: 1.6; 
                    background-color: #f5f5f5;
                }
                .container { 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    background-color: white; 
                    padding: 20px; 
                    border-radius: 8px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }
                h1 { 
                    color: #333; 
                    text-align: center; 
                    margin-bottom: 30px;
                    border-bottom: 3px solid #007bff;
                    padding-bottom: 10px;
                }
                .stats { 
                    display: flex; 
                    justify-content: space-around; 
                    margin-bottom: 30px; 
                    background-color: #f8f9fa; 
                    padding: 15px; 
                    border-radius: 5px;
                }
                .stat-item { 
                    text-align: center; 
                }
                .stat-number { 
                    font-size: 2em; 
                    font-weight: bold; 
                    color: #007bff; 
                }
                .stat-label { 
                    color: #666; 
                    font-size: 0.9em; 
                }
                .goal-card { 
                    border: 1px solid #ddd; 
                    margin-bottom: 20px; 
                    padding: 20px; 
                    border-radius: 8px; 
                    background-color: #fff;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.05);
                    transition: transform 0.2s;
                }
                .goal-card:hover { 
                    transform: translateY(-2px); 
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                }
                .goal-header { 
                    display: flex; 
                    justify-content: space-between; 
                    align-items: center; 
                    margin-bottom: 15px;
                    border-bottom: 1px solid #eee;
                    padding-bottom: 10px;
                }
                .goal-title { 
                    font-size: 1.2em; 
                    font-weight: bold; 
                    color: #333; 
                    flex: 1;
                }
                .goal-status { 
                    padding: 5px 12px; 
                    border-radius: 20px; 
                    font-size: 0.8em; 
                    font-weight: bold; 
                    text-transform: uppercase;
                }
                .status-pending { background-color: #fff3cd; color: #856404; }
                .status-in-progress { background-color: #d1ecf1; color: #0c5460; }
                .status-completed { background-color: #d4edda; color: #155724; }
                .status-cancelled { background-color: #f8d7da; color: #721c24; }
                .status-default { background-color: #e2e3e5; color: #383d41; }
                .employee-info { 
                    background-color: #f8f9fa; 
                    padding: 15px; 
                    border-radius: 5px; 
                    margin-bottom: 15px;
                }
                .employee-name { 
                    font-weight: bold; 
                    color: #007bff; 
                    font-size: 1.1em;
                }
                .employee-details { 
                    color: #666; 
                    font-size: 0.9em; 
                    margin-top: 5px;
                }
                .goal-details { 
                    display: grid; 
                    grid-template-columns: 1fr 1fr; 
                    gap: 15px; 
                    margin-top: 15px;
                }
                .detail-item { 
                    display: flex; 
                    justify-content: space-between; 
                    padding: 8px 0; 
                    border-bottom: 1px solid #f0f0f0;
                }
                .detail-label { 
                    font-weight: bold; 
                    color: #555; 
                }
                .detail-value { 
                    color: #333; 
                }
                .progress-bar { 
                    width: 100%; 
                    height: 20px; 
                    background-color: #e9ecef; 
                    border-radius: 10px; 
                    overflow: hidden; 
                    margin-top: 10px;
                }
                .progress-fill { 
                    height: 100%; 
                    background-color: #28a745; 
                    transition: width 0.3s ease;
                }
                .back-link { 
                    margin-bottom: 20px; 
                    text-align: center;
                }
                .back-link a { 
                    display: inline-block;
                    padding: 10px 20px;
                    background-color: #6c757d;
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    transition: background-color 0.3s;
                }
                .back-link a:hover { 
                    background-color: #5a6268; 
                }
                .no-data { 
                    text-align: center; 
                    color: #666; 
                    font-style: italic; 
                    padding: 40px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="back-link">
                    <a href="/dbview">← Back to Database Tables</a>
                </div>
                <h1>Goal-Employee Relationships</h1>
        """
        
        if not goals_with_employees:
            html_content += """
                <div class="no-data">
                    <h3>No Goal-Employee relationships found</h3>
                    <p>There are currently no goals assigned to employees in the database.</p>
                </div>
            """
        else:
            # Calculate statistics
            total_goals = len(goals_with_employees)
            status_counts = {}
            for goal in goals_with_employees:
                status = goal.status or "Unknown"
                status_counts[status] = status_counts.get(status, 0) + 1
            
            # Add statistics section
            html_content += f"""
                <div class="stats">
                    <div class="stat-item">
                        <div class="stat-number">{total_goals}</div>
                        <div class="stat-label">Total Goals</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{status_counts.get('Completed', 0)}</div>
                        <div class="stat-label">Completed</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{status_counts.get('In Progress', 0)}</div>
                        <div class="stat-label">In Progress</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number">{status_counts.get('Pending', 0)}</div>
                        <div class="stat-label">Pending</div>
                    </div>
                </div>
            """
            
            # Add goal cards
            for goal in goals_with_employees:
                status = goal.status or "Unknown"
                status_class = f"status-{status.lower().replace(' ', '-')}" if status.lower() in ['pending', 'in progress', 'completed', 'cancelled'] else "status-default"
                
                progress = goal.progress_percentage or 0
                target_date = goal.target_date.strftime("%Y-%m-%d") if goal.target_date else "Not set"
                start_date = goal.start_date.strftime("%Y-%m-%d") if goal.start_date else "Not set"
                
                html_content += f"""
                <div class="goal-card">
                    <div class="goal-header">
                        <div class="goal-title">{goal.title or 'Untitled Goal'}</div>
                        <div class="goal-status {status_class}">{status}</div>
                    </div>
                    
                    <div class="employee-info">
                        <div class="employee-name">👤 {goal.employee.full_name}</div>
                        <div class="employee-details">
                            Employee ID: {goal.employee.employee_id} | 
                            Email: {goal.employee.email} | 
                            Designation: {goal.employee.designation or 'Not specified'}
                        </div>
                    </div>
                    
                    <div class="goal-details">
                        <div class="detail-item">
                            <span class="detail-label">Goal ID:</span>
                            <span class="detail-value">{goal.goal_id}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Priority:</span>
                            <span class="detail-value">{goal.priority or 'Not set'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Category:</span>
                            <span class="detail-value">{goal.category or 'Not specified'}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Start Date:</span>
                            <span class="detail-value">{start_date}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Target Date:</span>
                            <span class="detail-value">{target_date}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Progress:</span>
                            <span class="detail-value">{progress}%</span>
                        </div>
                    </div>
                    
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {progress}%"></div>
                    </div>
                    
                    {f'<div style="margin-top: 15px; padding: 10px; background-color: #f8f9fa; border-radius: 5px; font-style: italic; color: #666;">{goal.description}</div>' if goal.description else ''}
                </div>
                """
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        logger.error(f"Error displaying goal-employee relationships: {str(e)}")
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)

@app.get("/employees-departments", response_class=HTMLResponse, tags=["UI"])
async def employees_departments_view(db: Session = Depends(get_db)):
    """Display Employee-Department relationships in a simple UI."""
    try:
        employees = db.query(Employee).all()

        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Employee-Department Relationships</title>
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f5f5f5; }
                .container { max-width: 1000px; margin: 0 auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                h1 { color: #333; text-align: center; margin-bottom: 20px; border-bottom: 3px solid #28a745; padding-bottom: 10px; }
                .back-link a { display: inline-block; padding: 8px 15px; background-color: #6c757d; color: white; text-decoration: none; border-radius: 5px; margin-bottom: 15px; }
                .back-link a:hover { background-color: #5a6268; }
                .list { display: grid; grid-template-columns: 1fr; gap: 12px; }
                .card { border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px; background-color: #fafafa; }
                .emp-name { font-weight: 600; color: #222; }
                .emp-info { color: #555; font-size: 0.95em; margin-top: 6px; }
                .dept-tag { display: inline-block; padding: 4px 8px; background-color: #e9f7ef; color: #155724; border: 1px solid #c3e6cb; border-radius: 4px; font-size: 0.85em; margin-top: 8px; }
                .empty { text-align: center; color: #666; font-style: italic; padding: 30px; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="back-link"><a href="/dbview">← Back to Database Tables</a></div>
                <h1>Employee-Department Relationships</h1>
                <div class="list">
        """

        if not employees:
            html_content += "<div class='empty'>No employees found.</div>"
        else:
            for emp in employees:
                dept_name = emp.department.name if getattr(emp, "department", None) else "No department"
                html_content += f"""
                <div class="card">
                    <div class="emp-name">👤 {emp.full_name}</div>
                    <div class="emp-info">Employee ID: {emp.employee_id} | Email: {emp.email} | Designation: {emp.designation or 'Not specified'}</div>
                    <div class="dept-tag">🏢 Department: {dept_name}</div>
                </div>
                """

        html_content += """
                </div>
            </div>
        </body>
        </html>
        """

        return HTMLResponse(content=html_content)
    except Exception as e:
        logger.error(f"Error displaying employee-department relationships: {str(e)}")
        return HTMLResponse(content=f"<h1>Error: {str(e)}</h1>", status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )