from openai import OpenAI
from sqlalchemy.orm import Session
from typing import Dict, List, Any
import json
import logging
from config import settings
from models import Employee, Department, Goal, Project, Skill

logger = logging.getLogger(__name__)


class AIQueryService:
    """Service for answering questions about employee data using OpenAI."""
    
    def __init__(self, db: Session):
        self.db = db
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = "gpt-4o-mini"  # Using cost-effective model
    
    def _get_database_context(self) -> str:
        """Get a summary of the database for context."""
        try:
            # Get counts
            employee_count = self.db.query(Employee).count()
            goal_count = self.db.query(Goal).count()
            project_count = self.db.query(Project).count()
            skill_count = self.db.query(Skill).count()
            
            # Get sample data structures
            sample_employee = self.db.query(Employee).first()
            sample_goal = self.db.query(Goal).first()
            
            context = f"""
Database Schema and Context:

Total Records:
- Employees: {employee_count}
- Goals: {goal_count}
- Projects: {project_count}
- Skills: {skill_count}

Employee Table Structure:
- employee_id (unique identifier)
- full_name
- email
- designation (job title)
- salary
- manager_employee_id
- blocked (boolean)
- active (boolean)
- last_login
- created_date, changed_date

Employee Goals Table:
- goal_id
- employee_id (foreign key)
- title
- description
- target_date
- status (Pending, In Progress, Completed, Cancelled)
- progress_percentage (0-100)
- priority (Low, Medium, High)
- category

Projects Table:
- project_id
- name
- description
- status (Active, On Hold, Completed, Cancelled)
- start_date, end_date
- client_name
- project_type

Skills Table:
- skill_id
- name
- category (Technical, Soft Skills, Domain Knowledge)
- description

Employee-Project Relationship:
- Links employees to projects
- Includes role and allocation_percentage

Employee-Skill Relationship:
- Links employees to skills
- Includes proficiency_level (Beginner, Intermediate, Advanced, Expert)
- years_of_experience
- certified (boolean)
"""
            return context
            
        except Exception as e:
            logger.error(f"Error getting database context: {str(e)}")
            return "Database context unavailable"
    
    def _execute_query(self, query_type: str, parameters: Dict) -> List[Dict]:
        """Execute a database query based on AI-generated parameters."""
        try:
            if query_type == "get_employees":
                filters = parameters.get("filters", {})
                query = self.db.query(Employee)
                
                if filters.get("name"):
                    # Search by name (partial match, case insensitive)
                    query = query.filter(Employee.full_name.ilike(f"%{filters['name']}%"))
                if filters.get("designation"):
                    query = query.filter(Employee.designation.ilike(f"%{filters['designation']}%"))
                if filters.get("active") is not None:
                    query = query.filter(Employee.active == filters["active"])
                if filters.get("blocked") is not None:
                    query = query.filter(Employee.blocked == filters["blocked"])
                
                employees = query.all()
                return [
                    {
                        "employee_id": emp.employee_id,
                        "full_name": emp.full_name,
                        "email": emp.email,
                        "designation": emp.designation,
                        "manager_employee_id": emp.manager_employee_id,
                        "active": emp.active,
                        "blocked": emp.blocked
                    }
                    for emp in employees
                ]
            
            elif query_type == "get_goals":
                filters = parameters.get("filters", {})
                query = self.db.query(Goal)
                
                if filters.get("employee_id"):
                    query = query.filter(Goal.employee_id == filters["employee_id"])
                if filters.get("status"):
                    query = query.filter(Goal.status == filters["status"])
                if filters.get("priority"):
                    query = query.filter(Goal.priority == filters["priority"])
                
                goals = query.all()
                return [
                    {
                        "goal_id": goal.goal_id,
                        "employee_id": goal.employee_id,
                        "title": goal.title,
                        "description": goal.description,
                        "status": goal.status,
                        "progress_percentage": goal.progress_percentage,
                        "priority": goal.priority,
                        "target_date": goal.target_date.isoformat() if goal.target_date else None
                    }
                    for goal in goals
                ]
            
            elif query_type == "get_employee_with_goals":
                employee_id = parameters.get("employee_id")
                employee = self.db.query(Employee).filter(
                    Employee.employee_id == employee_id
                ).first()
                
                if not employee:
                    return []
                
                goals = self.db.query(Goal).filter(
                    Goal.employee_id == employee_id
                ).all()
                
                return [{
                    "employee": {
                        "employee_id": employee.employee_id,
                        "full_name": employee.full_name,
                        "designation": employee.designation,
                        "email": employee.email
                    },
                    "goals": [
                        {
                            "title": goal.title,
                            "status": goal.status,
                            "progress_percentage": goal.progress_percentage,
                            "priority": goal.priority
                        }
                        for goal in goals
                    ]
                }]
            
            elif query_type == "get_pending_goals":
                goals = self.db.query(Goal).filter(
                    Goal.status.in_(["Pending", "In Progress"])
                ).all()
                
                result = []
                for goal in goals:
                    employee = self.db.query(Employee).filter(
                        Employee.employee_id == goal.employee_id
                    ).first()
                    
                    result.append({
                        "goal_id": goal.goal_id,
                        "title": goal.title,
                        "status": goal.status,
                        "progress_percentage": goal.progress_percentage,
                        "employee_name": employee.full_name if employee else "Unknown",
                        "employee_id": goal.employee_id
                    })
                
                return result
            
            elif query_type == "get_projects":
                filters = parameters.get("filters", {})
                query = self.db.query(Project)
                
                if filters.get("status"):
                    query = query.filter(Project.status == filters["status"])
                
                projects = query.all()
                return [
                    {
                        "project_id": proj.project_id,
                        "name": proj.name,
                        "description": proj.description,
                        "status": proj.status,
                        "client_name": proj.client_name
                    }
                    for proj in projects
                ]
            
            elif query_type == "get_team_members":
                manager_id = parameters.get("manager_id")
                team = self.db.query(Employee).filter(
                    Employee.manager_employee_id == manager_id
                ).all()
                
                return [
                    {
                        "employee_id": emp.employee_id,
                        "full_name": emp.full_name,
                        "designation": emp.designation,
                        "email": emp.email
                    }
                    for emp in team
                ]
            
            return []
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return []
    
    def answer_question(self, question: str) -> Dict[str, Any]:
        """
        Use OpenAI to understand the question and generate an appropriate response.
        """
        try:
            # Get database context
            db_context = self._get_database_context()
            
            # System prompt with instructions - MUST include the word "json" when using json response format
            system_prompt = f"""You are an AI assistant that helps answer questions about employee data.
You have access to a database with the following information:

{db_context}

When answering questions:
1. First determine what data needs to be queried
2. Generate appropriate query parameters
3. Use the query results to formulate a natural language answer
4. Be specific and provide relevant details
5. If the data doesn't exist, say so clearly

Available query types:
- get_employees: Get employee information with optional filters (name, designation, active, blocked)
- get_goals: Get goals with filters (employee_id, status, priority)
- get_employee_with_goals: Get specific employee with their goals
- get_pending_goals: Get all pending or in-progress goals
- get_projects: Get projects with filters (status)
- get_team_members: Get team members reporting to a manager

Important: When searching for a specific person by name, use get_employees with name filter.

You MUST respond with a valid JSON object in this format:
{{
  "query_type": "query type to execute",
  "parameters": {{"filter parameters"}},
  "explanation": "brief explanation of what you'll query"
}}
"""
            
            # First API call - understand the question and generate query
            response1 = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            query_plan = json.loads(response1.choices[0].message.content)
            logger.info(f"Query plan: {query_plan}")
            
            # Execute the query
            query_type = query_plan.get("query_type")
            parameters = query_plan.get("parameters", {})
            query_results = self._execute_query(query_type, parameters)
            
            # Second API call - generate natural language answer
            answer_prompt = f"""Based on the following query results, provide a clear and helpful answer to the user's question.

Original Question: {question}

Query Results:
{json.dumps(query_results, indent=2)}

Provide a natural, conversational answer that directly addresses the question.
Include specific details from the data.
If there are no results, explain that clearly.
Format the response in a readable way."""
            
            response2 = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant providing clear answers about employee data."},
                    {"role": "user", "content": answer_prompt}
                ],
                temperature=0.7
            )
            
            answer = response2.choices[0].message.content
            
            return {
                "success": True,
                "question": question,
                "answer": answer,
                "query_type": query_type,
                "data_points": len(query_results),
                "raw_data": query_results if len(query_results) <= 10 else query_results[:10]
            }
            
        except Exception as e:
            logger.error(f"Error answering question: {str(e)}")
            return {
                "success": False,
                "question": question,
                "answer": f"I encountered an error while processing your question: {str(e)}",
                "error": str(e)
            }