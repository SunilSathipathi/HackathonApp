from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Dict
import logging
from mendix_client import MendixAPIClient
from models import (
    Employee, Department, Goal, Project, EmployeeProject,
    Skill, EmployeeSkill, SyncLog
)

logger = logging.getLogger(__name__)


class SyncService:
    """Service for synchronizing data from Mendix API to local database."""
    
    def __init__(self, db: Session):
        self.db = db
        self.api_client = MendixAPIClient()
    
    def _log_sync(self, sync_type: str, status: str, records_synced: int, 
                  error_message: str = None, start_time: datetime = None):
        """Log sync operation to database."""
        try:
            sync_log = SyncLog(
                sync_type=sync_type,
                status=status,
                records_synced=records_synced,
                error_message=error_message,
                sync_started_at=start_time or datetime.utcnow(),
                sync_completed_at=datetime.utcnow()
            )
            self.db.add(sync_log)
            self.db.commit()
        except Exception as e:
            logger.error(f"Error logging sync: {str(e)}")
            self.db.rollback()
    
    def sync_employees(self) -> int:
        """Sync employees from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting employee sync...")
            employees_data = self.api_client.get_employees()
            
            for emp_data in employees_data:
                try:
                    # Clean employee_id (remove extra spaces)
                    employee_id = emp_data.get("EmployeeID", "").strip()
                    if not employee_id:
                        continue
                    
                    account = emp_data.get("Account", {})
                    
                    # Check if employee exists
                    employee = self.db.query(Employee).filter(
                        Employee.employee_id == employee_id
                    ).first()
                    
                    if employee:
                        # Update existing employee
                        employee.full_name = account.get("FullName", "")
                        employee.email = account.get("Email", "")
                        employee.designation = emp_data.get("Designation", "")
                        employee.salary = emp_data.get("Salary", 0.0)
                        employee.manager_employee_id = emp_data.get("ManagerEmployeeID", "")
                        employee.blocked = account.get("Blocked", False)
                        employee.active = account.get("Active", True)
                        
                        # Parse dates
                        if account.get("LastLogin"):
                            try:
                                employee.last_login = datetime.fromisoformat(
                                    account["LastLogin"].replace("Z", "+00:00")
                                )
                            except:
                                pass
                        
                        employee.changed_date = datetime.utcnow()
                    else:
                        # Create new employee
                        last_login = None
                        if account.get("LastLogin"):
                            try:
                                last_login = datetime.fromisoformat(
                                    account["LastLogin"].replace("Z", "+00:00")
                                )
                            except:
                                pass
                        
                        employee = Employee(
                            employee_id=employee_id,
                            full_name=account.get("FullName", ""),
                            email=account.get("Email", ""),
                            designation=emp_data.get("Designation", ""),
                            salary=emp_data.get("Salary", 0.0),
                            manager_employee_id=emp_data.get("ManagerEmployeeID", ""),
                            blocked=account.get("Blocked", False),
                            active=account.get("Active", True),
                            last_login=last_login
                        )
                        self.db.add(employee)
                    
                    records_synced += 1
                    
                except Exception as e:
                    logger.error(f"Error processing employee {emp_data.get('EmployeeID')}: {str(e)}")
                    continue
            
            self.db.commit()
            logger.info(f"Successfully synced {records_synced} employees")
            self._log_sync("employees", "success", records_synced, start_time=start_time)
            return records_synced
            
        except Exception as e:
            logger.error(f"Error syncing employees: {str(e)}")
            self.db.rollback()
            self._log_sync("employees", "failed", records_synced, str(e), start_time)
            return 0
    
    def sync_departments(self) -> int:
        """Sync departments from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting department sync...")
            departments_data = self.api_client.get_departments()
            
            for dept_data in departments_data:
                try:
                    department_id = dept_data.get("DepartmentID", "").strip()
                    if not department_id:
                        continue
                    
                    department = self.db.query(Department).filter(
                        Department.department_id == department_id
                    ).first()
                    
                    if department:
                        department.name = dept_data.get("Name", "")
                        department.description = dept_data.get("Description", "")
                        department.head_employee_id = dept_data.get("HeadEmployeeID", "")
                        department.changed_date = datetime.utcnow()
                    else:
                        department = Department(
                            department_id=department_id,
                            name=dept_data.get("Name", ""),
                            description=dept_data.get("Description", ""),
                            head_employee_id=dept_data.get("HeadEmployeeID", "")
                        )
                        self.db.add(department)
                    
                    records_synced += 1
                    
                except Exception as e:
                    logger.error(f"Error processing department {dept_data.get('DepartmentID')}: {str(e)}")
                    continue
            
            self.db.commit()
            logger.info(f"Successfully synced {records_synced} departments")
            self._log_sync("departments", "success", records_synced, start_time=start_time)
            return records_synced
            
        except Exception as e:
            logger.error(f"Error syncing departments: {str(e)}")
            self.db.rollback()
            self._log_sync("departments", "failed", records_synced, str(e), start_time)
            return 0
    
    def sync_goals(self) -> int:
        """Sync employee goals from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting goals sync...")
            goals_data = self.api_client.get_goals()
            
            for goal_data in goals_data:
                try:
                    goal_id = goal_data.get("GoalID", "").strip()
                    if not goal_id:
                        continue
                    
                    goal = self.db.query(Goal).filter(Goal.goal_id == goal_id).first()
                    
                    # Parse target date
                    target_date = None
                    if goal_data.get("TargetDate"):
                        try:
                            target_date = datetime.fromisoformat(
                                goal_data["TargetDate"].replace("Z", "+00:00")
                            )
                        except:
                            pass
                    
                    if goal:
                        goal.employee_id = goal_data.get("EmployeeID", "")
                        goal.title = goal_data.get("Title", "")
                        goal.description = goal_data.get("Description", "")
                        goal.target_date = target_date
                        goal.status = goal_data.get("Status", "Pending")
                        goal.progress_percentage = goal_data.get("ProgressPercentage", 0.0)
                        goal.priority = goal_data.get("Priority", "Medium")
                        goal.category = goal_data.get("Category", "")
                        goal.changed_date = datetime.utcnow()
                    else:
                        goal = Goal(
                            goal_id=goal_id,
                            employee_id=goal_data.get("EmployeeID", ""),
                            title=goal_data.get("Title", ""),
                            description=goal_data.get("Description", ""),
                            target_date=target_date,
                            status=goal_data.get("Status", "Pending"),
                            progress_percentage=goal_data.get("ProgressPercentage", 0.0),
                            priority=goal_data.get("Priority", "Medium"),
                            category=goal_data.get("Category", "")
                        )
                        self.db.add(goal)
                    
                    records_synced += 1
                    
                except Exception as e:
                    logger.error(f"Error processing goal {goal_data.get('GoalID')}: {str(e)}")
                    continue
            
            self.db.commit()
            logger.info(f"Successfully synced {records_synced} goals")
            self._log_sync("goals", "success", records_synced, start_time=start_time)
            return records_synced
            
        except Exception as e:
            logger.error(f"Error syncing goals: {str(e)}")
            self.db.rollback()
            self._log_sync("goals", "failed", records_synced, str(e), start_time)
            return 0
    
    def sync_projects(self) -> int:
        """Sync projects from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting projects sync...")
            projects_data = self.api_client.get_projects()
            
            for proj_data in projects_data:
                try:
                    project_id = proj_data.get("ProjectID", "").strip()
                    if not project_id:
                        continue
                    
                    project = self.db.query(Project).filter(
                        Project.project_id == project_id
                    ).first()
                    
                    # Parse dates
                    start_date = None
                    end_date = None
                    
                    if proj_data.get("StartDate"):
                        try:
                            start_date = datetime.fromisoformat(
                                proj_data["StartDate"].replace("Z", "+00:00")
                            )
                        except:
                            pass
                    
                    if proj_data.get("EndDate"):
                        try:
                            end_date = datetime.fromisoformat(
                                proj_data["EndDate"].replace("Z", "+00:00")
                            )
                        except:
                            pass
                    
                    if project:
                        project.name = proj_data.get("Name", "")
                        project.description = proj_data.get("Description", "")
                        project.status = proj_data.get("Status", "Active")
                        project.start_date = start_date
                        project.end_date = end_date
                        project.client_name = proj_data.get("ClientName", "")
                        project.project_type = proj_data.get("ProjectType", "")
                        project.changed_date = datetime.utcnow()
                    else:
                        project = Project(
                            project_id=project_id,
                            name=proj_data.get("Name", ""),
                            description=proj_data.get("Description", ""),
                            status=proj_data.get("Status", "Active"),
                            start_date=start_date,
                            end_date=end_date,
                            client_name=proj_data.get("ClientName", ""),
                            project_type=proj_data.get("ProjectType", "")
                        )
                        self.db.add(project)
                    
                    records_synced += 1
                    
                except Exception as e:
                    logger.error(f"Error processing project {proj_data.get('ProjectID')}: {str(e)}")
                    continue
            
            self.db.commit()
            logger.info(f"Successfully synced {records_synced} projects")
            self._log_sync("projects", "success", records_synced, start_time=start_time)
            return records_synced
            
        except Exception as e:
            logger.error(f"Error syncing projects: {str(e)}")
            self.db.rollback()
            self._log_sync("projects", "failed", records_synced, str(e), start_time)
            return 0
    
    def sync_skills(self) -> int:
        """Sync skills from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting skills sync...")
            skills_data = self.api_client.get_skills()
            
            for skill_data in skills_data:
                try:
                    skill_id = skill_data.get("SkillID", "").strip()
                    if not skill_id:
                        continue
                    
                    skill = self.db.query(Skill).filter(Skill.skill_id == skill_id).first()
                    
                    if skill:
                        skill.name = skill_data.get("Name", "")
                        skill.category = skill_data.get("Category", "")
                        skill.description = skill_data.get("Description", "")
                    else:
                        skill = Skill(
                            skill_id=skill_id,
                            name=skill_data.get("Name", ""),
                            category=skill_data.get("Category", ""),
                            description=skill_data.get("Description", "")
                        )
                        self.db.add(skill)
                    
                    records_synced += 1
                    
                except Exception as e:
                    logger.error(f"Error processing skill {skill_data.get('SkillID')}: {str(e)}")
                    continue
            
            self.db.commit()
            logger.info(f"Successfully synced {records_synced} skills")
            self._log_sync("skills", "success", records_synced, start_time=start_time)
            return records_synced
            
        except Exception as e:
            logger.error(f"Error syncing skills: {str(e)}")
            self.db.rollback()
            self._log_sync("skills", "failed", records_synced, str(e), start_time)
            return 0
    
    def sync_all(self) -> Dict[str, int]:
        """Sync all data from Mendix API."""
        logger.info("Starting full data synchronization...")
        
        results = {
            "employees": self.sync_employees(),
            "departments": self.sync_departments(),
            "goals": self.sync_goals(),
            "projects": self.sync_projects(),
            "skills": self.sync_skills()
        }
        
        logger.info(f"Full synchronization completed: {results}")
        return results