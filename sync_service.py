from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
import logging
from mendix_client import MendixAPIClient
from models import (
    Employee, Department, Goal, Project, EmployeeProject,
    Skill, EmployeeSkill, SyncLog
)
from config import settings

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
            # Resolve the authenticated user's employee_id (used for fallbacks)
            self_emp: Optional[Employee] = self.db.query(Employee).filter(
                Employee.email == settings.mendix_api_username
            ).first()
            self_emp_id: Optional[str] = self_emp.employee_id if self_emp else None
            
            for goal_data in goals_data:
                try:
                    # Robust goal_id resolution with deterministic fallback
                    raw_goal_id = str(
                        goal_data.get("GoalID")
                        or goal_data.get("GoalId")
                        or goal_data.get("Id")
                        or goal_data.get("ID")
                        or ""
                    ).strip()
                    if raw_goal_id:
                        goal_id = raw_goal_id
                    else:
                        # Build a stable synthetic ID based on key fields
                        title = (goal_data.get("Title") or "").strip()
                        due_raw = goal_data.get("TargetDate") or goal_data.get("DueDate") or goal_data.get("EndDate") or ""
                        owner_emp = (goal_data.get("EmployeeID") or self_emp_id or "").strip()
                        key = f"{owner_emp}|{title}|{due_raw}"
                        goal_id = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
                    
                    goal = self.db.query(Goal).filter(Goal.goal_id == goal_id).first()
                    
                    # Parse target/due date
                    target_date = None
                    iso_date = goal_data.get("TargetDate") or goal_data.get("DueDate") or None
                    if iso_date:
                        try:
                            target_date = datetime.fromisoformat(str(iso_date).replace("Z", "+00:00"))
                        except Exception:
                            target_date = None

                    # Parse start date
                    start_date = None
                    start_iso = goal_data.get("StartDate") or None
                    if start_iso:
                        try:
                            start_date = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
                        except Exception:
                            start_date = None

                    # Parse assigned date
                    assigned_date = None
                    assigned_iso = goal_data.get("AssignedDate") or None
                    if assigned_iso:
                        try:
                            assigned_date = datetime.fromisoformat(str(assigned_iso).replace("Z", "+00:00"))
                        except Exception:
                            assigned_date = None

                    # Assigned-to / Assigned-by employee IDs from nested objects
                    assigned_to_id = None
                    assigned_by_id = None
                    try:
                        assigned_to = goal_data.get("GoalAssignedTo") or {}
                        assigned_to_id = (assigned_to.get("EmployeeID") or "").strip() or None
                    except Exception:
                        assigned_to_id = None
                    try:
                        assigned_by = goal_data.get("GoalAssignedBy") or {}
                        assigned_by_id = (assigned_by.get("EmployeeID") or "").strip() or None
                    except Exception:
                        assigned_by_id = None
                    # Fallbacks using MySelf flag and authenticated user
                    myself = goal_data.get("MySelf")
                    if self_emp_id:
                        if myself is True:
                            assigned_to_id = assigned_to_id or self_emp_id
                            assigned_by_id = assigned_by_id or self_emp_id
                        elif myself is False:
                            assigned_to_id = assigned_to_id or self_emp_id
                    
                    # Resolve owner employee_id with fallback to authenticated user if missing
                    owner_employee_id = (goal_data.get("EmployeeID") or "").strip() or self_emp_id or ""

                    # Priority via weightage bucketing if provided
                    weightage = goal_data.get("Weightage")
                    priority_val = None
                    try:
                        if isinstance(weightage, (int, float)):
                            if weightage >= 67:
                                priority_val = "High"
                            elif weightage <= 33:
                                priority_val = "Low"
                            else:
                                priority_val = "Medium"
                    except Exception:
                        priority_val = None
                    if not priority_val:
                        priority_val = goal_data.get("Priority", "Medium")

                    # Build description from Description field only
                    description_text = (goal_data.get("Description") or "").strip()
                    # Keep measurement criteria separate
                    measurement_criteria = (goal_data.get("MeasurementCriteria") or "").strip()

                    if goal:
                        goal.employee_id = owner_employee_id
                        goal.title = goal_data.get("Title", "")
                        goal.description = description_text
                        goal.target_date = target_date
                        goal.start_date = start_date
                        # Support both legacy keys and new keys from payload
                        goal.status = goal_data.get("Status") or goal_data.get("GoalStatus") or "Pending"
                        goal.progress_percentage = goal_data.get("ProgressPercentage", 0.0)
                        goal.priority = priority_val
                        goal.category = goal_data.get("Category") or goal_data.get("GoalCategory") or ""
                        # New relationships
                        goal.assigned_to_employee_id = assigned_to_id
                        goal.assigned_by_employee_id = assigned_by_id
                        # All new comprehensive fields
                        goal.weightage = weightage
                        goal.measurement_criteria = measurement_criteria
                        goal.is_smart = goal_data.get("IsSMART", False)
                        goal.progress = goal_data.get("Progress", 0)
                        goal.assigned_date = assigned_date
                        goal.myself_required = goal_data.get("MySelfRequired", False)
                        goal.file_id = goal_data.get("FileID")
                        goal.delete_after_download = goal_data.get("DeleteAfterDownload", False)
                        goal.has_contents = goal_data.get("HasContents", False)
                        goal.size = goal_data.get("Size", -1)
                        goal.changed_date = datetime.utcnow()
                    else:
                        goal = Goal(
                            goal_id=goal_id,
                            employee_id=owner_employee_id or "",
                            title=goal_data.get("Title", ""),
                            description=description_text,
                            target_date=target_date,
                            start_date=start_date,
                            status=goal_data.get("Status") or goal_data.get("GoalStatus") or "Pending",
                            progress_percentage=goal_data.get("ProgressPercentage", 0.0),
                            priority=priority_val,
                            category=goal_data.get("Category") or goal_data.get("GoalCategory") or "",
                            assigned_to_employee_id=assigned_to_id,
                            assigned_by_employee_id=assigned_by_id,
                            # All new comprehensive fields
                            weightage=weightage,
                            measurement_criteria=measurement_criteria,
                            is_smart=goal_data.get("IsSMART", False),
                            progress=goal_data.get("Progress", 0),
                            assigned_date=assigned_date,
                            myself_required=goal_data.get("MySelfRequired", False),
                            file_id=goal_data.get("FileID"),
                            delete_after_download=goal_data.get("DeleteAfterDownload", False),
                            has_contents=goal_data.get("HasContents", False),
                            size=goal_data.get("Size", -1)
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
                    project_id = str(proj_data.get("ProjectID", "")).strip()
                    if not project_id:
                        continue

                    project = self.db.query(Project).filter(Project.project_id == project_id).first()

                    # Safe ISO date parser
                    def parse_date(date_str):
                        try:
                            return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None
                        except Exception:
                            return None

                    start_date = parse_date(proj_data.get("StartDate"))
                    end_date = parse_date(proj_data.get("EndDate"))
                    created_date = parse_date(proj_data.get("createdDate"))
                    changed_date = parse_date(proj_data.get("changedDate"))

                    # Extract manager info (nested)
                    manager_data = proj_data.get("Manager", {})
                    manager_employee_id = manager_data.get("EmployeeID")

                    if project:
                        project.name = proj_data.get("ProjectName", "")
                        project.project_manager = proj_data.get("ProjectManager", "")
                        project.description = proj_data.get("Description", "")
                        project.manager_employee_id = manager_employee_id
                        project.start_date = start_date
                        project.end_date = end_date
                        project.created_date = created_date
                        project.changed_date = changed_date
                    else:
                        project = Project(
                            project_id=project_id,
                            name=proj_data.get("ProjectName", ""),
                            project_manager=proj_data.get("ProjectManager", ""),
                            description=proj_data.get("Description", ""),
                            manager_employee_id=manager_employee_id,
                            start_date=start_date,
                            end_date=end_date,
                            created_date=created_date,
                            changed_date=changed_date,
                        )
                        self.db.add(project)

                    # Commit once per project so we can safely create relations
                    self.db.commit()

                    # Handle Employees list (many-to-many)
                    employee_links = proj_data.get("Employees", [])
                    for emp in employee_links:
                        emp_id = emp.get("EmployeeID")
                        if emp_id:
                            link_exists = self.db.query(EmployeeProject).filter_by(
                                project_id=project.project_id,
                                employee_id=emp_id
                            ).first()
                            if not link_exists:
                                self.db.add(EmployeeProject(
                                    project_id=project.project_id,
                                    employee_id=emp_id
                                ))

                    records_synced += 1

                except Exception as e:
                    logger.error(f"Error processing project {proj_data.get('ProjectID')}: {str(e)}")
                    self.db.rollback()
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

    # def sync_projects(self) -> int:
    #     """Sync projects from Mendix API."""
    #     start_time = datetime.utcnow()
    #     records_synced = 0
        
    #     try:
    #         logger.info("Starting projects sync...")
    #         projects_data = self.api_client.get_projects()
            
    #         for proj_data in projects_data:
    #             try:
    #                 project_id = proj_data.get("ProjectID", "").strip()
    #                 if not project_id:
    #                     continue
                    
    #                 project = self.db.query(Project).filter(
    #                     Project.project_id == project_id
    #                 ).first()
                    
    #                 # Parse dates
    #                 start_date = None
    #                 end_date = None
                    
    #                 if proj_data.get("StartDate"):
    #                     try:
    #                         start_date = datetime.fromisoformat(
    #                             proj_data["StartDate"].replace("Z", "+00:00")
    #                         )
    #                     except:
    #                         pass
                    
    #                 if proj_data.get("EndDate"):
    #                     try:
    #                         end_date = datetime.fromisoformat(
    #                             proj_data["EndDate"].replace("Z", "+00:00")
    #                         )
    #                     except:
    #                         pass
                    
    #                 if project:
    #                     project.name = proj_data.get("Name", "")
    #                     project.description = proj_data.get("Description", "")
    #                     project.status = proj_data.get("Status", "Active")
    #                     project.start_date = start_date
    #                     project.end_date = end_date
    #                     project.client_name = proj_data.get("ClientName", "")
    #                     project.project_type = proj_data.get("ProjectType", "")
    #                     project.changed_date = datetime.utcnow()
    #                 else:
    #                     project = Project(
    #                         project_id=project_id,
    #                         name=proj_data.get("Name", ""),
    #                         description=proj_data.get("Description", ""),
    #                         status=proj_data.get("Status", "Active"),
    #                         start_date=start_date,
    #                         end_date=end_date,
    #                         client_name=proj_data.get("ClientName", ""),
    #                         project_type=proj_data.get("ProjectType", "")
    #                     )
    #                     self.db.add(project)
                    
    #                 records_synced += 1
                    
    #             except Exception as e:
    #                 logger.error(f"Error processing project {proj_data.get('ProjectID')}: {str(e)}")
    #                 continue
            
    #         self.db.commit()
    #         logger.info(f"Successfully synced {records_synced} projects")
    #         self._log_sync("projects", "success", records_synced, start_time=start_time)
    #         return records_synced
            
    #     except Exception as e:
    #         logger.error(f"Error syncing projects: {str(e)}")
    #         self.db.rollback()
    #         self._log_sync("projects", "failed", records_synced, str(e), start_time)
    #         return 0
    
    def sync_skills(self) -> int:
        """Sync skills from Mendix API."""
        start_time = datetime.utcnow()
        records_synced = 0
        
        try:
            logger.info("Starting skills sync...")
            skills_data = self.api_client.get_skills()
            
            for skill_data in skills_data:
                try:
                    # Map IDs and names from Mendix payload
                    raw_skill_id = skill_data.get("SkillID")
                    skill_id = str(raw_skill_id).strip() if raw_skill_id is not None else ""
                    if not skill_id:
                        continue
                    
                    skill = self.db.query(Skill).filter(Skill.skill_id == skill_id).first()
                    
                    if skill:
                        skill.name = skill_data.get("SkillName") or skill_data.get("Name", "")
                        skill.category = skill_data.get("Category", "")
                        skill.description = skill_data.get("Description", "")
                        # Parse and update created/changed dates if provided
                        try:
                            if skill_data.get("createdDate"):
                                skill.created_date = datetime.fromisoformat(str(skill_data["createdDate"]).replace("Z", "+00:00"))
                        except Exception:
                            pass
                        try:
                            if skill_data.get("changedDate"):
                                skill.changed_date = datetime.fromisoformat(str(skill_data["changedDate"]).replace("Z", "+00:00"))
                        except Exception:
                            pass
                    else:
                        skill = Skill(
                            skill_id=skill_id,
                            name=skill_data.get("SkillName") or skill_data.get("Name", ""),
                            category=skill_data.get("Category", ""),
                            description=skill_data.get("Description", "")
                        )
                        # Parse and set created/changed dates if provided
                        try:
                            if skill_data.get("createdDate"):
                                skill.created_date = datetime.fromisoformat(str(skill_data["createdDate"]).replace("Z", "+00:00"))
                        except Exception:
                            pass
                        try:
                            if skill_data.get("changedDate"):
                                skill.changed_date = datetime.fromisoformat(str(skill_data["changedDate"]).replace("Z", "+00:00"))
                        except Exception:
                            pass
                        self.db.add(skill)
                    
                    # Many-to-many: create EmployeeSkill relations from Employees list
                    employees = skill_data.get("Employees") or []
                    for emp in employees:
                        emp_id = (emp.get("EmployeeID") or "").strip()
                        if not emp_id:
                            continue
                        # Ensure employee exists
                        employee = self.db.query(Employee).filter(Employee.employee_id == emp_id).first()
                        if not employee:
                            logger.warning(f"Employee '{emp_id}' not found for skill '{skill_id}'")
                            continue
                        # Check if relation already exists
                        existing_rel = (
                            self.db.query(EmployeeSkill)
                            .filter(
                                EmployeeSkill.employee_id == emp_id,
                                EmployeeSkill.skill_id == skill_id,
                            )
                            .first()
                        )
                        if not existing_rel:
                            rel = EmployeeSkill(
                                employee_id=emp_id,
                                skill_id=skill_id,
                            )
                            self.db.add(rel)
                    
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