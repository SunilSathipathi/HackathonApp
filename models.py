from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Employee(Base):
    """Employee model representing employee data from Mendix."""
    __tablename__ = "employees"
    
    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(String(50), unique=True, index=True, nullable=False)
    full_name = Column(String(200), nullable=False)
    email = Column(String(200), unique=True, index=True, nullable=False)
    designation = Column(String(100))
    salary = Column(Float, default=0.0)
    manager_employee_id = Column(String(50), index=True)
    blocked = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    last_login = Column(DateTime, nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    changed_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    goals = relationship("Goal", back_populates="employee", cascade="all, delete-orphan")
    projects = relationship("EmployeeProject", back_populates="employee", cascade="all, delete-orphan")
    skills = relationship("EmployeeSkill", back_populates="employee", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Employee(employee_id='{self.employee_id}', name='{self.full_name}')>"


class Department(Base):
    """Department model."""
    __tablename__ = "departments"
    
    id = Column(Integer, primary_key=True, index=True)
    department_id = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    head_employee_id = Column(String(50), nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    changed_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Department(name='{self.name}')>"


class Goal(Base):
    """Employee goals model."""
    __tablename__ = "goals"
    
    id = Column(Integer, primary_key=True, index=True)
    goal_id = Column(String(50), unique=True, index=True, nullable=False)
    employee_id = Column(String(50), ForeignKey("employees.employee_id"), nullable=False)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    target_date = Column(DateTime)
    status = Column(String(50), default="Pending")  # Pending, In Progress, Completed, Cancelled
    progress_percentage = Column(Float, default=0.0)
    priority = Column(String(20), default="Medium")  # Low, Medium, High
    category = Column(String(100))  # Performance, Development, etc.
    created_date = Column(DateTime, default=datetime.utcnow)
    changed_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    employee = relationship("Employee", back_populates="goals")
    
    def __repr__(self):
        return f"<Goal(title='{self.title}', status='{self.status}')>"


class Project(Base):
    """Projects model."""
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    status = Column(String(50), default="Active")  # Active, On Hold, Completed, Cancelled
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    client_name = Column(String(200))
    project_type = Column(String(100))
    created_date = Column(DateTime, default=datetime.utcnow)
    changed_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    employees = relationship("EmployeeProject", back_populates="project", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Project(name='{self.name}', status='{self.status}')>"


class EmployeeProject(Base):
    """Many-to-many relationship between employees and projects."""
    __tablename__ = "employee_projects"
    
    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(String(50), ForeignKey("employees.employee_id"), nullable=False)
    project_id = Column(String(50), ForeignKey("projects.project_id"), nullable=False)
    role = Column(String(100))  # Developer, Lead, Manager, etc.
    allocation_percentage = Column(Float, default=100.0)
    start_date = Column(DateTime)
    end_date = Column(DateTime, nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    employee = relationship("Employee", back_populates="projects")
    project = relationship("Project", back_populates="employees")
    
    def __repr__(self):
        return f"<EmployeeProject(employee_id='{self.employee_id}', project_id='{self.project_id}')>"


class Skill(Base):
    """Skills master data."""
    __tablename__ = "skills"
    
    id = Column(Integer, primary_key=True, index=True)
    skill_id = Column(String(50), unique=True, index=True, nullable=False)
    name = Column(String(200), nullable=False, unique=True)
    category = Column(String(100))  # Technical, Soft Skills, Domain Knowledge, etc.
    description = Column(Text)
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    employees = relationship("EmployeeSkill", back_populates="skill", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Skill(name='{self.name}', category='{self.category}')>"


class EmployeeSkill(Base):
    """Many-to-many relationship between employees and skills."""
    __tablename__ = "employee_skills"
    
    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(String(50), ForeignKey("employees.employee_id"), nullable=False)
    skill_id = Column(String(50), ForeignKey("skills.skill_id"), nullable=False)
    proficiency_level = Column(String(50), default="Beginner")  # Beginner, Intermediate, Advanced, Expert
    years_of_experience = Column(Float, default=0.0)
    last_used = Column(DateTime, nullable=True)
    certified = Column(Boolean, default=False)
    created_date = Column(DateTime, default=datetime.utcnow)
    changed_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    employee = relationship("Employee", back_populates="skills")
    skill = relationship("Skill", back_populates="employees")
    
    def __repr__(self):
        return f"<EmployeeSkill(employee_id='{self.employee_id}', skill_id='{self.skill_id}')>"


class SyncLog(Base):
    """Log table to track sync operations."""
    __tablename__ = "sync_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    sync_type = Column(String(50), nullable=False)  # employees, goals, projects, skills
    status = Column(String(20), nullable=False)  # success, failed, partial
    records_synced = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    sync_started_at = Column(DateTime, default=datetime.utcnow)
    sync_completed_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<SyncLog(sync_type='{self.sync_type}', status='{self.status}')>"