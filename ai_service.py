from openai import OpenAI
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text
from typing import Dict, List, Any, Optional
import json
import re
from unidecode import unidecode
try:
    from rapidfuzz import process as rf_process, fuzz as rf_fuzz
except Exception:
    rf_process = None
    rf_fuzz = None
import logging
from config import settings
from models import Employee, Department, Goal, Project, Skill, AIQueryLog
from database import engine
from vector_engine import VectorEngine
from offline_queries import OfflineQueryService
from query_router import QueryRouter

logger = logging.getLogger(__name__)


class AIQueryService:
    """Service for answering questions about employee data using OpenAI."""
    
    def __init__(self, db: Session):
        self.db = db
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model  # configurable model
    
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
                manager_id = parameters.get("manager_id") or parameters.get("manager_employee_id")
                team: List[Employee] = []

                if manager_id:
                    if str(manager_id).strip().upper().startswith("LCL"):
                        team = self.db.query(Employee).filter(
                            Employee.manager_employee_id == manager_id
                        ).all()
                    else:
                        candidates = self.db.query(Employee).filter(
                            Employee.full_name.ilike(f"%{manager_id}%")
                        ).all()
                        candidate_ids = [c.employee_id for c in candidates]
                        if candidate_ids:
                            team = self.db.query(Employee).filter(
                                Employee.manager_employee_id.in_(candidate_ids)
                            ).all()
                else:
                    manager_name = (
                        parameters.get("manager_name")
                        or parameters.get("name")
                        or parameters.get("manager")
                        or parameters.get("manager_full_name")
                    )
                    if manager_name:
                        candidates = self.db.query(Employee).filter(
                            Employee.full_name.ilike(f"%{manager_name}%")
                        ).all()
                        candidate_ids = [c.employee_id for c in candidates]
                        if candidate_ids:
                            team = self.db.query(Employee).filter(
                                Employee.manager_employee_id.in_(candidate_ids)
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


class DynamicAIQueryService:
    """
    Fully dynamic AI Query Service:
    - Introspects SQLAlchemy schema automatically
    - Classifies question (sql/semantic/hybrid)
    - Generates safe, parameterized SQL via GPT
    - Executes SQL and/or vector search, composes an answer via GPT
    - Logs every query and response
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        self.router = QueryRouter()
        self.vectors = VectorEngine()

    def _schema_introspection(self) -> Dict[str, Any]:
        insp = inspect(engine)
        schema: Dict[str, Any] = {"tables": {}}
        for table_name in insp.get_table_names():
            cols = insp.get_columns(table_name)
            fks = insp.get_foreign_keys(table_name)
            schema["tables"][table_name] = {
                "columns": [c["name"] for c in cols],
                "foreign_keys": [
                    {
                        "constrained_columns": fk.get("constrained_columns"),
                        "referred_table": fk.get("referred_table"),
                        "referred_columns": fk.get("referred_columns"),
                    }
                    for fk in fks
                ],
            }
            return schema

    def _schema_summary(self, schema: Dict[str, Any]) -> str:
        lines: List[str] = []
        for t, info in schema.get("tables", {}).items():
            cols = ", ".join(info.get("columns", []))
            fk_desc = "; ".join(
                [
                    f"FK({','.join(fk.get('constrained_columns', []))})"
                    f"->{fk.get('referred_table')}.{','.join(fk.get('referred_columns', []) or [])}"
                    for fk in info.get("foreign_keys", [])
                ]
            )
            lines.append(f"{t}: {cols}" + (f" | {fk_desc}" if fk_desc else ""))
        return "\n".join(lines)

    def _generate_sql(self, question: str, schema_summary: str) -> Dict[str, Any]:
        system = (
            "You generate SAFE, PARAMETERIZED SQL for a given question.\n"
            "Rules: Use only available tables/columns from the provided schema summary. Do not invent columns.\n"
            "Prefer joins via foreign keys listed; match constrained_columns to referred_table.referred_columns exactly.\n"
            "CRITICAL: Use business keys (employee_id, goal_id, project_id, skill_id, department_id) when joining, NOT integer id, unless the FK explicitly references id.\n"
            "Text columns MUST use case-insensitive matching (ILIKE) when filtering by names, roles, titles, or skill names. NEVER use LIKE for text matching - always use ILIKE.\n"
            "Employees table: use 'full_name' for person name; use 'designation' for job title. There is NO 'name' column on employees.\n"
            "Departments table uses 'name' for the department name.\n"
            "Manager relationship: employees.manager_employee_id references employees.employee_id. For 'reports to' queries, self-join employees e to employees r using e.manager_employee_id = r.employee_id. In these queries, SELECT subordinate fields from e (e.full_name, e.designation, e.employee_id) unless the question explicitly asks about the manager.\n"
            "Goals relationship mapping: If the question says 'assigned by <person>', filter goals.assigned_by_employee_id via a join to employees ON employees.employee_id = goals.assigned_by_employee_id. If the question says 'assigned to <person>', filter goals.assigned_to_employee_id via a join to employees ON employees.employee_id = goals.assigned_to_employee_id.\n"
            "When filtering by person names for goals, join the appropriate employees alias (e_by or e_to) and use e_by.full_name ILIKE :employee_name or e_to.full_name ILIKE :employee_name respectively.\n"
            "If the question provides an employee ID (e.g., 'LCL...'), for goals queries avoid name joins and filter directly: g.assigned_by_employee_id = :employee_id for 'assigned by', or g.assigned_to_employee_id = :employee_id for 'assigned to'.\n"
            "Skills relationship mapping: Skills table uses 'name' column (NOT skill_name). For 'who has X skills' queries, join employees -> employee_skills -> skills: FROM employees e JOIN employee_skills es ON e.employee_id = es.employee_id JOIN skills s ON es.skill_id = s.skill_id WHERE s.name ILIKE :skill_name. Always use ILIKE for skill name matching. Include proficiency_level, years_of_experience, certified from employee_skills if relevant.\n"
            "When using ILIKE, set parameter values with wildcards (e.g., :manager_name => '%rammohan%').\n"
            "If the question contains an employee ID (e.g., starts with 'LCL'), filter on r.employee_id = :manager_id instead of name matching. Also allow both filters when helpful.\n"
            "Always use named bind parameters like :param. Never interpolate raw text.\n"
            "Return JSON: {\"sql\": <string>, \"parameters\": <object>, \"notes\": <string>}\n"
        )
        user = (
            f"Schema:\n{schema_summary}\n\nQuestion: {question}\n"
            "Produce a SELECT query. Limit results to 50 where appropriate. For title/designation lookups (e.g., CEO), prefer ILIKE '%keyword%'."
        )
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.1,
        )
        return json.loads(resp.choices[0].message.content)

    def _regenerate_sql_with_feedback(self, question: str, schema_summary: str, previous_sql: str, error_message: str) -> Dict[str, Any]:
        """Ask the model to fix SQL using schema when execution fails, without regex, using structured guidance."""
        system = (
            "Previous SQL failed. Generate a corrected, SAFE, PARAMETERIZED SQL.\n"
            "Use ONLY tables/columns that exist in the schema summary.\n"
            "If a column is invalid, replace with the appropriate existing column (e.g., use employees.designation for job title).\n"
            "Prefer ILIKE for case-insensitive text filters.\n"
            "Always use named bind parameters like :param.\n"
            "Return JSON: {\"sql\": <string>, \"parameters\": <object>, \"notes\": <string>}\n"
        )
        user = (
            f"Schema:\n{schema_summary}\n\nQuestion: {question}\n\n"
            f"Previous SQL:\n{previous_sql}\n\nError: {error_message}\n\n"
            "Produce a corrected SELECT query. Limit results to 50 where appropriate."
        )
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        import json as _json
        return _json.loads(resp.choices[0].message.content)

    def _try_execute_sql(self, sql: str, params: Optional[Dict[str, Any]]) -> (List[Dict[str, Any]], Optional[str]):
        try:
            result = self.db.execute(text(sql), params or {})
            rows = result.mappings().all()
            return [dict(r) for r in rows], None
        except Exception as e:
            logging.error(f"SQL execution error: {e}")
            return [], str(e)

    def _ensure_like_wildcards(self, sql: str, params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Ensure parameters used with LIKE/ILIKE have %wildcards% for flexible matching.
        This avoids strict equality when users provide partial names like first names.
        No regex is used; we perform simple token scanning for ':param' after LIKE/ILIKE.
        """
        if not params or not sql:
            return params
        s = sql.lower()
        idx = 0
        like_params = []
        while True:
            # Find next occurrence of LIKE or ILIKE followed by ':'
            next_like = s.find(" like :", idx)
            next_ilike = s.find(" ilike :", idx)
            # pick the earliest occurrence
            candidates = [p for p in (next_like, next_ilike) if p != -1]
            if not candidates:
                break
            pos = min(candidates)
            # Move to start of param name
            pos += len(" like :") if pos == next_like else len(" ilike :")
            # Read param name characters (alnum or underscore)
            name_chars = []
            while pos < len(s):
                ch = sql[pos]  # use original case
                if ch.isalnum() or ch == "_":
                    name_chars.append(ch)
                    pos += 1
                else:
                    break
            if name_chars:
                like_params.append("".join(name_chars))
            idx = pos
        # Wrap values with % if not already present
        for p in like_params:
            val = params.get(p)
            if isinstance(val, str):
                trimmed = val.strip()
                if "%" not in trimmed:
                    params[p] = f"%{trimmed}%"
        return params

    def _ensure_case_insensitive_like(self, sql: str) -> str:
        """Ensure case-insensitive text matching across dialects.
        - For SQLite, replace ILIKE with LIKE (SQLite LIKE is case-insensitive by default for ASCII).
        - For Postgres/others, replace LIKE with ILIKE.
        """
        if not sql:
            return sql
        dialect = engine.dialect.name.lower()
        if "sqlite" in dialect:
            # Convert any ILIKE to LIKE for SQLite compatibility
            return re.sub(r"\bILIKE\b", "LIKE", sql, flags=re.IGNORECASE)
        # Default: prefer ILIKE for case-insensitive matching
        return re.sub(r"\bLIKE\b", "ILIKE", sql, flags=re.IGNORECASE)

    def _fallback_reporting_message(self, question: str, sql_text: Optional[str], sql_params: Optional[Dict[str, Any]]) -> Optional[str]:
        """If a reporting-to query returns no rows, check manager presence by name.
        - If manager exists but no reports, state that clearly with IDs.
        - If manager not found, suggest using employee_id or syncing data.
        - Additionally, attempt semantic (vector) name matching to find nearest manager candidates
          and check reports under those candidate manager IDs dynamically.
        - If still no direct reports, infer likely team members via projects
          (where the person is project manager) and departments (where the person
          is department head), so the user still gets a practical answer.
        """
        q = (question or "").lower()
        if "report" not in q:
            return None
        if not sql_params or "manager_name" not in sql_params:
            return None
        mgr_name = sql_params.get("manager_name")
        try:
            rows, err = self._try_execute_sql(
                "SELECT full_name, employee_id FROM employees WHERE full_name LIKE :manager_name LIMIT 5",
                {"manager_name": mgr_name},
            )
            if err is not None:
                return None
            if rows:
                names = ", ".join([f"{r.get('full_name')} ({r.get('employee_id')})" for r in rows])
                # No direct reports via exact/wildcard name; try semantic candidates
                semantic_msg = self._semantic_reports_fallback(mgr_name)
                if semantic_msg:
                    return semantic_msg
                # Try indirect team inference via projects/departments
                indirect_msg = self._indirect_team_fallback(mgr_name)
                if indirect_msg:
                    return indirect_msg
                return (
                    f"Found manager candidate(s): {names}. No direct reports are recorded. "
                    f"Verify subordinates have their manager_employee_id set to the manager's employee_id."
                )
            # No matching manager by name
            # Try semantic (vector) candidate search for near-miss names
            semantic_msg = self._semantic_reports_fallback(mgr_name)
            if semantic_msg:
                return semantic_msg
            # Try indirect team inference via projects/departments using name
            indirect_msg = self._indirect_team_fallback(mgr_name)
            if indirect_msg:
                return indirect_msg
            stripped = str(mgr_name).replace('%','')
            return (
                f"No employee found whose name contains '{stripped}'. "
                f"Try using the manager's employee ID (e.g., LCL...), or run POST /api/sync to refresh data."
            )
        except Exception:
            return None

    def _semantic_reports_fallback(self, manager_name: str) -> Optional[str]:
        """Use vector search to find nearest employee names for the manager and try fetching their reports.
        Returns a dynamic message listing candidate managers and any discovered direct reports.
        """
        try:
            if not self.vectors.enabled:
                return None
            # Ensure embeddings exist for semantic search
            self.vectors.upsert_all(self.db)
            candidates = self.vectors.search(str(manager_name).replace('%', ''), top_k=5)
            emp_candidates = [c for c in candidates if c.get("metadata", {}).get("type") == "employee"]
            if not emp_candidates:
                return None
            # Try fetching reports for each candidate manager_id
            reports: List[Dict[str, Any]] = []
            cand_strs: List[str] = []
            for c in emp_candidates:
                meta = c.get("metadata", {})
                cand_id = meta.get("employee_id")
                cand_name = meta.get("full_name") or c.get("text", "").split("|")[0].replace("Employee:", "").strip()
                if cand_id:
                    cand_strs.append(f"{cand_name} ({cand_id})")
                    rws, err = self._try_execute_sql(
                        "SELECT e.full_name, e.designation, e.employee_id FROM employees e WHERE e.manager_employee_id = :manager_id LIMIT 50",
                        {"manager_id": cand_id},
                    )
                    if not err and rws:
                        # Tag rows with the manager candidate for clarity
                        for row in rws:
                            reports.append({
                                "manager_name": cand_name,
                                "manager_id": cand_id,
                                **row
                            })
            if reports:
                # Compose a concise dynamic message listing discovered reports
                by_mgr: Dict[str, List[str]] = {}
                for r in reports:
                    key = f"{r.get('manager_name')} ({r.get('manager_id')})"
                    by_mgr.setdefault(key, []).append(f"{r.get('full_name')} [{r.get('employee_id')}] - {r.get('designation')}")
                parts = []
                for mgr_key, subs in by_mgr.items():
                    parts.append(f"Under {mgr_key}: " + "; ".join(subs))
                return "Found direct reports via semantic manager match. " + " | ".join(parts)
            # No reports found under semantic candidates; return candidates list to guide user
            if cand_strs:
                return (
                    f"Nearest manager name match(es): {', '.join(cand_strs)}. "
                    f"No direct reports recorded under these candidates. Verify manager_employee_id assignments or use the exact employee ID."
                )
            return None
        except Exception:
            return None

    def _indirect_team_fallback(self, manager_name: str) -> Optional[str]:
        """Infer team members via Projects and Departments when direct reports are missing.
        Heuristics:
        - Projects where candidate is project manager (by manager_employee_id or project_manager name)
        - Departments where candidate is department head (head_employee_id)
        Compose a concise message listing inferred team members.
        """
        try:
            stripped = str(manager_name).replace('%', '')
            # Gather candidate manager IDs by exact name LIKE
            cand_rows, err = self._try_execute_sql(
                "SELECT full_name, employee_id FROM employees WHERE full_name LIKE :name LIMIT 5",
                {"name": manager_name},
            )
            if err is not None:
                return None
            candidates = [(r.get("employee_id"), r.get("full_name")) for r in (cand_rows or []) if r.get("employee_id")]

            # Also use semantic vector search to widen candidate pool
            try:
                if self.vectors.enabled:
                    self.vectors.upsert_all(self.db)
                    vec_cands = self.vectors.search(stripped, top_k=5)
                    for c in vec_cands:
                        meta = c.get("metadata", {})
                        if meta.get("type") == "employee" and meta.get("employee_id"):
                            candidates.append((meta.get("employee_id"), meta.get("full_name") or stripped))
            except Exception:
                pass

            # Deduplicate candidates
            seen = set()
            uniq_candidates = []
            for cid, cname in candidates:
                if cid and cid not in seen:
                    uniq_candidates.append((cid, cname))
                    seen.add(cid)

            if not uniq_candidates and not stripped:
                return None

            project_members: Dict[str, List[str]] = {}
            dept_members: Dict[str, List[str]] = {}

            # Search projects by manager_employee_id and project_manager LIKE
            # First, by manager_employee_id for each candidate
            for cid, cname in uniq_candidates:
                proj_rows, perr = self._try_execute_sql(
                    "SELECT project_id, name FROM projects WHERE manager_employee_id = :mid LIMIT 10",
                    {"mid": cid},
                )
                if perr is None and proj_rows:
                    for p in proj_rows:
                        pid = p.get("project_id")
                        pname = p.get("name")
                        # Fetch team members for this project
                        mem_rows, merr = self._try_execute_sql(
                            "SELECT e.employee_id, e.full_name, e.designation FROM employee_projects ep JOIN employees e ON e.employee_id = ep.employee_id WHERE ep.project_id = :pid AND e.employee_id <> :mid",
                            {"pid": pid, "mid": cid},
                        )
                        if merr is None and mem_rows:
                            key = f"Project {pname} ({pid})"
                            project_members.setdefault(key, []).extend(
                                [f"{m.get('full_name')} [{m.get('employee_id')}] - {m.get('designation')}" for m in mem_rows]
                            )

            # Next, projects where project_manager name matches
            proj_by_name_rows, nerr = self._try_execute_sql(
                "SELECT project_id, name FROM projects WHERE project_manager LIKE :pname LIMIT 10",
                {"pname": manager_name},
            )
            if nerr is None and proj_by_name_rows:
                for p in proj_by_name_rows:
                    pid = p.get("project_id")
                    pname = p.get("name")
                    mem_rows, merr = self._try_execute_sql(
                        "SELECT e.employee_id, e.full_name, e.designation FROM employee_projects ep JOIN employees e ON e.employee_id = ep.employee_id WHERE ep.project_id = :pid",
                        {"pid": pid},
                    )
                    if merr is None and mem_rows:
                        key = f"Project {pname} ({pid})"
                        project_members.setdefault(key, []).extend(
                            [f"{m.get('full_name')} [{m.get('employee_id')}] - {m.get('designation')}" for m in mem_rows]
                        )

            # Search departments where candidate is head
            for cid, cname in uniq_candidates:
                dept_rows, derr = self._try_execute_sql(
                    "SELECT department_id, name FROM departments WHERE head_employee_id = :mid LIMIT 5",
                    {"mid": cid},
                )
                if derr is None and dept_rows:
                    for d in dept_rows:
                        did = d.get("department_id")
                        dname = d.get("name")
                        mem_rows, merr = self._try_execute_sql(
                            "SELECT employee_id, full_name, designation FROM employees WHERE department_id = :did AND employee_id <> :mid",
                            {"did": did, "mid": cid},
                        )
                        if merr is None and mem_rows:
                            key = f"Department {dname} ({did})"
                            dept_members.setdefault(key, []).extend(
                                [f"{m.get('full_name')} [{m.get('employee_id')}] - {m.get('designation')}" for m in mem_rows]
                            )

            # Compose message if we inferred any members
            parts = []
            if project_members:
                for ctx, members in project_members.items():
                    if members:
                        # Deduplicate within context
                        uniq = []
                        seen_m = set()
                        for m in members:
                            if m not in seen_m:
                                uniq.append(m)
                                seen_m.add(m)
                        parts.append(f"Under {ctx}: " + "; ".join(uniq))
            if dept_members:
                for ctx, members in dept_members.items():
                    if members:
                        uniq = []
                        seen_m = set()
                        for m in members:
                            if m not in seen_m:
                                uniq.append(m)
                                seen_m.add(m)
                        parts.append(f"Under {ctx}: " + "; ".join(uniq))

            if parts:
                return (
                    "No direct reports recorded. Inferred team via projects/departments. "
                    + " | ".join(parts)
                )
            return None
        except Exception:
            return None
    def _fuzzy_reports_fallback(self, manager_name: str) -> Optional[str]:
        """Use RapidFuzz to find nearest employee names and try fetching their reports.
        Returns a dynamic message listing candidate managers and any discovered direct reports.
        """
        try:
            if rf_process is None or rf_fuzz is None:
                return None
            # Normalize input: strip wildcards and accents, lowercase
            query = unidecode(str(manager_name).replace('%', '').strip().lower())
            # Pull all employees into memory for fuzzy match (acceptable for typical org sizes)
            employees = self.db.query(Employee.employee_id, Employee.full_name).all()
            names = [unidecode((e.full_name or '').strip().lower()) for e in employees]
            mapping = {unidecode((e.full_name or '').strip().lower()): e.employee_id for e in employees}
            # Extract top candidates using token_set_ratio for robustness to order and extra tokens
            extracted = rf_process.extract(query, names, scorer=rf_fuzz.token_set_ratio, limit=5)
            # Filter by a reasonable threshold
            candidates = [(name, score) for name, score, _ in extracted if score >= 75]
            if not candidates:
                return None
            reports: List[Dict[str, Any]] = []
            cand_strs: List[str] = []
            for name, score in candidates:
                cand_id = mapping.get(name)
                cand_strs.append(f"{name.title()} ({cand_id}) ~{score}")
                if cand_id:
                    rws, err = self._try_execute_sql(
                        "SELECT e.full_name, e.designation, e.employee_id FROM employees e WHERE e.manager_employee_id = :manager_id LIMIT 50",
                        {"manager_id": cand_id},
                    )
                    if not err and rws:
                        for row in rws:
                            reports.append({
                                "manager_name": name.title(),
                                "manager_id": cand_id,
                                **row
                            })
            if reports:
                by_mgr: Dict[str, List[str]] = {}
                for r in reports:
                    key = f"{r.get('manager_name')} ({r.get('manager_id')})"
                    by_mgr.setdefault(key, []).append(f"{r.get('full_name')} [{r.get('employee_id')}] - {r.get('designation')}")
                parts = []
                for mgr_key, subs in by_mgr.items():
                    parts.append(f"Under {mgr_key}: " + "; ".join(subs))
                return "Found direct reports via fuzzy manager match. " + " | ".join(parts)
            if cand_strs:
                return (
                    f"Nearest manager name match(es) by fuzzy search: {', '.join(cand_strs)}. "
                    f"No direct reports recorded under these candidates. Verify manager_employee_id assignments or use exact employee ID."
                )
            return None
        except Exception:
            return None

    def _compose_answer(self, question: str, sql_rows: List[Dict[str, Any]], semantic_docs: List[Dict[str, Any]]) -> str:
        sys_prompt = "You write clear answers citing specific data points."
        user_prompt = (
            f"Question: {question}\n\n"
            f"SQL Rows ({len(sql_rows)}):\n{json.dumps(sql_rows[:10], indent=2)}\n\n"
            f"Semantic Matches ({len(semantic_docs)}):\n{json.dumps(semantic_docs[:10], indent=2)}\n\n"
            "Compose a precise answer. Mention names and IDs where useful."
        )
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}],
            temperature=0.4,
        )
        return resp.choices[0].message.content

    def _log(self, question: str, qtype: str, sql: Optional[str], params: Optional[Dict[str, Any]], result_count: int, answer: str):
        try:
            self.db.add(AIQueryLog(
                question=question,
                query_type=qtype,
                sql=sql,
                parameters=params,
                result_count=result_count,
                answer=answer,
            ))
            self.db.commit()
        except Exception as e:
            logging.error(f"Failed to log AI query: {e}")
            self.db.rollback()

    def answer(self, question: str) -> Dict[str, Any]:
        try:
            schema = self._schema_introspection()
            schema_summary = self._schema_summary(schema)
            route = self.router.classify(question, schema_summary)
            qtype = route.get("type", "sql")

            sql_rows: List[Dict[str, Any]] = []
            semantic_docs: List[Dict[str, Any]] = []
            sql_text_used: Optional[str] = None
            sql_params_used: Optional[Dict[str, Any]] = None

            if qtype in ("sql", "hybrid"):
                gen = self._generate_sql(question, schema_summary)
                sql_text_used = self._ensure_case_insensitive_like(gen.get("sql", ""))
                sql_params_used = self._ensure_like_wildcards(sql_text_used, gen.get("parameters", {}))
                sql_rows, sql_err = self._try_execute_sql(sql_text_used, sql_params_used)
                if sql_err:
                    # Re-generate with error feedback and try once more
                    fixed = self._regenerate_sql_with_feedback(question, schema_summary, sql_text_used, sql_err)
                    sql_text_used = self._ensure_case_insensitive_like(fixed.get("sql", sql_text_used))
                    sql_params_used = self._ensure_like_wildcards(sql_text_used, fixed.get("parameters", sql_params_used))
                    sql_rows, sql_err2 = self._try_execute_sql(sql_text_used, sql_params_used)
                    if sql_err2:
                        logging.error(f"SQL still failing after regeneration: {sql_err2}")

            if settings.enable_vector_search and qtype in ("semantic", "hybrid"):
                try:
                    # Lazy-build embeddings if collection exists but is empty
                    if self.vectors.enabled:
                        self.vectors.upsert_all(self.db)
                    semantic_docs = self.vectors.search(question, top_k=10)
                except Exception as e:
                    logging.error(f"Vector search error: {e}")

            answer = self._compose_answer(question, sql_rows, semantic_docs)
            # Improve clarity when reporting queries return no rows
            if qtype in ("sql", "hybrid") and not sql_rows:
                # Try SQL name check, then semantic vectors, then fuzzy resolver
                fallback_msg = self._fallback_reporting_message(question, sql_text_used, sql_params_used)
                if fallback_msg:
                    answer = fallback_msg
                else:
                    # If basic fallback produced nothing, attempt semantic and fuzzy directly
                    mgr_param = (sql_params_used or {}).get("manager_name")
                    if mgr_param:
                        semantic_msg = self._semantic_reports_fallback(mgr_param)
                        if semantic_msg:
                            answer = semantic_msg
                        else:
                            fuzzy_msg = self._fuzzy_reports_fallback(mgr_param)
                            if fuzzy_msg:
                                answer = fuzzy_msg
            self._log(question, qtype, sql_text_used, sql_params_used, len(sql_rows) + len(semantic_docs), answer)

            preview = sql_rows if sql_rows else semantic_docs
            query_used = sql_text_used or ("vector_search:mendix" if semantic_docs else "")
            return {
                "success": True,
                "question": question,
                "answer": answer,
                "query_type": qtype,
                "query_used": query_used,
                "data_preview": preview[:10] if isinstance(preview, list) else preview,
            }
        except Exception as e:
            # On OpenAI quota errors or other failures, provide offline fallback
            logging.error(f"Dynamic AI path failed, falling back to offline: {e}")
            offline = OfflineQueryService(self.db)
            return offline.answer(question)