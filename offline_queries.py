from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from models import Employee, Department, Goal, Project, Skill, EmployeeSkill


class OfflineQueryService:
    """Deterministic, OpenAI-free handler for common structured queries.

    This service is intentionally simple and only covers basic intents
    like counts, lists, and straightforward relationship filters.
    It returns a dict compatible with QueryResponse used by /query.
    """

    def __init__(self, db: Session):
        self.db = db

    def answer(self, question: str) -> Dict[str, Any]:
        q = (question or "").strip()
        ql = q.lower()

        # Routing by keywords. Order matters for specificity.
        if "how many employees" in ql or ("count" in ql and "employees" in ql):
            return self._count_employees(q)
        if "list all active employees" in ql or ("active employees" in ql and "list" in ql):
            return self._list_employees(active=True)
        if "blocked employees" in ql:
            return self._list_employees(blocked=True)
        if "list all departments" in ql or ("departments" in ql and "list" in ql):
            return self._list_departments()
        if ("how many departments" in ql) or ("count" in ql and "departments" in ql):
            return self._count_departments(q)
        if "list all skills" in ql or ("skills" in ql and "list" in ql and "employees" not in ql):
            return self._list_skills()
        # Skills: who has X skills
        if "who has" in ql and "skill" in ql:
            # extract skill name between 'who has' and 'skill'
            skill = self._extract_between(ql, "who has", "skill")
            skill = (skill or "").replace("skills", "").strip()
            if skill:
                return self._employees_with_skill(skill)
        if "who has" in ql and "skills" in ql:
            skill = self._extract_between(ql, "who has", "skills")
            skill = (skill or "").strip()
            if skill:
                return self._employees_with_skill(skill)
        # Pending goals
        if "pending goals" in ql or ("in progress" in ql and "goals" in ql):
            return self._list_pending_goals()
        # Goals assigned to/by NAME or ID
        if "assigned to" in ql and "goals" in ql:
            who = self._extract_after(ql, "assigned to")
            return self._goals_assigned(to=who)
        if "assigned by" in ql and "goals" in ql:
            who = self._extract_after(ql, "assigned by")
            return self._goals_assigned(by=who)
        # Reports to / under manager NAME
        if ("reports to" in ql or "under manager" in ql) and "employees" in ql:
            # try to extract manager name after phrase
            key = "reports to" if "reports to" in ql else "under manager"
            who = self._extract_after(ql, key)
            return self._employees_reporting_to(who)

        # Default: offline not sure
        return {
            "success": False,
            "question": q,
            "answer": "Offline mode: I could not classify this into a supported basic query.",
            "query_type": "offline-sql",
            "query_used": None,
            "data_preview": None,
            "error": "unsupported_offline_query",
        }

    # ------------ helpers ------------
    def _extract_between(self, text: str, start: str, end: str) -> Optional[str]:
        try:
            s = text.find(start)
            e = text.find(end, s + len(start))
            if s != -1 and e != -1:
                return text[s + len(start):e].strip()
        except Exception:
            pass
        return None

    def _extract_after(self, text: str, key: str) -> Optional[str]:
        try:
            i = text.find(key)
            if i != -1:
                return text[i + len(key):].strip()
        except Exception:
            pass
        return None

    # ------------ query implementations ------------
    def _count_employees(self, question: str) -> Dict[str, Any]:
        total = self.db.query(func.count(Employee.id)).scalar() or 0
        return {
            "success": True,
            "question": question,
            "answer": f"Total employees: {total}",
            "query_type": "offline-sql",
            "query_used": "SELECT COUNT(*) FROM employees",
            "data_preview": [{"total_employees": total}],
        }

    def _list_employees(self, active: Optional[bool] = None, blocked: Optional[bool] = None) -> Dict[str, Any]:
        q = self.db.query(Employee)
        if active is not None:
            q = q.filter(Employee.active == active)
        if blocked is not None:
            q = q.filter(Employee.blocked == blocked)
        rows = q.limit(50).all()
        preview = [
            {
                "employee_id": r.employee_id,
                "full_name": r.full_name,
                "designation": r.designation,
                "active": r.active,
                "blocked": r.blocked,
            }
            for r in rows
        ]
        label = "active" if active else ("blocked" if blocked else "")
        ans = f"Found {len(preview)} {label} employees".strip()
        return {
            "success": True,
            "question": f"List {label} employees",
            "answer": ans,
            "query_type": "offline-sql",
            "query_used": "employees filtered by flags",
            "data_preview": preview,
        }

    def _list_departments(self) -> Dict[str, Any]:
        rows = self.db.query(Department).limit(50).all()
        preview = [
            {"department_id": r.department_id, "name": r.name, "head_employee_id": r.head_employee_id}
            for r in rows
        ]
        return {
            "success": True,
            "question": "List all departments",
            "answer": f"Found {len(preview)} departments",
            "query_type": "offline-sql",
            "query_used": "SELECT name, department_id FROM departments LIMIT 50",
            "data_preview": preview,
        }

    def _count_departments(self, question: str) -> Dict[str, Any]:
        total = self.db.query(func.count(Department.id)).scalar() or 0
        return {
            "success": True,
            "question": question,
            "answer": f"Total departments: {total}",
            "query_type": "offline-sql",
            "query_used": "SELECT COUNT(*) FROM departments",
            "data_preview": [{"total_departments": total}],
        }

    def _list_skills(self) -> Dict[str, Any]:
        rows = self.db.query(Skill).limit(50).all()
        preview = [{"skill_id": r.skill_id, "name": r.name, "category": r.category} for r in rows]
        return {
            "success": True,
            "question": "List all skills",
            "answer": f"Found {len(preview)} skills",
            "query_type": "offline-sql",
            "query_used": "SELECT name, skill_id FROM skills LIMIT 50",
            "data_preview": preview,
        }

    def _employees_with_skill(self, skill_name: str) -> Dict[str, Any]:
        # Use ORM join and ilike for portability (SQLite/PG)
        rows = (
            self.db.query(Employee, EmployeeSkill, Skill)
            .join(EmployeeSkill, Employee.employee_id == EmployeeSkill.employee_id)
            .join(Skill, EmployeeSkill.skill_id == Skill.skill_id)
            .filter(Skill.name.ilike(f"%{skill_name}%"))
            .limit(50)
            .all()
        )
        preview = [
            {
                "employee_id": e.employee_id,
                "full_name": e.full_name,
                "designation": e.designation,
                "skill": s.name,
                "proficiency_level": es.proficiency_level,
                "years_of_experience": es.years_of_experience,
                "certified": es.certified,
            }
            for (e, es, s) in rows
        ]
        return {
            "success": True,
            "question": f"Who has {skill_name} skills",
            "answer": f"Found {len(preview)} employees with {skill_name} skills",
            "query_type": "offline-sql",
            "query_used": "employees JOIN employee_skills JOIN skills WHERE skills.name ILIKE :skill",
            "data_preview": preview,
        }

    def _list_pending_goals(self) -> Dict[str, Any]:
        rows = (
            self.db.query(Goal)
            .filter(Goal.status.in_(["Pending", "In Progress"]))
            .limit(50)
            .all()
        )
        # Attach employee info
        preview = []
        for g in rows:
            emp = self.db.query(Employee).filter(Employee.employee_id == g.employee_id).first()
            preview.append({
                "goal_id": g.goal_id,
                "title": g.title,
                "status": g.status,
                "employee_id": g.employee_id,
                "employee_name": emp.full_name if emp else None,
            })
        return {
            "success": True,
            "question": "Show all pending goals",
            "answer": f"Found {len(preview)} pending or in-progress goals",
            "query_type": "offline-sql",
            "query_used": "SELECT * FROM goals WHERE status IN ('Pending','In Progress')",
            "data_preview": preview,
        }

    def _goals_assigned(self, to: Optional[str] = None, by: Optional[str] = None) -> Dict[str, Any]:
        q = self.db.query(Goal)
        question = "List goals"
        if to:
            question = f"Goals assigned to {to}"
            to_clean = to.strip()
            if to_clean.upper().startswith("LCL"):
                q = q.filter(Goal.assigned_to_employee_id == to_clean)
            else:
                # join to employees via name
                q = q.join(Employee, Employee.employee_id == Goal.assigned_to_employee_id)
                q = q.filter(Employee.full_name.ilike(f"%{to_clean}%"))

        if by:
            question = f"Goals assigned by {by}"
            by_clean = by.strip()
            if by_clean.upper().startswith("LCL"):
                q = q.filter(Goal.assigned_by_employee_id == by_clean)
            else:
                q = q.join(Employee, Employee.employee_id == Goal.assigned_by_employee_id)
                q = q.filter(Employee.full_name.ilike(f"%{by_clean}%"))

        rows = q.limit(50).all()
        preview = [
            {
                "goal_id": g.goal_id,
                "title": g.title,
                "status": g.status,
                "assigned_to_employee_id": g.assigned_to_employee_id,
                "assigned_by_employee_id": g.assigned_by_employee_id,
            }
            for g in rows
        ]
        return {
            "success": True,
            "question": question,
            "answer": f"Found {len(preview)} goals",
            "query_type": "offline-sql",
            "query_used": "goals filtered by assigned_to/assigned_by",
            "data_preview": preview,
        }

    def _employees_reporting_to(self, who: Optional[str]) -> Dict[str, Any]:
        who = (who or "").strip()
        q = (
            self.db.query(Employee)
            .join(Employee, Employee.manager_employee_id == Employee.employee_id, isouter=True)
        )
        # The above join with same model is tricky; prefer explicit alias via subquery
        # For simplicity, do a two-step: resolve manager by name, then filter subordinates.
        mgr_candidates = []
        if who:
            if who.upper().startswith("LCL"):
                mgr_candidates = self.db.query(Employee).filter(Employee.employee_id == who).all()
            else:
                mgr_candidates = self.db.query(Employee).filter(Employee.full_name.ilike(f"%{who}%")).all()
        subs: List[Dict[str, Any]] = []
        for m in mgr_candidates:
            rows = self.db.query(Employee).filter(Employee.manager_employee_id == m.employee_id).limit(50).all()
            for e in rows:
                subs.append({
                    "manager_id": m.employee_id,
                    "manager_name": m.full_name,
                    "employee_id": e.employee_id,
                    "full_name": e.full_name,
                    "designation": e.designation,
                })
        answer = (
            f"Found {len(subs)} direct reports" + (f" under {who}" if who else "")
        )
        return {
            "success": True,
            "question": f"Employees reporting to {who}" if who else "Employees reporting to manager",
            "answer": answer,
            "query_type": "offline-sql",
            "query_used": "employees self-join via manager_employee_id",
            "data_preview": subs,
        }