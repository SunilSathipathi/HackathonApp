import requests
from requests.auth import HTTPBasicAuth
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from config import settings

logger = logging.getLogger(__name__)


class MendixAPIClient:
    """Client for interacting with Mendix REST API."""
    
    def __init__(self):
        self.base_url = settings.mendix_api_base_url
        self.auth = HTTPBasicAuth(
            settings.mendix_api_username,
            settings.mendix_api_password
        )
        self.headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _make_request(self, endpoint: str, method: str = "GET", params: Dict = None) -> Optional[Dict]:
        """Make HTTP request to Mendix API with retry logic."""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            logger.info(f"Making {method} request to: {url}")
            
            if method == "GET":
                response = requests.get(
                    url,
                    auth=self.auth,
                    headers=self.headers,
                    params=params,
                    timeout=30
                )
            elif method == "POST":
                response = requests.post(
                    url,
                    auth=self.auth,
                    headers=self.headers,
                    json=params,
                    timeout=30
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error for {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for {url}: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {str(e)}")
            raise
    
    def get_employees(self) -> List[Dict]:
        """Fetch all employees from Mendix API."""
        try:
            data = self._make_request("employee")
            logger.info(f"Successfully fetched {len(data) if data else 0} employees")
            return data or []
        except Exception as e:
            logger.error(f"Error fetching employees: {str(e)}")
            return []
    
    def get_departments(self) -> List[Dict]:
        """Fetch all departments from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("department")
            logger.info(f"Successfully fetched {len(data) if data else 0} departments")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching departments (endpoint may not exist): {str(e)}")
            return []
    
    def get_goals(self) -> List[Dict]:
        """Fetch all employee goals from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("goal")
            logger.info(f"Successfully fetched {len(data) if data else 0} goals")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching goals (endpoint may not exist): {str(e)}")
            return []

    def get_forms(self) -> List[Dict]:
        """Fetch all forms tied to goals from Mendix API."""
        try:
            data = self._make_request("form")
            logger.info(f"Successfully fetched {len(data) if data else 0} forms")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching forms (endpoint may not exist): {str(e)}")
            return []
    
    def get_projects(self) -> List[Dict]:
        """Fetch all projects from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("project")
            logger.info(f"Successfully fetched {len(data) if data else 0} projects")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching projects (endpoint may not exist): {str(e)}")
            return []
    
    def get_skills(self) -> List[Dict]:
        """Fetch all skills from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("skill")
            logger.info(f"Successfully fetched {len(data) if data else 0} skills")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching skills (endpoint may not exist): {str(e)}")
            return []
    
    def get_employee_projects(self) -> List[Dict]:
        """Fetch employee-project assignments from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("employee-project")
            logger.info(f"Successfully fetched {len(data) if data else 0} employee-project assignments")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching employee-project assignments (endpoint may not exist): {str(e)}")
            return []
    
    def get_employee_skills(self) -> List[Dict]:
        """Fetch employee-skill mappings from Mendix API."""
        try:
            # Assuming endpoint exists - adjust if different
            data = self._make_request("employee-skill")
            logger.info(f"Successfully fetched {len(data) if data else 0} employee-skill mappings")
            return data or []
        except Exception as e:
            logger.warning(f"Error fetching employee-skill mappings (endpoint may not exist): {str(e)}")
            return []