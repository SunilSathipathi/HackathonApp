"""
Test script to verify the Mendix Employee Intelligence API setup
"""
import requests
import json
import sys
from rich.console import Console
from rich.table import Table
from rich import print as rprint

console = Console()

BASE_URL = "http://localhost:8000"

# Toggle to run the large set of 100+ AI questions.
# Set to True if you want the full sweep; default False for faster CI.
RUN_FULL_AI_QUERIES = False


def test_health_check():
    """Test the health check endpoint"""
    console.print("\n[bold blue]Testing Health Check...[/bold blue]")
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=10)
        response.raise_for_status()
        data = response.json()
        
        console.print(f"✓ Status: {data['status']}", style="green")
        console.print(f"✓ Database: {data['database']}", style="green")
        console.print(f"✓ Scheduler Running: {data['scheduler']['running']}", style="green")
        return True
    except Exception as e:
        console.print(f"✗ Health check failed: {str(e)}", style="red")
        return False


def test_statistics():
    """Test the statistics endpoint"""
    console.print("\n[bold blue]Testing Statistics...[/bold blue]")
    try:
        response = requests.get(f"{BASE_URL}/api/stats", timeout=10)
        response.raise_for_status()
        data = response.json()
        
        table = Table(title="Database Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="magenta")
        
        table.add_row("Total Employees", str(data['employees']['total']))
        table.add_row("Active Employees", str(data['employees']['active']))
        table.add_row("Total Goals", str(data['goals']['total']))
        table.add_row("Pending Goals", str(data['goals']['pending']))
        table.add_row("Total Projects", str(data['projects']['total']))
        
        console.print(table)
        return True
    except Exception as e:
        console.print(f"✗ Statistics failed: {str(e)}", style="red")
        return False


def test_ask_question(question: str):
    """Test the AI query endpoint"""
    console.print(f"\n[bold blue]Testing AI Query: '{question}'[/bold blue]")
    try:
        response = requests.post(
            f"{BASE_URL}/api/ask",
            json={"question": question},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if data['success']:
            console.print("\n[bold green]Answer:[/bold green]")
            console.print(data['answer'])
            console.print(f"\n[dim]Query Type: {data.get('query_type')}[/dim]")
            console.print(f"[dim]Data Points: {data.get('data_points')}[/dim]")
            return True
        else:
            console.print(f"✗ Query failed: {data.get('error')}", style="red")
            return False
    except Exception as e:
        console.print(f"✗ Query failed: {str(e)}", style="red")
        return False


def test_sync():
    """Test manual sync"""
    console.print("\n[bold blue]Testing Manual Sync (this may take a moment)...[/bold blue]")
    try:
        response = requests.post(f"{BASE_URL}/api/sync", timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if data['success']:
            console.print("✓ Sync completed successfully", style="green")
            results = data.get('results', {})
            
            table = Table(title="Sync Results")
            table.add_column("Data Type", style="cyan")
            table.add_column("Records Synced", style="magenta")
            
            for key, value in results.items():
                table.add_row(key.capitalize(), str(value))
            
            console.print(table)
            return True
        else:
            console.print(f"✗ Sync failed: {data.get('message')}", style="red")
            return False
    except Exception as e:
        console.print(f"✗ Sync failed: {str(e)}", style="red")
        return False


def test_sync_history():
    """Test sync history endpoint"""
    console.print("\n[bold blue]Testing Sync History...[/bold blue]")
    try:
        response = requests.get(f"{BASE_URL}/api/sync-history?limit=5", timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data['logs']:
            table = Table(title="Recent Sync Operations")
            table.add_column("Type", style="cyan")
            table.add_column("Status", style="magenta")
            table.add_column("Records", style="green")
            table.add_column("Started At", style="yellow")
            
            for log in data['logs'][:5]:
                status_style = "green" if log['status'] == 'success' else "red"
                table.add_row(
                    log['sync_type'],
                    f"[{status_style}]{log['status']}[/{status_style}]",
                    str(log['records_synced']),
                    log['sync_started_at'][:19] if log['sync_started_at'] else 'N/A'
                )
            
            console.print(table)
            return True
        else:
            console.print("No sync history available yet", style="yellow")
            return True
    except Exception as e:
        console.print(f"✗ Sync history failed: {str(e)}", style="red")
        return False


def test_dynamic_reporting_by_name(manager_name: str, expected_min: int = 0):
    """Validate reporting results via dynamic endpoint by manager name."""
    console.print(f"\n[bold blue]Testing Reporting (by name): '{manager_name}'[/bold blue]")
    try:
        response = requests.post(
            f"{BASE_URL}/query",
            json={"question": f"who are reporting to {manager_name}"},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            console.print(f"✗ Dynamic reporting failed: {data.get('error')}", style="red")
            return False
        # Print a concise preview
        console.print("[bold green]Answer:[/bold green]" + f"\n{data.get('answer')}")
        preview = data.get("data_preview") or {}
        rows = preview.get("rows") if isinstance(preview, dict) else None
        count = len(rows) if rows else 0
        console.print(f"[dim]Query Used: {data.get('query_used')}[/dim]")
        console.print(f"[dim]Rows: {count}[/dim]")
        if count >= expected_min:
            return True
        # If answer string contains a count, accept that
        ans = (data.get("answer") or "").lower()
        if expected_min == 0 and ("no employees" in ans or "no direct reports" in ans):
            return True
        return count >= expected_min
    except Exception as e:
        console.print(f"✗ Dynamic reporting by name failed: {str(e)}", style="red")
        return False


def test_dynamic_reporting_by_id(manager_id: str, expected_min: int = 0):
    """Validate reporting results via dynamic endpoint by manager employee_id."""
    console.print(f"\n[bold blue]Testing Reporting (by ID): '{manager_id}'[/bold blue]")
    try:
        response = requests.post(
            f"{BASE_URL}/query",
            json={"question": f"who are reporting to employee {manager_id}"},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            console.print(f"✗ Dynamic reporting failed: {data.get('error')}", style="red")
            return False
        console.print("[bold green]Answer:[/bold green]" + f"\n{data.get('answer')}")
        preview = data.get("data_preview") or {}
        rows = preview.get("rows") if isinstance(preview, dict) else None
        count = len(rows) if rows else 0
        console.print(f"[dim]Query Used: {data.get('query_used')}[/dim]")
        console.print(f"[dim]Rows: {count}[/dim]")
        if count >= expected_min:
            return True
        ans = (data.get("answer") or "").lower()
        if expected_min == 0 and ("no employees" in ans or "no direct reports" in ans):
            return True
        return count >= expected_min
    except Exception as e:
        console.print(f"✗ Dynamic reporting by ID failed: {str(e)}", style="red")
        return False


def main():
    """Run all tests"""
    console.print("\n[bold magenta]═══════════════════════════════════════════════════[/bold magenta]")
    console.print("[bold magenta]  Mendix Employee Intelligence API - Test Suite  [/bold magenta]")
    console.print("[bold magenta]═══════════════════════════════════════════════════[/bold magenta]\n")
    
    console.print(f"[dim]Testing API at: {BASE_URL}[/dim]\n")
    
    # Check if server is running
    try:
        requests.get(BASE_URL, timeout=5)
    except:
        console.print("\n[bold red]ERROR: API server is not running![/bold red]")
        console.print("Please start the server first:")
        console.print("  python main.py\n")
        sys.exit(1)
    
    results = []
    
    # Run tests
    results.append(("Health Check", test_health_check()))
    results.append(("Manual Sync", test_sync()))
    results.append(("Statistics", test_statistics()))
    results.append(("Sync History", test_sync_history()))
    
    # Focused validations for reporting relationships
    results.append(("Reporting by name: rammohan", test_dynamic_reporting_by_name("rammohan", expected_min=0)))
    results.append(("Reporting by name: sairam", test_dynamic_reporting_by_name("sairam", expected_min=1)))
    results.append(("Reporting by ID: LCL16110001", test_dynamic_reporting_by_id("LCL16110001", expected_min=1)))
    
    # Test some AI queries
   
       # Test 100 diverse AI queries across all Mendix entities
    test_questions = [
        # Employee related
        "How many employees do we have in total?",
        "List all active employees.",
        "Who are the blocked employees?",
        "Show employees who joined recently.",
        "Which employees have not logged in recently?",
        "List employees under manager Harshitha Byrishetty.",
        "Who are the top-paid employees?",
        "Which employees belong to the AI department?",
        "Show employees who are inactive.",
        "Which employees are managers of any project?",

        # Department related
        "List all departments in the company.",
        "Who is the head of the HR department?",
        "Which department has the highest number of employees?",
        "Show employees working in the Finance department.",
        "When was the IT department created?",
        "List all department names and their heads.",
        "Which department does Praveen Sharma belong to?",
        "How many departments do we have?",
        "Which department has the most goals assigned?",
        "Show departments that have no employees assigned.",

        # Goals related
        "List all goals assigned to Sunil Sathpathi.",
        "Show all pending goals.",
        "Which employees have completed all their goals?",
        "What are the top priority goals in progress?",
        "Show all goals that are overdue.",
        "List all goals assigned by Praveen Sharma.",
        "Which goals have progress below 50 percent?",
        "Show goals categorized under performance improvement.",
        "List employees with the highest goal completion rate.",
        "How many total goals exist in the database?",

        # Projects related
        "List all active projects.",
        "Which employees are working on Project Alpha?",
        "Show projects managed by Harshitha Byrishetty.",
        "Which projects are marked as completed?",
        "Show the client names for each project.",
        "Which project started most recently?",
        "List all projects currently on hold.",
        "How many projects are in progress?",
        "Which project has the highest number of team members?",
        "List all projects and their start and end dates.",

        # Skills related
        "List all skills available in the system.",
        "Show employees who are certified in Python.",
        "Which employees are skilled in Mendix?",
        "List employees with expert-level skills.",
        "Show all technical skills under AI category.",
        "Which employees have more than 5 years of experience in Java?",
        "List skills that are marked as soft skills.",
        "Which skill has the most certified employees?",
        "Show all employees who have at least 3 skills.",
        "List employees who recently updated their skill records.",

        # Forms related
        "List all forms and their current statuses.",
        "Which forms are still in progress?",
        "Show forms linked to pending goals.",
        "Which form was created most recently?",
        "List forms submitted in the last 7 days.",
        "Show forms related to goal G001.",
        "Which forms have been submitted but not approved?",
        "List employees who submitted forms successfully.",
        "Which goals do not have any form linked yet?",
        "How many total forms exist in the database?",

        # Tasks related
        "List all tasks and their owners.",
        "Which tasks are assigned to Harshitha Byrishetty?",
        "Show all tasks under forms that are still in progress.",
        "Which tasks have status Pending?",
        "List tasks created in the last 24 hours.",
        "Which task owners have multiple open tasks?",
        "Show all completed tasks and their owners.",
        "Which tasks belong to form F001?",
        "How many total tasks exist in the database?",
        "List all default return owner tasks.",

        # Cross-entity / Relationship queries
        "List all employees with their departments and project names.",
        "Show all employees and their current goals.",
        "Which employees are both project managers and goal assignees?",
        "List each employee with their top skill and active project.",
        "Show employees along with their goal completion percentage.",
        "Which employees have forms pending approval?",
        "List all employees and how many tasks they own.",
        "Which departments have employees working on more than one project?",
        "List employees with at least one high-priority goal.",
        "Show project-wise employee allocation percentages.",

        # Analytics & counts
        "How many active projects are currently running?",
        "Count employees per department.",
        "How many skills exist per category?",
        "Count total pending goals by department.",
        "Show number of completed forms this month.",
        "How many tasks are marked completed?",
        "How many employees are certified in at least one skill?",
        "Count number of goals per employee.",
        "Show count of employees managed by each manager.",
        "How many goals were assigned in the last 30 days?",

        # General / descriptive
        "Give a summary of all departments and their heads.",
        "Provide an overview of current project statuses.",
        "Summarize employee skill distribution by category.",
        "What are the most common goal categories?",
        "Summarize task completion trends.",
        "Which employees are involved in both AI and Cloud projects?",
        "Give details of all employees having no assigned projects.",
        "List goals and their associated forms and tasks.",
        "Which projects have no active employees?",
        "Show employees working under multiple managers."
    ]

    
    if RUN_FULL_AI_QUERIES:
        for question in test_questions:
            results.append((f"Query: {question[:30]}...", test_ask_question(question)))
    
    # Summary
    console.print("\n[bold magenta]═══════════════════════════════════════════════════[/bold magenta]")
    console.print("[bold magenta]              Test Summary                        [/bold magenta]")
    console.print("[bold magenta]═══════════════════════════════════════════════════[/bold magenta]\n")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        style = "green" if result else "red"
        console.print(f"{status:10} {test_name}", style=style)
    
    console.print(f"\n[bold]Results: {passed}/{total} tests passed[/bold]\n")
    
    if passed == total:
        console.print("[bold green]All tests passed! 🎉[/bold green]\n")
    else:
        console.print("[bold yellow]Some tests failed. Check the output above for details.[/bold yellow]\n")


if __name__ == "__main__":
    main()