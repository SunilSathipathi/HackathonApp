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
    
    # Test some AI queries
    test_questions = [
        "How many employees do we have?",
        "Who are the Mendix Developers?",
        "What are the pending goals?"
    ]
    
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