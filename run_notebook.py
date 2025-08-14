#!/usr/bin/env python3
"""
Run Notebooks on Databricks Cluster

Execute notebooks directly on your Databricks cluster from local development.
"""

import os
import sys
import json
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

def check_environment():
    """Check if required environment variables are set."""
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Set them with:")
        print("export DATABRICKS_HOST='your-workspace-url'")
        print("export DATABRICKS_CLUSTER_ID='your-cluster-id'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return False
    
    return True

def run_notebook_on_cluster(notebook_path, parameters=None):
    """Run a notebook on the Databricks cluster."""
    try:
        print(f"🚀 Running notebook: {notebook_path}")
        print(f"🔄 On cluster: {os.environ['DATABRICKS_CLUSTER_ID']}")
        
        # Create Databricks session
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Run the notebook
        print("\n📖 Executing notebook...")
        
        # Method 1: Run notebook content directly
        if notebook_path.endswith('.py'):
            # For Python files, we can execute them directly
            print("📝 Executing Python file content...")
            
            # Read and execute the file
            with open(notebook_path, 'r') as f:
                content = f.read()
            
            # Execute the content
            exec(content)
            
            print("✅ Python file executed successfully!")
            
        else:
            print("⚠️ Only .py files are supported for direct execution")
            print("💡 For .ipynb files, use the web interface or convert to .py")
        
    except Exception as e:
        print(f"❌ Error running notebook: {e}")
        print("\n💡 Make sure your cluster is running and you have the right permissions")

def run_job_on_cluster(notebook_path, job_name=None, parameters=None):
    """Submit a job to run on the cluster."""
    try:
        print(f"🚀 Submitting job: {notebook_path}")
        
        # Create workspace client
        client = WorkspaceClient(
            host=os.environ['DATABRICKS_HOST'],
            token=os.environ['DATABRICKS_TOKEN']
        )
        
        # Create job
        job_settings = jobs.JobSettings(
            name=job_name or f"Run {notebook_path}",
            tasks=[
                jobs.Task(
                    task_key="run_notebook",
                    notebook_task=jobs.NotebookTask(
                        notebook_path=notebook_path,
                        base_parameters=parameters or {}
                    ),
                    existing_cluster_id=os.environ['DATABRICKS_CLUSTER_ID']
                )
            ]
        )
        
        # Submit job
        job = client.jobs.create(job_settings)
        print(f"✅ Job submitted successfully!")
        print(f"📊 Job ID: {job.job_id}")
        print(f"🔗 View job: {os.environ['DATABRICKS_HOST']}/#job/{job.job_id}")
        
        return job.job_id
        
    except Exception as e:
        print(f"❌ Error submitting job: {e}")
        print("\n💡 Make sure your cluster is running and you have the right permissions")
        return None

def list_available_notebooks():
    """List notebooks available to run."""
    try:
        print("📚 Available notebooks in this project:")
        print("=" * 50)
        
        # List notebooks directory
        notebooks_dir = "notebooks"
        if os.path.exists(notebooks_dir):
            for file in os.listdir(notebooks_dir):
                if file.endswith(('.py', '.ipynb')):
                    print(f"   📝 {file}")
        
        # List Python files in root
        for file in os.listdir('.'):
            if file.endswith('.py') and file not in ['run_notebook.py', 'setup_databricks_connect.py']:
                print(f"   📝 {file}")
        
        print("\n💡 To run a notebook:")
        print("   python run_notebook.py <notebook_path>")
        print("   Example: python run_notebook.py notebooks/local_development_example.py")
        
    except Exception as e:
        print(f"❌ Error listing notebooks: {e}")

def main():
    """Main function."""
    print("🚀 Databricks Notebook Runner")
    print("=" * 40)
    
    # Check environment
    if not check_environment():
        return
    
    # Check arguments
    if len(sys.argv) < 2:
        print("📚 No notebook specified. Available options:")
        list_available_notebooks()
        return
    
    notebook_path = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(notebook_path):
        print(f"❌ File not found: {notebook_path}")
        list_available_notebooks()
        return
    
    # Run the notebook
    print(f"\n🎯 Running: {notebook_path}")
    print("=" * 50)
    
    # Choose execution method
    print("\n🔧 Choose execution method:")
    print("1. Run directly on cluster (immediate execution)")
    print("2. Submit as job (background execution)")
    
    try:
        choice = input("\n🎯 Choose option (1 or 2): ").strip()
        
        if choice == "1":
            run_notebook_on_cluster(notebook_path)
        elif choice == "2":
            job_name = input("📝 Enter job name (optional): ").strip()
            if not job_name:
                job_name = f"Run {notebook_path}"
            run_job_on_cluster(notebook_path, job_name)
        else:
            print("❌ Invalid choice. Using direct execution.")
            run_notebook_on_cluster(notebook_path)
            
    except KeyboardInterrupt:
        print("\n👋 Execution cancelled")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
