#!/usr/bin/env python3
"""
IDE Notebook Launcher

This script launches a notebook in your IDE with Databricks Connect.
Perfect for development and debugging!
"""

import os
import sys
import subprocess
import json

def check_environment():
    """Check if Databricks Connect environment is set up."""
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

def list_available_notebooks():
    """List notebooks available to run."""
    print("📚 Available notebooks for IDE development:")
    print("=" * 60)
    
    # List notebooks directory
    notebooks_dir = "notebooks"
    if os.path.exists(notebooks_dir):
        print(f"\n📁 {notebooks_dir}/")
        for file in os.listdir(notebooks_dir):
            if file.endswith('.py'):
                print(f"   📝 {file}")
    
    # List Python files in root
    print(f"\n📁 Root directory")
    for file in os.listdir('.'):
        if file.endswith('.py') and file not in ['launch_ide_notebook.py', 'run_notebook.py', 'setup_databricks_connect.py']:
            print(f"   📝 {file}")

def launch_notebook(notebook_path):
    """Launch a notebook in the IDE with Databricks Connect."""
    try:
        print(f"🚀 Launching notebook: {notebook_path}")
        print("=" * 60)
        
        # Check if file exists
        if not os.path.exists(notebook_path):
            print(f"❌ File not found: {notebook_path}")
            return False
        
        # Show instructions
        print("🎯 **IDE Development Mode Activated!**")
        print("\n📋 **How to use this notebook in your IDE:**")
        print("1. **Open the file** in Cursor")
        print("2. **Set breakpoints** on any line")
        print("3. **Run individual cells** by selecting code blocks")
        print("4. **Debug step-by-step** with full IDE features")
        print("5. **Code runs on remote cluster** while you develop locally")
        
        print(f"\n🔧 **Current Configuration:**")
        print(f"   🌐 Workspace: {os.environ.get('DATABRICKS_HOST')}")
        print(f"   🔧 Cluster: {os.environ.get('DATABRICKS_CLUSTET_ID')}")
        print(f"   📁 Notebook: {notebook_path}")
        
        print(f"\n💡 **Pro Tips:**")
        print("   • Use Ctrl+Shift+P to open command palette")
        print("   • Set breakpoints by clicking left of line numbers")
        print("   • Use F5 to start debugging")
        print("   • Use F9 to toggle breakpoints")
        
        print(f"\n🚀 **Ready to develop!**")
        print(f"   Open {notebook_path} in Cursor and start coding!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error launching notebook: {e}")
        return False

def main():
    """Main function."""
    print("🚀 Databricks Connect IDE Launcher")
    print("=" * 50)
    
    # Check environment
    if not check_environment():
        return
    
    # Check arguments
    if len(sys.argv) < 2:
        print("📚 No notebook specified. Available options:")
        list_available_notebooks()
        print(f"\n💡 To launch a notebook:")
        print(f"   python launch_ide_notebook.py <notebook_path>")
        print(f"   Example: python launch_ide_notebook.py notebooks/ide_integration_demo.py")
        return
    
    notebook_path = sys.argv[1]
    
    # Launch the notebook
    launch_notebook(notebook_path)

if __name__ == "__main__":
    main()
