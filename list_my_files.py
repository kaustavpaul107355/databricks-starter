#!/usr/bin/env python3
"""
Simple File Lister for Databricks User Folder

A clean, simple way to see your files in the workspace.
"""

import os
import sys
from databricks.connect import DatabricksSession
from src.user_folder_manager import create_user_folder_manager

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

def format_size(size_bytes):
    """Format file size in human-readable format."""
    if size_bytes is None:
        return "Unknown"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

def list_files():
    """List all files in the user folder."""
    try:
        # Connect to Databricks
        print("🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Create user folder manager
        user_manager = create_user_folder_manager(spark)
        print(f"📁 Browsing: {user_manager.user_folder}")
        
        # Get file information
        print("\n🔍 Reading files...")
        files_df = spark.read.format("binaryFile").load(user_manager.user_folder)
        
        if files_df.count() == 0:
            print("\n📂 Your user folder is empty")
            return
        
        # Process files
        files = []
        directories = []
        
        for file in files_df.collect():
            file_path = file["path"]
            filename = file_path.split("/")[-1]
            
            if file["isDirectory"]:
                directories.append({
                    'name': filename,
                    'path': file_path,
                    'modified': file["modificationTime"]
                })
            else:
                files.append({
                    'name': filename,
                    'path': file_path,
                    'size': file["length"],
                    'modified': file["modificationTime"]
                })
        
        # Sort files and directories
        directories.sort(key=lambda x: x['name'].lower())
        files.sort(key=lambda x: x['name'].lower())
        
        # Display results
        print(f"\n📊 Found {len(directories)} directories and {len(files)} files")
        print("=" * 80)
        
        # Show directories first
        if directories:
            print("\n📁 Directories:")
            for dir_info in directories:
                print(f"   📁 {dir_info['name']}/")
        
        # Show files
        if files:
            print("\n📄 Files:")
            for file_info in files:
                size = format_size(file_info['size'])
                print(f"   📄 {file_info['name']} ({size})")
        
        # Show summary
        total_size = sum(f['size'] or 0 for f in files)
        print(f"\n📈 Summary:")
        print(f"   📁 Directories: {len(directories)}")
        print(f"   📄 Files: {len(files)}")
        print(f"   💾 Total size: {format_size(total_size)}")
        
        print(f"\n🎯 Your files are ready to work with!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure your cluster is running and you have the right permissions")

def main():
    """Main function."""
    print("🚀 Simple Databricks File Lister")
    print("=" * 40)
    
    # Check environment
    if not check_environment():
        return
    
    # List files
    list_files()

if __name__ == "__main__":
    main()
