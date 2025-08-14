#!/usr/bin/env python3
"""
Simple File Finder for Databricks User Folder

Quickly find files by name pattern.
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

def find_files(search_term):
    """Find files matching the search term."""
    try:
        # Connect to Databricks
        print("🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Create user folder manager
        user_manager = create_user_folder_manager(spark)
        print(f"🔍 Searching in: {user_manager.user_folder}")
        print(f"🔍 Looking for: '{search_term}'")
        
        # Get file information
        print("\n📖 Reading files...")
        files_df = spark.read.format("binaryFile").load(user_manager.user_folder)
        
        if files_df.count() == 0:
            print("\n📂 No files found to search")
            return
        
        # Search for matching files
        matching_files = []
        
        for file in files_df.collect():
            file_path = file["path"]
            filename = file_path.split("/")[-1]
            
            # Check if filename contains search term (case-insensitive)
            if search_term.lower() in filename.lower():
                matching_files.append({
                    'name': filename,
                    'path': file_path,
                    'size': file["length"],
                    'is_directory': file["isDirectory"],
                    'modified': file["modificationTime"]
                })
        
        # Display results
        if matching_files:
            print(f"\n✅ Found {len(matching_files)} matching files:")
            print("=" * 80)
            print(f"{'Name':<40} {'Type':<10} {'Size':<12}")
            print("-" * 80)
            
            for file_info in matching_files:
                file_type = "📁 DIR" if file_info['is_directory'] else "📄 FILE"
                size = format_size(file_info['size'])
                print(f"{file_info['name']:<40} {file_type:<10} {size:<12}")
            
            print(f"\n🎯 Found {len(matching_files)} files matching '{search_term}'")
        else:
            print(f"\n❌ No files found matching '{search_term}'")
            print("💡 Try a different search term or check your spelling")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure your cluster is running and you have the right permissions")

def main():
    """Main function."""
    print("🔍 Simple Databricks File Finder")
    print("=" * 40)
    
    # Check environment
    if not check_environment():
        return
    
    # Get search term
    if len(sys.argv) > 1:
        search_term = sys.argv[1]
    else:
        search_term = input("🔍 Enter search term: ").strip()
    
    if not search_term:
        print("❌ No search term provided")
        return
    
    # Find files
    find_files(search_term)

if __name__ == "__main__":
    main()
