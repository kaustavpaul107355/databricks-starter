#!/usr/bin/env python3
"""
Interactive User Folder Browser

A rich, interactive way to explore your Databricks user folder
from your local development environment.
"""

import os
import sys
import json
from datetime import datetime
from databricks.connect import DatabricksSession
from src.user_folder_manager import create_user_folder_manager
from pyspark.sql.functions import col, when, lit, round as spark_round

class UserFolderBrowser:
    """Interactive browser for Databricks user folder."""
    
    def __init__(self, spark: DatabricksSession, username: str = None):
        """Initialize the browser."""
        self.spark = spark
        self.user_manager = create_user_folder_manager(spark, username)
        self.current_path = ""
        
    def format_size(self, size_bytes):
        """Format file size in human-readable format."""
        if size_bytes is None:
            return "Unknown"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    def format_timestamp(self, timestamp):
        """Format timestamp in human-readable format."""
        if timestamp is None:
            return "Unknown"
        
        try:
            # Convert milliseconds to datetime
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return "Invalid"
    
    def get_file_info(self, path: str = ""):
        """Get detailed file information for a path."""
        try:
            full_path = f"{self.user_manager.user_folder}/{path}".rstrip('/')
            files_df = self.spark.read.format("binaryFile").load(full_path)
            
            if files_df.count() == 0:
                return None
            
            # Add formatted columns
            files_df = files_df.withColumn("size_formatted", 
                when(col("length").isNotNull(), 
                     spark_round(col("length") / 1024, 2)).otherwise(lit("Unknown")))
            
            files_df = files_df.withColumn("modified_formatted",
                when(col("modificationTime").isNotNull(),
                     col("modificationTime")).otherwise(lit("Unknown")))
            
            return files_df
            
        except Exception as e:
            print(f"❌ Error reading path {path}: {e}")
            return None
    
    def list_directory(self, path: str = ""):
        """List contents of a directory with rich formatting."""
        print(f"\n📁 Directory: /Users/{self.user_manager.username}/{path}")
        print("=" * 80)
        
        files_df = self.get_file_info(path)
        
        if files_df is None:
            print("📂 Directory is empty or could not be accessed")
            return
        
        # Get file count
        total_files = files_df.count()
        print(f"📊 Total items: {total_files}")
        
        if total_files == 0:
            print("📂 No files found")
            return
        
        # Show files in a table format
        print("\n📋 File Listing:")
        print("-" * 80)
        print(f"{'Name':<40} {'Size':<12} {'Modified':<20} {'Type':<10}")
        print("-" * 80)
        
        # Collect and sort files
        files = files_df.collect()
        files.sort(key=lambda x: (x["isDirectory"], x["path"].lower()))
        
        for file in files:
            # Extract filename from path
            filename = file["path"].split("/")[-1]
            
            # Determine file type
            if file["isDirectory"]:
                file_type = "📁 DIR"
            else:
                ext = filename.split(".")[-1] if "." in filename else "file"
                file_type = f"📄 {ext.upper()}"
            
            # Format size
            size = self.format_size(file["length"])
            
            # Format timestamp
            modified = self.format_timestamp(file["modificationTime"])
            
            # Truncate long names
            display_name = filename[:37] + "..." if len(filename) > 40 else filename
            
            print(f"{display_name:<40} {size:<12} {modified:<20} {file_type:<10}")
    
    def explore_file(self, filepath: str):
        """Explore the contents of a specific file."""
        try:
            print(f"\n🔍 Exploring file: {filepath}")
            print("=" * 60)
            
            # Determine file type and read accordingly
            if filepath.endswith('.parquet'):
                df = self.user_manager.read_file(filepath, "parquet")
                print(f"📊 Parquet file with {df.count()} rows and {len(df.columns)} columns")
                print("\n📋 Schema:")
                df.printSchema()
                print(f"\n📈 Sample data (first 5 rows):")
                df.show(5, truncate=False)
                
            elif filepath.endswith('.csv'):
                df = self.user_manager.read_file(filepath, "csv")
                print(f"📊 CSV file with {df.count()} rows and {len(df.columns)} columns")
                print("\n📋 Schema:")
                df.printSchema()
                print(f"\n📈 Sample data (first 5 rows):")
                df.show(5, truncate=False)
                
            elif filepath.endswith('.json'):
                df = self.user_manager.read_file(filepath, "json")
                print(f"📊 JSON file with {df.count()} rows and {len(df.columns)} columns")
                print("\n📋 Schema:")
                df.printSchema()
                print(f"\n📈 Sample data (first 5 rows):")
                df.show(5, truncate=False)
                
            elif filepath.endswith('.delta'):
                df = self.user_manager.read_file(filepath, "delta")
                print(f"📊 Delta file with {df.count()} rows and {len(df.columns)} columns")
                print("\n📋 Schema:")
                df.printSchema()
                print(f"\n📈 Sample data (first 5 rows):")
                df.show(5, truncate=False)
                
            else:
                print(f"⚠️ File type not supported for preview: {filepath}")
                print("💡 Supported types: .parquet, .csv, .json, .delta")
                
        except Exception as e:
            print(f"❌ Error exploring file {filepath}: {e}")
    
    def get_folder_stats(self):
        """Get comprehensive statistics about the user folder."""
        print("\n📊 User Folder Statistics")
        print("=" * 50)
        
        try:
            # Get basic info
            folder_info = self.user_manager.get_folder_info()
            
            print(f"👤 User: {folder_info['username']}")
            print(f"📁 Folder: {folder_info['user_folder']}")
            print(f"📊 Total files: {folder_info['total_files']}")
            
            if folder_info['file_types']:
                print("\n📋 File type breakdown:")
                for ext, count in folder_info['file_types'].items():
                    print(f"   {ext}: {count} files")
            else:
                print("\n📋 No files found")
                
        except Exception as e:
            print(f"❌ Error getting folder stats: {e}")
    
    def interactive_browser(self):
        """Start an interactive file browser session."""
        print("🚀 Interactive User Folder Browser")
        print("=" * 50)
        print("Commands:")
        print("  ls [path]     - List directory contents")
        print("  cat <file>    - Explore file contents")
        print("  stats         - Show folder statistics")
        print("  pwd           - Show current path")
        print("  cd <path>     - Change directory")
        print("  help          - Show this help")
        print("  quit          - Exit browser")
        print("=" * 50)
        
        while True:
            try:
                command = input(f"\n📁 /Users/{self.user_manager.username}{self.current_path}> ").strip()
                
                if not command:
                    continue
                    
                parts = command.split()
                cmd = parts[0].lower()
                
                if cmd == "quit" or cmd == "exit":
                    print("👋 Goodbye!")
                    break
                    
                elif cmd == "help":
                    print("Commands:")
                    print("  ls [path]     - List directory contents")
                    print("  cat <file>    - Explore file contents")
                    print("  stats         - Show folder statistics")
                    print("  pwd           - Show current path")
                    print("  cd <path>     - Change directory")
                    print("  help          - Show this help")
                    print("  quit          - Exit browser")
                    
                elif cmd == "ls":
                    path = parts[1] if len(parts) > 1 else self.current_path
                    self.list_directory(path)
                    
                elif cmd == "cat":
                    if len(parts) < 2:
                        print("❌ Usage: cat <filepath>")
                        continue
                    filepath = parts[1]
                    self.explore_file(filepath)
                    
                elif cmd == "stats":
                    self.get_folder_stats()
                    
                elif cmd == "pwd":
                    print(f"📁 Current path: /Users/{self.user_manager.username}{self.current_path}")
                    
                elif cmd == "cd":
                    if len(parts) < 2:
                        print("❌ Usage: cd <path>")
                        continue
                    path = parts[1]
                    if path == "..":
                        # Go up one level
                        if self.current_path:
                            self.current_path = "/".join(self.current_path.split("/")[:-1])
                    elif path == "/" or path == "~":
                        # Go to root
                        self.current_path = ""
                    else:
                        # Go to specific path
                        if path.startswith("/"):
                            self.current_path = path
                        else:
                            self.current_path = f"{self.current_path}/{path}".rstrip("/")
                    print(f"📁 Changed to: /Users/{self.user_manager.username}{self.current_path}")
                    
                else:
                    print(f"❌ Unknown command: {cmd}")
                    print("💡 Type 'help' for available commands")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")

def main():
    """Main function to run the browser."""
    # Check environment variables
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables:")
        print("export DATABRICKS_HOST='your-workspace-url'")
        print("export DATABRICKS_CLUSTER_ID='your-cluster-id'")
        print("export DATABRICKS_TOKEN='your-personal-access-token'")
        return
    
    try:
        # Create Databricks session
        print("🔄 Connecting to Databricks...")
        spark = DatabricksSession.builder.remote().getOrCreate()
        print(f"✅ Connected! Spark version: {spark.version}")
        
        # Create browser
        browser = UserFolderBrowser(spark)
        
        # Show initial stats
        browser.get_folder_stats()
        
        # Start interactive session
        browser.interactive_browser()
        
    except Exception as e:
        print(f"❌ Failed to start browser: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
