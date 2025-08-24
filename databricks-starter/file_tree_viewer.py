#!/usr/bin/env python3
"""
File Tree Viewer for Databricks User Folder

Shows your user folder structure in a tree format, similar to a file explorer.
"""

import os
import sys
from databricks.connect import DatabricksSession
from src.user_folder_manager import create_user_folder_manager
from pyspark.sql.functions import col, when, lit

class FileTreeViewer:
    """Tree-style viewer for Databricks user folder."""
    
    def __init__(self, spark: DatabricksSession, username: str = None):
        """Initialize the tree viewer."""
        self.spark = spark
        self.user_manager = create_user_folder_manager(spark, username)
        
    def get_file_tree(self, path: str = "", max_depth: int = 3, current_depth: int = 0):
        """Recursively build a file tree structure."""
        if current_depth > max_depth:
            return None
            
        try:
            full_path = f"{self.user_manager.user_folder}/{path}".rstrip('/')
            files_df = self.spark.read.format("binaryFile").load(full_path)
            
            if files_df.count() == 0:
                return None
                
            tree = {
                'path': path,
                'full_path': full_path,
                'files': [],
                'directories': [],
                'total_size': 0
            }
            
            # Process files and directories
            for file in files_df.collect():
                file_path = file["path"]
                filename = file_path.split("/")[-1]
                
                if file["isDirectory"]:
                    # Recursively get subdirectory contents
                    sub_path = f"{path}/{filename}".strip('/')
                    sub_tree = self.get_file_tree(sub_path, max_depth, current_depth + 1)
                    if sub_tree:
                        tree['directories'].append(sub_tree)
                else:
                    # Add file information
                    tree['files'].append({
                        'name': filename,
                        'size': file["length"],
                        'modified': file["modificationTime"],
                        'path': file_path
                    })
                    tree['total_size'] += file["length"] or 0
            
            return tree
            
        except Exception as e:
            print(f"⚠️ Error reading path {path}: {e}")
            return None
    
    def format_size(self, size_bytes):
        """Format file size in human-readable format."""
        if size_bytes is None or size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    def print_tree(self, tree, prefix: str = "", is_last: bool = True, show_details: bool = False):
        """Print the file tree in a visual format."""
        if not tree:
            return
            
        # Print current directory
        if tree['path']:
            dir_name = tree['path'].split('/')[-1] if '/' in tree['path'] else tree['path']
            print(f"{prefix}{'└── ' if is_last else '├── '}📁 {dir_name}/")
        else:
            print(f"{prefix}📁 /Users/{self.user_manager.username}/")
        
        # Print files
        files = tree['files']
        for i, file in enumerate(files):
            is_last_file = i == len(files) - 1 and not tree['directories']
            file_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            
            if show_details:
                size = self.format_size(file['size'])
                print(f"{file_prefix}{'└── ' if is_last_file else '├── '}📄 {file['name']} ({size})")
            else:
                print(f"{file_prefix}{'└── ' if is_last_file else '├── '}📄 {file['name']}")
        
        # Print subdirectories
        directories = tree['directories']
        for i, subdir in enumerate(directories):
            is_last_subdir = i == len(directories) - 1
            subdir_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            self.print_tree(subdir, subdir_prefix, is_last_subdir, show_details)
    
    def show_detailed_tree(self, path: str = "", max_depth: int = 3):
        """Show a detailed tree with file sizes and counts."""
        print(f"\n🌳 File Tree for: /Users/{self.user_manager.username}/{path}")
        print("=" * 80)
        
        tree = self.get_file_tree(path, max_depth)
        if tree:
            self.print_tree(tree, show_details=True)
            
            # Show summary
            total_files = sum(len(d['files']) for d in self._get_all_directories(tree))
            total_dirs = len(self._get_all_directories(tree))
            total_size = sum(f['size'] or 0 for f in self._get_all_files(tree))
            
            print("\n📊 Summary:")
            print(f"   📁 Directories: {total_dirs}")
            print(f"   📄 Files: {total_files}")
            print(f"   💾 Total size: {self.format_size(total_size)}")
        else:
            print("📂 No files found or path not accessible")
    
    def show_simple_tree(self, path: str = "", max_depth: int = 3):
        """Show a simple tree without file details."""
        print(f"\n🌳 File Tree for: /Users/{self.user_manager.username}/{path}")
        print("=" * 60)
        
        tree = self.get_file_tree(path, max_depth)
        if tree:
            self.print_tree(tree, show_details=False)
        else:
            print("📂 No files found or path not accessible")
    
    def _get_all_directories(self, tree):
        """Get all directories from the tree recursively."""
        directories = [tree]
        for subdir in tree['directories']:
            directories.extend(self._get_all_directories(subdir))
        return directories
    
    def _get_all_files(self, tree):
        """Get all files from the tree recursively."""
        files = tree['files'].copy()
        for subdir in tree['directories']:
            files.extend(self._get_all_files(subdir))
        return files
    
    def search_files(self, query: str, path: str = ""):
        """Search for files by name pattern."""
        print(f"\n🔍 Searching for files containing '{query}' in /Users/{self.user_manager.username}/{path}")
        print("=" * 80)
        
        try:
            full_path = f"{self.user_manager.user_folder}/{path}".rstrip('/')
            files_df = self.spark.read.format("binaryFile").load(full_path)
            
            if files_df.count() == 0:
                print("📂 No files found to search")
                return
            
            # Filter files by name
            matching_files = []
            for file in files_df.collect():
                filename = file["path"].split("/")[-1]
                if query.lower() in filename.lower():
                    matching_files.append({
                        'name': filename,
                        'path': file["path"],
                        'size': file["length"],
                        'is_directory': file["isDirectory"]
                    })
            
            if matching_files:
                print(f"✅ Found {len(matching_files)} matching files:")
                print("-" * 80)
                print(f"{'Name':<40} {'Type':<10} {'Size':<12}")
                print("-" * 80)
                
                for file in matching_files:
                    file_type = "📁 DIR" if file['is_directory'] else "📄 FILE"
                    size = self.format_size(file['size'])
                    print(f"{file['name']:<40} {file_type:<10} {size:<12}")
            else:
                print(f"❌ No files found matching '{query}'")
                
        except Exception as e:
            print(f"❌ Error searching files: {e}")

def main():
    """Main function to run the tree viewer."""
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
        
        # Create tree viewer
        viewer = FileTreeViewer(spark)
        
        print("\n🌳 File Tree Viewer for Databricks User Folder")
        print("=" * 60)
        print("Available views:")
        print("1. Simple tree (file names only)")
        print("2. Detailed tree (with file sizes)")
        print("3. Search files by name")
        print("4. Exit")
        
        while True:
            try:
                choice = input("\n🎯 Choose an option (1-4): ").strip()
                
                if choice == "1":
                    viewer.show_simple_tree()
                elif choice == "2":
                    viewer.show_detailed_tree()
                elif choice == "3":
                    query = input("🔍 Enter search term: ").strip()
                    if query:
                        viewer.search_files(query)
                elif choice == "4":
                    print("👋 Goodbye!")
                    break
                else:
                    print("❌ Invalid choice. Please enter 1-4.")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                
    except Exception as e:
        print(f"❌ Failed to start tree viewer: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
