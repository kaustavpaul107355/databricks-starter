# 🔄 Dual Repository Setup

This folder (`zerobus-delta-app`) exists in **TWO Git repositories** simultaneously:

1. **Standalone Repo**: [kaustav-paul_data/zerobus-delta-app](https://github.com/kaustav-paul_data/zerobus-delta-app) (GitHub Enterprise)
2. **Parent Repo**: [kaustavpaul107355/databricks-starter](https://github.com/kaustavpaul107355/databricks-starter)

---

## 📋 Current Configuration

### **Standalone Repository**
- **Remote**: `https://github.com/kaustav-paul_data/zerobus-delta-app.git`
- **Purpose**: Dedicated repo for the Zerobus Delta App
- **Audience**: Users who only need this specific app
- **Location**: This directory has its own `.git` folder

### **Parent Repository**
- **Remote**: `https://github.com/kaustavpaul107355/databricks-starter.git`
- **Purpose**: Collection of Databricks projects and templates
- **Audience**: Users exploring multiple Databricks examples
- **Location**: Parent directory includes this folder as part of larger project

---

## 🚀 Workflow Options

### **Option 1: Automatic Push to Both (Recommended)**

Use the provided script to push changes to both repos:

```bash
./push-to-both.sh
```

The script will:
1. Commit and push changes to the standalone `zerobus-delta-app` repo
2. Commit and push the folder to the parent `databricks-starter` repo

### **Option 2: Manual Push to Each Repo**

#### **Push to Standalone Repo (zerobus-delta-app)**

```bash
# From this directory
cd /path/to/zerobus-delta-app
git add .
git commit -m "Your commit message"
git push origin main
```

#### **Push to Parent Repo (databricks-starter)**

```bash
# From parent directory
cd /path/to/Databricks
git add databricks-starter/databricks-apps/zerobus-delta-app/
git commit -m "Update zerobus-delta-app"
git push origin main
```

### **Option 3: Push to One Repo at a Time**

```bash
# Only push to standalone repo
cd zerobus-delta-app && git push origin main

# Only push to parent repo
cd ../../../ && git add databricks-starter/databricks-apps/zerobus-delta-app/ && git commit -m "msg" && git push
```

---

## 🔐 Authentication

### **GitHub Enterprise (kaustav-paul_data)**

Since this is Databricks-managed GitHub Enterprise, you need:

1. **Personal Access Token (PAT)**
   - Go to GitHub Enterprise Settings → Developer settings → Personal access tokens
   - Generate token with `repo` scope
   - Use token as password when prompted

2. **macOS Keychain** (Configured)
   - Credentials are stored in macOS keychain
   - First push will prompt for username and PAT
   - Subsequent pushes will use stored credentials

3. **SSH Keys** (Alternative)
   ```bash
   # Switch to SSH
   cd zerobus-delta-app
   git remote set-url origin git@github.com:kaustav-paul_data/zerobus-delta-app.git
   ```

### **GitHub.com (kaustavpaul107355)**

Standard GitHub authentication via HTTPS or SSH.

---

## 📁 File Management

### **Files Tracked by Both Repos**

All application files are tracked by both repos:
- `*.py` files
- `*.md` documentation
- `static/`, `writers/`, `zerobus_sdk/` directories
- Configuration files (`app.yaml`, `requirements.txt`, etc.)

### **Files Only in Standalone Repo**

The standalone repo has its own:
- `.git/` directory (ignored by parent)
- `.gitignore` specific to this app
- `push-to-both.sh` helper script
- This `DUAL_REPO_SETUP.md` file

---

## 🔧 Troubleshooting

### **"Repository not found" Error**

**Problem**: GitHub Enterprise authentication issue

**Solution**:
```bash
# Reset credentials
git config --local --unset credential.helper
git config --local credential.helper osxkeychain

# Try push again (will prompt for credentials)
git push origin main
```

### **Merge Conflicts Between Repos**

**Problem**: Changes in one repo conflict with the other

**Solution**:
```bash
# Pull latest from standalone repo
cd zerobus-delta-app
git pull origin main

# Pull latest from parent repo
cd ../../../
git pull origin main

# Resolve conflicts manually if needed
```

### **Accidental Push to Wrong Repo**

**Problem**: Pushed to parent when you meant standalone (or vice versa)

**Solution**:
```bash
# Changes are separate - just push to the other repo
# Use push-to-both.sh to sync both
./push-to-both.sh
```

---

## 🎯 Best Practices

1. **Always push to both repos** to keep them in sync
2. **Use the `push-to-both.sh` script** for convenience
3. **Write clear commit messages** that make sense in both contexts
4. **Test changes before pushing** to ensure they work in both scenarios
5. **Keep documentation updated** in both repos

---

## 📝 Quick Reference

| Action | Command |
|--------|---------|
| Push to both repos | `./push-to-both.sh` |
| Check standalone status | `git status` (from this dir) |
| Check parent status | `cd ../../../ && git status` |
| View standalone commits | `git log --oneline` |
| View parent commits | `cd ../../../ && git log --oneline databricks-starter/databricks-apps/zerobus-delta-app/` |

---

## 🔗 Repository Links

- **Standalone**: https://github.com/kaustav-paul_data/zerobus-delta-app
- **Parent**: https://github.com/kaustavpaul107355/databricks-starter

---

**Last Updated**: October 10, 2025

