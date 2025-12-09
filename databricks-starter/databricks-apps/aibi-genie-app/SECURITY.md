# 🔐 Security Guidelines for AIBI Genie App

**CRITICAL**: This document contains important security information. Please read before committing code.

---

## ⚠️ IMPORTANT: Credentials Management

### **Files with Sensitive Information**

The following file contains **sensitive credentials** and should **NEVER be committed to git**:

- `app.yaml` - Contains `DATABRICKS_TOKEN_FOR_GENIE`

### **Git Protection**

✅ **Already Protected:**
- `app.yaml` is listed in `.gitignore` to prevent accidental commits
- A template file `app.yaml.template` is provided for reference

⚠️ **Before Committing:**
```bash
# ALWAYS check before git commit
git status

# If you see app.yaml in the list, DO NOT COMMIT IT
# Verify .gitignore is working:
git check-ignore app.yaml
# Should output: app.yaml
```

---

## 🔑 Token Management

### **Current Token Setup**

```yaml
DATABRICKS_TOKEN_FOR_GENIE: Used for Genie Space API calls
- Purpose: Authenticate Genie API requests
- Scopes needed: Genie Space access, SQL access
- Location: app.yaml (workspace only, not in git)
```

### **How to Set Up Tokens**

#### **For Databricks Workspace Deployment:**

1. **Update Local app.yaml** (already done):
   ```yaml
   - name: "DATABRICKS_TOKEN_FOR_GENIE"
     value: "dapi..."  # Your actual token
   ```

2. **Sync to Workspace**:
   ```bash
   databricks sync . /Workspace/Users/kaustav.paul@databricks.com/aibi-genie-app --profile DEFAULT
   ```

3. **Deploy**:
   ```bash
   ./deploy.sh
   ```

#### **For Local Development:**

Update `run_local.sh` or set environment variables:
```bash
export DATABRICKS_TOKEN_FOR_GENIE="dapi..."
./run_local.sh
```

---

## 📋 Security Checklist

### **Before Every Git Commit:**

- [ ] Verify `app.yaml` is NOT staged for commit
- [ ] Check that `.gitignore` includes `app.yaml`
- [ ] Ensure no tokens are in any committed files
- [ ] Review diff: `git diff --cached`
- [ ] Use template file for sharing: `app.yaml.template`

### **Token Rotation Best Practices:**

- [ ] Rotate tokens every 90 days (or per policy)
- [ ] Use separate tokens for dev/staging/prod
- [ ] Revoke tokens when no longer needed
- [ ] Monitor token usage in workspace logs

### **If Token is Accidentally Committed:**

1. **Immediately revoke the token** in Databricks workspace
2. Generate a new token
3. Update `app.yaml` with new token
4. Remove from git history:
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch app.yaml" \
     --prune-empty --tag-name-filter cat -- --all
   ```
5. Force push (coordinate with team)
6. Notify security team

---

## 🔒 Alternative Security Approaches

### **Option 1: Environment Variables (Recommended for Production)**

Instead of storing in `app.yaml`, use environment variables:

```python
# In app.py
import os
token = os.getenv('DATABRICKS_TOKEN_FOR_GENIE')
```

Set in Databricks workspace settings or CI/CD pipeline.

### **Option 2: Databricks Secrets**

Use Databricks Secrets for production deployments:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
token = w.secrets.get_secret(scope="app-secrets", key="genie-token")
```

### **Option 3: Service Principal (OAuth)**

For SQL connections, the app already uses OAuth service principal (automatic in Databricks Apps).

---

## 🚨 What NOT to Do

❌ **NEVER:**
- Commit `app.yaml` with real tokens to git
- Share tokens in Slack, email, or other insecure channels
- Use the same token for dev and production
- Hardcode tokens in Python code
- Store tokens in plain text files in the repo
- Push tokens to public repositories

✅ **ALWAYS:**
- Use `.gitignore` to exclude sensitive files
- Use `app.yaml.template` for sharing configuration
- Rotate tokens regularly
- Use separate tokens per environment
- Store tokens securely (secrets manager, environment variables)
- Review git diffs before committing

---

## 📝 File Structure

```
aibi-genie-app/
├── app.yaml              ← ⚠️ NOT in git (has real token)
├── app.yaml.template     ← ✅ In git (placeholder token)
├── .gitignore           ← ✅ Protects app.yaml
├── SECURITY.md          ← ✅ This file
└── ...
```

---

## 🔍 Verifying Security

### **Check Git Status:**
```bash
cd aibi-genie-app
git status

# app.yaml should NOT appear in the list
# If it does, it means .gitignore is not working
```

### **Check .gitignore:**
```bash
git check-ignore -v app.yaml
# Should output: .gitignore:4:app.yaml    app.yaml
```

### **Check What Will Be Committed:**
```bash
git add .
git status
# Verify app.yaml is NOT in "Changes to be committed"
```

---

## 📞 Questions or Issues?

- **Token Issues**: Contact Databricks workspace admin
- **Security Concerns**: Contact security team
- **App Issues**: See README.md or TROUBLESHOOTING.md

---

## 🎯 Quick Reference

| Action | Command |
|--------|---------|
| **Generate Token** | Databricks UI → User Settings → Developer → Access Tokens |
| **Update Local** | Edit `app.yaml` with token |
| **Sync to Workspace** | `databricks sync . /Workspace/Users/.../aibi-genie-app --profile DEFAULT` |
| **Deploy** | `./deploy.sh` |
| **Verify .gitignore** | `git check-ignore app.yaml` |
| **Check Git Status** | `git status` (app.yaml should NOT appear) |

---

**Last Updated**: December 8, 2025  
**Maintained By**: Kaustav Paul (kaustav.paul@databricks.com)  
**Security Classification**: INTERNAL - Contains Sensitive Configuration Instructions

