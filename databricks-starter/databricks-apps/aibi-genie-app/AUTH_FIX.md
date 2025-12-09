# Authentication Fix - URGENT

**Date**: December 8, 2025  
**Issue**: OAuth/PAT conflict causing authentication errors  
**Status**: ✅ FIXED

---

## 🚨 Problem

When deploying to Databricks Apps, the application was getting this error:

```
Error: validate: more than one authorization method configured: oauth and pat. 
Config: host=https://..., token=, client_id=..., client_secret=...
```

**Root Cause:**
- Databricks Apps automatically provides OAuth credentials (`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`)
- Our `app.yaml` also had `DATABRICKS_TOKEN` configured
- The Databricks SDK detected BOTH authentication methods and rejected the configuration

---

## ✅ Solution

### **Authentication Strategy:**

1. **SQL Connections (via databricks-sql-connector)**:
   - **Databricks Apps**: Use OAuth service principal (automatic, no token needed)
   - **Local Development**: Use PAT token from `DATABRICKS_TOKEN`

2. **Genie API Calls (via REST API)**:
   - **Always use PAT token** from `DATABRICKS_TOKEN_FOR_GENIE`
   - Genie API requires PAT with specific scopes (doesn't use OAuth)

---

## 🔧 Changes Made

### 1. **app.yaml** - Updated Configuration

**BEFORE (Caused Conflict):**
```yaml
- name: "DATABRICKS_TOKEN"
  value: "YOUR_DATABRICKS_TOKEN_HERE"
- name: "DATABRICKS_TOKEN_FOR_GENIE"
  value: "YOUR_GENIE_SPECIFIC_TOKEN_HERE"
```

**AFTER (Fixed):**
```yaml
# Do NOT set DATABRICKS_TOKEN for Databricks Apps (OAuth is automatic)
# Only set token for Genie API (which requires PAT)
- name: "DATABRICKS_TOKEN_FOR_GENIE"
  value: "YOUR_DATABRICKS_TOKEN_HERE"
```

### 2. **app.py** - Enhanced credential_provider()

```python
def credential_provider():
    """
    Uses OAuth service principal in production, PAT token for local development.
    
    IMPORTANT: When OAuth is available (Databricks Apps), do NOT set DATABRICKS_TOKEN
    in environment as it will cause conflicts. OAuth takes precedence.
    """
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    if client_id and client_secret:
        # Databricks Apps - use OAuth (no token!)
        logger.info("Using OAuth service principal authentication for SQL")
        config = Config(
            host=f'https://{os.getenv("DATABRICKS_HOST")}',
            client_id=client_id,
            client_secret=client_secret
        )
        return oauth_service_principal(config)
    else:
        # Local development - use PAT token
        logger.info("Using PAT token authentication for SQL")
        token = get_auth_token('sql')
        if not token:
            raise ValueError("No authentication credentials found.")
        return lambda: token
```

### 3. **run_local.sh** - Local Development Setup

For local development (no OAuth), both tokens are needed:
```bash
export DATABRICKS_TOKEN="YOUR_TOKEN_HERE"  # For SQL
export DATABRICKS_TOKEN_FOR_GENIE="YOUR_TOKEN_HERE"  # For Genie (can be same)
```

---

## 📋 Environment Variable Summary

| Variable | Databricks Apps | Local Development | Purpose |
|----------|----------------|-------------------|---------|
| `DATABRICKS_CLIENT_ID` | ✅ Auto-provided | ❌ Not set | OAuth for SQL |
| `DATABRICKS_CLIENT_SECRET` | ✅ Auto-provided | ❌ Not set | OAuth for SQL |
| `DATABRICKS_TOKEN` | ❌ **DO NOT SET** | ✅ Required | PAT for SQL (local only) |
| `DATABRICKS_TOKEN_FOR_GENIE` | ✅ Required | ✅ Required | PAT for Genie API |

---

## 🚀 Deployment Instructions

### **Step 1: Update app.yaml**

Make sure your `app.yaml` has:
- ❌ **NO** `DATABRICKS_TOKEN` variable
- ✅ **YES** `DATABRICKS_TOKEN_FOR_GENIE` with valid PAT

### **Step 2: Sync to Workspace**

```bash
cd /path/to/aibi-genie-app
databricks sync . /Workspace/Users/kaustav.paul@databricks.com/aibi-genie-app --profile DEFAULT --full
```

### **Step 3: Redeploy the App**

```bash
databricks apps deploy aibi-genie-app \
  --source-code-path /Workspace/Users/kaustav.paul@databricks.com/aibi-genie-app \
  --profile DEFAULT
```

### **Step 4: Verify**

1. **Check Source Data tab**: Should load data without authentication errors
2. **Check Genie tab**: Should accept queries without authentication errors
3. **Check logs**: 
   ```bash
   databricks apps logs aibi-genie-app --profile DEFAULT --tail
   ```
   Should see: "Using OAuth service principal authentication for SQL"

---

## 🧪 Testing Checklist

After redeployment:

- [ ] ✅ Source Data tab loads successfully
- [ ] ✅ Pagination works (Previous/Next buttons)
- [ ] ✅ Genie Space accepts queries
- [ ] ✅ No authentication errors in logs
- [ ] ✅ User information displays in sidebar
- [ ] ✅ AI/BI Dashboard tab loads

---

## 🔍 Troubleshooting

### If Source Data Tab Still Fails:

1. **Check logs for OAuth confirmation:**
   ```bash
   databricks apps logs aibi-genie-app --profile DEFAULT | grep -i "oauth"
   ```
   Should see: "Using OAuth service principal authentication for SQL"

2. **Verify no token is set:**
   ```bash
   databricks apps get aibi-genie-app --profile DEFAULT --output json | jq '.env'
   ```
   Should NOT see `DATABRICKS_TOKEN` in the list

3. **Check SQL Warehouse resource:**
   ```bash
   databricks apps get aibi-genie-app --profile DEFAULT --output json | jq '.resources'
   ```
   Should show sql-warehouse resource with correct ID

### If Genie Tab Still Fails:

1. **Verify token is set:**
   ```bash
   databricks apps get aibi-genie-app --profile DEFAULT --output json | jq '.env[] | select(.name=="DATABRICKS_TOKEN_FOR_GENIE")'
   ```
   Should show the token configuration

2. **Check token permissions:**
   - Token must have permissions for Genie Space
   - Token must have required scopes (check with workspace admin)

3. **Test token manually:**
   ```bash
   curl -H "Authorization: Bearer YOUR_TOKEN" \
     https://e2-demo-field-eng.cloud.databricks.com/api/2.0/genie/spaces/01f050501b7912148a8ee89a422369d6
   ```

---

## 📝 Key Takeaways

1. **Databricks Apps use OAuth automatically** - don't fight it by setting tokens
2. **Separate authentication for SQL vs API** - OAuth for SQL, PAT for Genie
3. **Local development is different** - needs PAT tokens for everything
4. **Test after deployment** - verify both tabs work correctly

---

## 🆘 Rollback (If Needed)

If issues persist, you can temporarily rollback by:

1. Reverting to single token approach (less secure but works):
   ```yaml
   - name: "DATABRICKS_TOKEN"
     value: "YOUR_TOKEN_HERE"
   ```

2. But this will cause the OAuth conflict again. Better to fix the root cause.

---

**Fixed by**: AI Assistant (Cursor)  
**Verified by**: Pending user testing  
**Priority**: 🔥 CRITICAL - Blocks app functionality

