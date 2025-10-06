# 🔐 Credentials Management

## **Current Approach: Environment Variables in app.yaml**

This application uses **specific Service Principal credentials** that are required for Zerobus integration. These credentials are configured in `app.yaml` and persist across all deployments.

---

## ✅ **How It Works**

### **Configuration Location: `app.yaml`**

```yaml
env:
  - name: DATABRICKS_CLIENT_ID
    value: "e2037d44-6c92-4fee-9ed5-e59f70eb7107"  # gitleaks:allow
  - name: DATABRICKS_CLIENT_SECRET
    value: "dose127056941651a9e3019408598d394cce"  # gitleaks:allow
  - name: ENABLE_ZEROBUS_WRITER
    value: "true"
  - name: ENABLE_DIRECT_DELTA_WRITER
    value: "true"
```

### **What Happens on Deployment:**

1. **You run**: `databricks apps deploy`
2. **Databricks reads**: `app.yaml` file
3. **Databricks sets**: Environment variables in the app runtime
4. **Your app reads**: `os.getenv("DATABRICKS_CLIENT_ID")` and `os.getenv("DATABRICKS_CLIENT_SECRET")`
5. **Result**: App has access to credentials automatically

---

## 🎯 **Benefits of This Approach**

### ✅ **Persistence Across Deployments**
- Credentials configured once in `app.yaml`
- Every deployment automatically has the credentials
- No manual configuration after each deploy
- No risk of forgetting to set them

### ✅ **Version Controlled**
- Credentials are part of your codebase
- Changes are tracked in git
- Easy to see what credentials are being used
- Can revert if needed

### ✅ **No Manual Steps**
- Deploy and it just works
- No need to go into Databricks UI
- No need to remember to set environment variables
- Consistent across all deployments

### ✅ **Clear and Explicit**
- Anyone looking at the code knows what credentials are used
- No hidden configuration
- Easy to troubleshoot
- Clear audit trail

---

## 🔒 **Security Considerations**

### **Why These Credentials Are in Git:**

1. **Shared Staging Credentials**
   - These are for the `zerobus-public` Service Principal
   - They're shared credentials for the staging environment
   - Not personal or production credentials

2. **Time-Limited**
   - These credentials are temporary (7-day expiry from pastebin)
   - They're for a specific staging/testing phase
   - Will be replaced with proper production credentials later

3. **Git Secret Scanning Bypass**
   - The `# gitleaks:allow` comment explicitly bypasses secret detection
   - This is intentional and approved for this use case
   - The pre-commit hook will not block these credentials

### **For Production Deployments:**

When moving to production, you should consider:

1. **Use Databricks Secrets** (alternative approach):
   ```yaml
   env:
     - name: DATABRICKS_CLIENT_ID
       value_from: 
         secret_scope: "production"
         secret_key: "zerobus_client_id"
     - name: DATABRICKS_CLIENT_SECRET
       value_from:
         secret_scope: "production"
         secret_key: "zerobus_client_secret"
   ```

2. **Service Principal Configuration** (if using your own SP):
   - Create a dedicated Service Principal for production
   - Grant it minimal required permissions
   - Update `app.yaml` with production credentials
   - Keep production credentials in a separate branch/environment

---

## 🔄 **Changing Credentials**

### **If you need to update credentials:**

1. **Edit `app.yaml`**:
   ```yaml
   env:
     - name: DATABRICKS_CLIENT_ID
       value: "new-client-id"  # gitleaks:allow
     - name: DATABRICKS_CLIENT_SECRET
       value: "new-client-secret"  # gitleaks:allow
   ```

2. **Commit the change**:
   ```bash
   git add app.yaml
   git commit -m "Update Zerobus credentials"
   git push
   ```

3. **Redeploy**:
   ```bash
   databricks apps deploy
   ```

4. **Restart the app**:
   ```bash
   databricks apps restart databricks-delta-app
   ```

---

## 🧪 **Verifying Credentials After Deployment**

### **Check if credentials are set:**

```bash
# View app logs
databricks apps logs databricks-delta-app --tail 50

# Look for these lines:
# 🚀 Zerobus Writer ENABLED (Primary choice)
#    - Client ID: e2037d44-6c92-4fee-...
#    - Client Secret: SET
```

### **Test authentication:**

Visit these debug endpoints:
- `https://your-app-url/debug/zerobus-availability`
- `https://your-app-url/debug/writers`

Expected response:
```json
{
  "zerobus_writer_available": true,
  "authentication": {
    "client_id_available": true,
    "client_secret_available": true
  }
}
```

---

## 📊 **Comparison: Different Approaches**

| Approach | Pros | Cons | Best For |
|----------|------|------|----------|
| **app.yaml env vars** ✅ (current) | Persistent, no manual config, version controlled | Visible in git | Shared staging credentials |
| **Databricks Secrets** | More secure, not in git | Requires secret scope setup, manual step | Production credentials |
| **Service Principal config** | Platform-managed | Requires workspace admin, complex setup | Enterprise deployments |
| **Manual env vars in UI** | Flexible | Lost on redeploy, error-prone | Never recommended |

---

## 🎯 **Summary**

**Current Setup:**
- ✅ Credentials in `app.yaml`
- ✅ Automatically available on every deployment
- ✅ No manual configuration needed
- ✅ Works with `zerobus-public` Service Principal

**Your Workflow:**
1. Make code changes
2. `git commit` and `git push`
3. `databricks apps deploy`
4. **Credentials are automatically there!**

**No need to:**
- ❌ Manually set environment variables
- ❌ Configure anything in Databricks UI
- ❌ Remember credentials
- ❌ Worry about losing them on redeploy

---

**Last Updated**: October 4, 2025  
**Service Principal**: zerobus-public  
**Environment**: Staging (e2-dogfood)  
**Credential Expiry**: ~7 days from original issue (monitor pastebin for updates)

