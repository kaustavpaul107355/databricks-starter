# 🔐 Authentication Guide for Databricks Direct Write App

This guide explains how authentication works in the Databricks Direct Write App and clarifies common misunderstandings.

---

## ✅ **THE CORRECT WAY: Automatic Authentication**

### **How Databricks Apps Handles Authentication**

When you deploy your app to Databricks Apps, authentication is **automatically handled** by the platform. You do **NOT** need to manually set environment variables.

### **What Databricks Apps Does Automatically:**

1. **Injects Service Principal Credentials**
   - `DATABRICKS_CLIENT_ID` → Automatically set
   - `DATABRICKS_CLIENT_SECRET` → Automatically set
   - `DATABRICKS_HOST` → Automatically set

2. **Provides User Tokens**
   - `X-Forwarded-Access-Token` → Header with user's token
   - Available for on-behalf-of-user operations

3. **Sets Up OAuth2 Flow**
   - OAuth2 token exchange handled automatically
   - Token refresh handled automatically
   - No manual token management required

---

## 🚀 **Deployment Setup (The Right Way)**

### **Method 1: Using databricks.yml (Recommended)**

Add Service Principal configuration to your `databricks.yml`:

```yaml
bundle:
  name: databricks-delta-app

workspace:
  host: https://your-workspace.cloud.databricks.com

resources:
  apps:
    databricks-delta-app:
      name: databricks-delta-app
      description: "FastAPI app for processing structured data"
      source_code_path: /Workspace/Users/${workspace.current_user.userName}/databricks-delta-app
      
      # ADD THIS: Configure Service Principal
      permissions:
        - service_principal_name: "your-service-principal-name"
          level: CAN_MANAGE
```

Then deploy:
```bash
databricks apps deploy
```

### **Method 2: Using CLI Flag**

Deploy with Service Principal specified via CLI:

```bash
databricks apps deploy --service-principal "your-service-principal-name"
```

### **Method 3: Using Databricks Apps UI**

1. Deploy your app: `databricks apps deploy`
2. Go to Databricks Apps UI
3. Select your app → Settings → Service Principal
4. Select the Service Principal from dropdown
5. Save and restart the app

---

## 🔍 **How the App Uses These Credentials**

### **Authentication Flow in the Code:**

The app automatically detects and uses credentials in this priority order:

```python
# 1. PAT Token (from Databricks Apps environment)
if DATABRICKS_TOKEN:
    use_pat_token()

# 2. Databricks SDK (automatic config detection)
elif databricks_sdk_available:
    client = WorkspaceClient()  # Automatically uses env vars
    use_sdk_token()

# 3. Service Principal OAuth2 (if CLIENT_ID and CLIENT_SECRET available)
elif DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET:
    token = get_oauth2_token(CLIENT_ID, CLIENT_SECRET)
    use_service_principal_token()
```

**Key Point**: The app code reads from environment variables that **Databricks Apps automatically sets**. You don't manually configure these.

---

## ❌ **INCORRECT: Manual Environment Variable Configuration**

### **Don't Do This:**

```bash
# ❌ WRONG: Manually setting these in Databricks Apps UI
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-secret
```

### **Why This Is Wrong:**

1. **Security Risk**: Credentials stored as plain text in UI
2. **Not Necessary**: Databricks Apps injects these automatically
3. **Maintenance Issue**: Must update manually if credentials change
4. **Against Best Practices**: Credentials should come from platform, not config

---

## ✅ **CORRECT: Platform-Managed Authentication**

### **Do This Instead:**

1. **Create Service Principal** (one-time setup):
   ```sql
   -- In Databricks SQL or via UI
   CREATE SERVICE PRINCIPAL 'zerobus-writer';
   ```

2. **Grant Permissions** (one-time setup):
   ```sql
   GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `zerobus-writer`;
   GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `zerobus-writer`;
   GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `zerobus-writer`;
   ```

3. **Configure App with Service Principal** (during deployment):
   - Option A: Add to `databricks.yml` as shown above
   - Option B: Use CLI flag during deployment
   - Option C: Configure in Databricks Apps UI after deployment

4. **Deploy**:
   ```bash
   databricks apps deploy
   ```

5. **Done!** The app automatically uses the credentials.

---

## 🧪 **Testing Authentication**

### **After Deployment, Verify Authentication Works:**

1. **Check Debug Endpoint**:
   ```bash
   curl https://your-app-url/debug/zerobus-availability
   ```

   Expected response:
   ```json
   {
     "zerobus_writer_available": true,
     "authentication": {
       "client_id_available": true,
       "client_secret_available": true,
       "token_obtainable": true
     }
   }
   ```

2. **Check App Logs**:
   ```bash
   databricks apps logs databricks-delta-app
   ```

   Look for:
   ```
   🔑 Service Principal credentials available
   ✅ Zerobus Writer is AVAILABLE
   ```

3. **Test a Write Operation**:
   - Use the Web UI to submit data
   - Select "Zerobus Writer"
   - Submit and verify success

---

## 🔧 **Troubleshooting Authentication Issues**

### **Issue: "No valid authentication token available"**

**Cause**: Service Principal not configured for the app

**Solution**:
1. Verify Service Principal exists: `databricks service-principals list`
2. Configure app with Service Principal (see methods above)
3. Restart the app: `databricks apps restart databricks-delta-app`

---

### **Issue: "Service Principal lacks permissions"**

**Cause**: Service Principal not granted access to catalog/schema/tables

**Solution**:
```sql
-- Run these SQL commands
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `<service-principal-name>`;
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `<service-principal-name>`;
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<service-principal-name>`;
```

---

### **Issue: "Zerobus Writer falls back to Mock Writer"**

**Cause**: Either authentication failed OR Zerobus SDK unavailable

**Debug Steps**:
1. Check authentication:
   ```bash
   curl https://your-app-url/debug/zerobus-availability
   ```

2. Check app logs for specific error:
   ```bash
   databricks apps logs databricks-delta-app --tail 100
   ```

3. Verify Service Principal is configured:
   ```bash
   databricks apps get databricks-delta-app --output json | jq '.service_principal'
   ```

---

## 📋 **Quick Reference: What You Need**

### **Prerequisites (One-Time Setup)**:
- [ ] Service Principal created in workspace
- [ ] Service Principal granted permissions to catalog/schema/tables
- [ ] SQL Warehouse created and running (for Direct Delta Writer)

### **Deployment (Each Deploy)**:
- [ ] Configure app with Service Principal (via databricks.yml, CLI, or UI)
- [ ] Deploy app: `databricks apps deploy`
- [ ] Verify authentication works (debug endpoints)

### **What You DON'T Need**:
- [ ] ❌ Manual environment variable configuration
- [ ] ❌ Storing credentials in config files
- [ ] ❌ Managing OAuth2 tokens manually
- [ ] ❌ Token refresh logic in your code

---

## 🎯 **Summary**

### **✅ The Right Approach:**
1. Create Service Principal
2. Grant permissions to Service Principal
3. Configure app to use Service Principal (via databricks.yml, CLI, or UI)
4. Deploy
5. **Databricks Apps automatically handles authentication**

### **❌ The Wrong Approach:**
1. ~~Manually set DATABRICKS_CLIENT_ID in environment variables~~
2. ~~Manually set DATABRICKS_CLIENT_SECRET in environment variables~~
3. ~~Store credentials in app.yaml or databricks.yml~~

---

## 💡 **Key Takeaway**

**You configure WHICH Service Principal to use, not its credentials.**

The Service Principal's credentials are managed by Databricks and automatically injected into your app's environment. Your app code reads from these automatically-set environment variables.

This is secure, maintainable, and follows Databricks Apps best practices! ✨

---

**Last Updated**: October 4, 2025  
**Related Files**: 
- `app.yaml` - Shows how credentials are automatically injected
- `DEPLOYMENT_CHECKLIST.md` - Full deployment process
- `main.py` - Authentication logic implementation
- `writers/zerobus.py` - Token factory implementation

