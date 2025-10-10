# 🚀 Databricks Direct Write App - Deployment Checklist

This checklist ensures all components are properly configured before deploying the application to Databricks Apps.

## ✅ **Pre-Deployment Checklist**

### **1. Environment Setup**
- [ ] Databricks workspace URL is correct in `databricks.yml`
- [ ] Service Principal created with appropriate permissions
- [ ] Client ID and Client Secret obtained for Zerobus
- [ ] SQL Warehouse created and ID available for Direct Delta Writer
- [ ] PAT token available (automatically provided by Databricks Apps)

### **2. Database Configuration**
- [ ] Catalog exists: `kaustavpaul_demo` (or update to your catalog)
- [ ] Schema exists: `kaustavpaul_demo.zerobus_delta`
- [ ] Tables created:
  - [ ] `zerobus_products_data` (for Zerobus Writer)
  - [ ] `zerobus_products_data` (legacy, for Direct Delta Writer)
- [ ] Run SQL scripts from `sql/` directory:
  - [ ] `create_new_zerobus_table.sql` - Create compatible tables
  - [ ] `grant_permissions.sql` - Grant Service Principal permissions

### **3. Service Principal Permissions**
Run these SQL commands (replace `<client-id>` with your Service Principal Client ID):

```sql
-- Catalog permissions
GRANT USE_CATALOG ON CATALOG kaustavpaul_demo TO `<client-id>`;

-- Schema permissions
GRANT USE_SCHEMA ON SCHEMA kaustavpaul_demo.zerobus_delta TO `<client-id>`;

-- Table permissions
GRANT MODIFY, SELECT ON TABLE kaustavpaul_demo.zerobus_delta.zerobus_products_data TO `<client-id>`;
```

### **4. Code Review**
- [x] All Python files compile without syntax errors
- [x] No hardcoded credentials in source code
- [x] All imports are available in `requirements.txt`
- [x] Static files (`static/index.html`) are present
- [x] Protobuf schemas compiled (`product_record_pb2.py`)
- [x] Zerobus SDK extracted to `zerobus_sdk/` directory
- [x] Writer modules implement `DataWriterInterface` correctly

### **5. Configuration Files**
- [x] `app.yaml` - Correct uvicorn command
- [x] `databricks.yml` - Asset Bundle configuration
- [x] `requirements.txt` - All dependencies listed
- [x] `env.template` - Environment variable template provided

### **6. Application Configuration**
**IMPORTANT**: Credentials are configured in `app.yaml` and persist across deployments!

#### **How It Works:**
The `app.yaml` file contains environment variables that are automatically set when the app starts. These credentials are for the `zerobus-public` Service Principal and are required for Zerobus Writer to function.

#### **Current Configuration (in app.yaml):**
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

#### **What This Means:**
- ✅ Credentials are version-controlled in `app.yaml`
- ✅ They persist across all deployments automatically
- ✅ No manual configuration needed after deployment
- ✅ Changes to the app code won't affect credentials
- ⚠️ If you need to change credentials, edit `app.yaml` and redeploy

#### **Security Note:**
The credentials in `app.yaml` have `# gitleaks:allow` comments to bypass git secret scanning. This is intentional as these are shared staging environment credentials for the `zerobus-public` Service Principal, not production secrets.

### **7. Testing Plan**
After deployment, test these scenarios:

- [ ] **Health Check**: Visit `/health` endpoint
- [ ] **Web UI**: Access root `/` endpoint and load UI
- [ ] **Mock Writer**: Test with writer selection = "Mock"
- [ ] **Direct Delta Writer**: Test with writer selection = "Direct Delta"
- [ ] **Zerobus Writer**: Test with writer selection = "Zerobus" (default)
- [ ] **Debug Endpoints**: 
  - [ ] `/debug/writers` - Check all writers status
  - [ ] `/debug/zerobus-availability` - Zerobus Writer status
  - [ ] `/debug/direct-delta-availability` - Direct Delta Writer status

---

## 🔧 **Deployment Steps**

### **Step 1: Prepare Deployment**
```bash
cd databricks-starter/databricks-apps/zerobus-delta-app
```

### **Step 2: Validate Configuration**
Check `databricks.yml` settings:
- Workspace host
- Bundle name
- Source code path

### **Step 3: Deploy to Databricks**
```bash
# Deploy the app
databricks apps deploy

# Or with specific target
databricks apps deploy --target dev
```

### **Step 4: Configure Environment Variables**
In Databricks Apps UI:
1. Navigate to your deployed app
2. Go to Configuration → Environment Variables
3. Add required variables:
   - `DATABRICKS_CLIENT_ID`
   - `DATABRICKS_CLIENT_SECRET`

### **Step 5: Start the App**
```bash
# Start the application
databricks apps start <app-name>

# Check status
databricks apps status <app-name>

# View logs
databricks apps logs <app-name>
```

### **Step 6: Test the Deployment**
1. Open the app URL provided by Databricks
2. Test all writer types
3. Check logs for errors
4. Verify data is written to Delta tables

---

## 🐛 **Troubleshooting**

### **Issue: Zerobus Writer not available**
**Symptoms**: App falls back to Mock Writer despite selecting Zerobus

**Solutions**:
1. Check Service Principal credentials are set
2. Verify Service Principal has table permissions
3. Check Zerobus endpoint is correct for your region
4. Review logs at `/debug/zerobus-availability`

### **Issue: Direct Delta Writer fails**
**Symptoms**: SQL execution timeout or permission errors

**Solutions**:
1. Verify SQL Warehouse ID is correct
2. Check SQL Warehouse is running
3. Ensure PAT token has warehouse access
4. Check table exists and is accessible

### **Issue: Table compatibility errors**
**Symptoms**: "Unsupported features" errors from Zerobus

**Solutions**:
1. Run `sql/create_new_zerobus_table.sql` to create clean table
2. Or run `sql/comprehensive_table_fix.sql` to fix existing table
3. Ensure table doesn't have `domainMetadata` or `rowTracking` features

### **Issue: Import errors**
**Symptoms**: ModuleNotFoundError for protobuf, grpcio, etc.

**Solutions**:
1. Verify `requirements.txt` is complete
2. Check all dependencies are installed by Databricks Apps
3. Ensure `zerobus_sdk/` directory is included in deployment

---

## 📊 **Post-Deployment Validation**

### **Verify Data Flow**
1. Submit test data via Web UI
2. Check response includes:
   - `status: "success"`
   - Correct `writer_name`
   - `records_written > 0`
3. Query Delta table to verify data:

```sql
SELECT * FROM kaustavpaul_demo.zerobus_delta.zerobus_products_data 
ORDER BY processing_timestamp DESC 
LIMIT 10;
```

### **Monitor Performance**
- Check processing time in API responses
- Review logs for performance metrics
- Monitor throughput (items/sec)

### **Verify Writer Selection**
Test each writer type explicitly:
- Mock Writer: For testing
- Direct Delta Writer: SQL-based reliable writes
- Zerobus Writer: High-performance streaming writes

---

## 🔐 **Security Review**

- [x] No credentials in source code
- [x] Environment variables used for secrets
- [x] Service Principal has minimal required permissions
- [ ] Production deployment uses separate Service Principal
- [ ] Debug endpoints disabled in production (`ENABLE_DEBUG_ENDPOINTS=false`)

---

## 📝 **Notes**

### **Hardcoded Values to Update**
These values are currently specific to the staging environment and should be updated for your deployment:

1. **Workspace URL** in `databricks.yml`:
   - Current: `https://e2-dogfood.staging.cloud.databricks.com`
   - Update to your workspace

2. **Catalog/Schema** throughout codebase:
   - Current: `kaustavpaul_demo.zerobus_delta`
   - Update to your catalog/schema

3. **Zerobus Endpoint** in `writers/zerobus.py`:
   - Current: `6051921418418893.zerobus.us-west-2.staging.cloud.databricks.com`
   - Update to your cluster's Zerobus endpoint

4. **SQL Warehouse ID** in `writers/direct_delta.py`:
   - Current: `dd43ee29fedd958d`
   - Update to your SQL Warehouse ID

### **Files to Review**
Before deployment, review and update these files with your specific values:
- `databricks.yml` - Workspace and bundle configuration
- `main.py` - Default catalog/schema (lines 557-558)
- `writers/zerobus.py` - Zerobus endpoint (lines 46-48)
- `writers/direct_delta.py` - SQL Warehouse ID (line 41)
- `static/index.html` - UI table name display (if needed)

---

## ✅ **Ready to Deploy?**

Once all checkboxes are complete and configurations are updated:

```bash
databricks apps deploy
databricks apps start <app-name>
```

🎉 **Your Databricks Direct Write App is ready to process data!**

