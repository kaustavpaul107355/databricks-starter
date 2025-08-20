# Databricks Connect Extension Troubleshooting

## 🚨 **Extension Not Working? Let's Fix It!**

### **1. Check Extension Installation**

**In Cursor/VS Code:**
- Press `Ctrl+Shift+X` (Windows/Linux) or `Cmd+Shift+X` (Mac)
- Search for "Databricks Connect"
- Make sure it's installed and enabled
- Restart Cursor after installation

### **2. Verify Configuration Files**

**Check these files exist:**
```bash
# Main config
.databrickscfg

# VS Code/Cursor settings
.vscode/settings.json
.vscode/launch.json
```

### **3. Test Basic Connection**

**Run this command to verify Databricks Connect works:**
```bash
export DATABRICKS_HOST="your-workspace-url"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_ORG_ID="your-org-id"

databricks-connect test
```

**Expected output:**
```
* Checking Python version
* Creating and validating a session with the default configuration
* Testing the connection to the cluster
* Testing dbutils.fs
* All tests passed!
```

### **4. Check Python Environment**

**Verify Python path:**
```bash
which python
python --version
```

**Should show:**
```
/opt/anaconda3/bin/python
Python 3.x.x
```

### **5. Extension Configuration Issues**

**Common problems and solutions:**

#### **Problem: Extension can't find cluster**
**Solution:** Check `.vscode/settings.json` has correct cluster_id

#### **Problem: Authentication failed**
**Solution:** Verify token is valid and not expired

#### **Problem: Connection timeout**
**Solution:** Check if cluster is running in Databricks workspace

### **6. Manual Extension Setup**

**If automatic setup fails:**

1. **Open Command Palette** (`Ctrl+Shift+P` or `Cmd+Shift+P`)
2. **Type:** `Databricks: Configure Connection`
3. **Enter manually:**
   - Host: `your-workspace-url`
   - Token: `your-personal-access-token`
   - Cluster ID: `your-cluster-id`
   - Org ID: `your-org-id`

### **7. Test Extension Functionality**

**After setup, try these commands:**

1. **Command Palette** → `Databricks: Connect to Cluster`
2. **Command Palette** → `Databricks: Show Cluster Status`
3. **Command Palette** → `Databricks: Open Workspace`

### **8. Alternative: Use Our Scripts**

**If extension still doesn't work, use our working scripts:**

```bash
# Browse files
python list_my_files.py

# Run notebooks
python run_notebook.py notebooks/ide_integration_demo.py

# Launch IDE mode
python launch_ide_notebook.py notebooks/ide_integration_demo.py
```

### **9. Check Logs**

**Enable extension logging:**
1. **Command Palette** → `Developer: Toggle Developer Tools`
2. **Console tab** → Look for Databricks Connect errors
3. **Extensions tab** → Check Databricks Connect output

### **10. Reset Extension**

**If all else fails:**
1. **Uninstall** Databricks Connect extension
2. **Restart** Cursor
3. **Reinstall** extension
4. **Reconfigure** connection

## 🔧 **Still Having Issues?**

**Check these common problems:**

- **Cluster not running** - Start it in Databricks workspace
- **Token expired** - Generate new token
- **Wrong Python path** - Update `.vscode/settings.json`
- **Network issues** - Check firewall/proxy settings
- **Extension version** - Update to latest version

## 📞 **Need Help?**

**Your setup should work with:**
- ✅ Databricks Connect: Working
- ✅ Cluster: Running
- ✅ Token: Valid
- ✅ Configuration: Complete

**If extension still fails, the manual scripts provide the same functionality!**
