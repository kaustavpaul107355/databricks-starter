#!/usr/bin/env bash
set -euo pipefail

HOST="https://e2-demo-field-eng.cloud.databricks.com"
TOKEN="${DATABRICKS_TOKEN:-}"
LOCAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_PATH="/Workspace/Users/kaustav.paul@databricks.com/ace-demo"

if [ -z "$TOKEN" ]; then
    echo "Error: DATABRICKS_TOKEN environment variable is required."
    echo "Usage: DATABRICKS_TOKEN=<your-token> $0"
    exit 1
fi

echo "Syncing ${LOCAL_PATH} -> ${WORKSPACE_PATH}"
echo ""

# Helper function to upload a file
upload_file() {
    local file_path="$1"
    local relative_path="${file_path#${LOCAL_PATH}/}"
    local target_path="${WORKSPACE_PATH}/${relative_path}"
    
    # Read file content and base64 encode
    local content=$(base64 -i "$file_path")
    
    # Create JSON payload
    local json_payload=$(cat <<EOF
{
  "path": "${target_path}",
  "content": "${content}",
  "overwrite": true,
  "format": "AUTO"
}
EOF
)
    
    # Upload via API
    curl -s --insecure \
        -X POST \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "${json_payload}" \
        "${HOST}/api/2.0/workspace/import" > /dev/null
    
    echo "✅ ${relative_path}"
}

# Create workspace directories
echo "Creating workspace directories..."
for dir in "" "/pipelines" "/scripts"; do
    curl -s --insecure \
        -X POST \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"path\": \"${WORKSPACE_PATH}${dir}\"}" \
        "${HOST}/api/2.0/workspace/mkdirs" > /dev/null
done

echo ""
echo "Uploading files..."

# Upload all Python and SQL files from pipelines/
for file in "${LOCAL_PATH}/pipelines"/*.py "${LOCAL_PATH}/pipelines"/*.sql; do
    if [ -f "$file" ]; then
        upload_file "$file"
    fi
done

# Upload all Python files from scripts/
for file in "${LOCAL_PATH}/scripts"/*.py; do
    if [ -f "$file" ]; then
        upload_file "$file"
    fi
done

# Upload README
if [ -f "${LOCAL_PATH}/README.md" ]; then
    upload_file "${LOCAL_PATH}/README.md"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Sync complete!"
echo ""
echo "🔗 View in workspace:"
echo "   ${HOST}/workspace${WORKSPACE_PATH}"
echo ""
