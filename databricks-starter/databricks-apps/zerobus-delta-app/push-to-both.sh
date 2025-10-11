#!/bin/bash
# Script to push changes to both git repos

set -e  # Exit on error

echo "🔄 Dual Repo Push Script"
echo "========================"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get current directory
ZEROBUS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(cd "$ZEROBUS_DIR/../../.." && pwd)"

echo -e "${BLUE}Step 1: Pushing to standalone zerobus-delta-app repo${NC}"
cd "$ZEROBUS_DIR"
git add .
if git diff --staged --quiet; then
    echo "No changes to commit in zerobus-delta-app"
else
    read -p "Enter commit message for zerobus-delta-app: " COMMIT_MSG
    git commit -m "$COMMIT_MSG"
fi
git push origin main
echo -e "${GREEN}✅ Pushed to kaustav-paul_data/zerobus-delta-app${NC}"
echo ""

echo -e "${BLUE}Step 2: Pushing to parent databricks-starter repo${NC}"
cd "$PARENT_DIR"
git add databricks-starter/databricks-apps/zerobus-delta-app/
if git diff --staged --quiet; then
    echo "No changes to commit in databricks-starter"
else
    read -p "Enter commit message for databricks-starter: " PARENT_COMMIT_MSG
    git commit -m "$PARENT_COMMIT_MSG"
fi
git push origin main
echo -e "${GREEN}✅ Pushed to kaustavpaul107355/databricks-starter${NC}"
echo ""

echo -e "${GREEN}🎉 Successfully pushed to both repos!${NC}"

