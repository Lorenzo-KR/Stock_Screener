#!/bin/bash
# 매일 장 마감 후 자동 실행되는 스크립트
# launchd가 호출합니다

set -e

PROJECT="/Users/jaelokkim/개인/stock-screener"
LOG="$PROJECT/logs/screener.log"

mkdir -p "$PROJECT/logs"

echo "========================================" >> "$LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 스크리너 시작" >> "$LOG"

cd "$PROJECT"

# Python 실행 (시스템 Python3 또는 venv 사용)
if [ -f "$PROJECT/.venv/bin/python" ]; then
    PYTHON="$PROJECT/.venv/bin/python"
else
    PYTHON="/usr/bin/python3"
fi

$PYTHON scripts/screener.py >> "$LOG" 2>&1

# results.json git push
git add data/results.json
if ! git diff --staged --quiet; then
    git commit -m "data: screener results $(date +'%Y-%m-%d')" >> "$LOG" 2>&1
    git push origin main >> "$LOG" 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] git push 완료" >> "$LOG"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 변경 없음 (push 건너뜀)" >> "$LOG"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 완료" >> "$LOG"
