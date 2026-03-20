#!/bin/bash
# Ralph Loop Runner for Kiro CLI
# Based on https://github.com/snarktank/ralph
#
# Usage: ./ralph.sh [--agent name] [--max N]
# Stop:  touch STOP_RALPH

set -e

AGENT="general"
MODEL="claude-opus-4.6-1m"
MAX_ITERATIONS=0  # 0 = unlimited (use STOP_RALPH to stop)

while [[ $# -gt 0 ]]; do
  case $1 in
    --agent)
      AGENT="$2"
      shift 2
      ;;
    --agent=*)
      AGENT="${1#*=}"
      shift
      ;;
    --max)
      MAX_ITERATIONS="$2"
      shift 2
      ;;
    --max=*)
      MAX_ITERATIONS="${1#*=}"
      shift
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        MAX_ITERATIONS="$1"
      fi
      shift
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRD_FILE="$SCRIPT_DIR/prd.json"
PROGRESS_FILE="$SCRIPT_DIR/progress.txt"
PROMPT_FILE="$SCRIPT_DIR/ralph-prompt.md"
ARCHIVE_DIR="$SCRIPT_DIR/archive"
LAST_BRANCH_FILE="$SCRIPT_DIR/.last-ralph-branch"

# Archive previous run if branch changed
if [ -f "$PRD_FILE" ] && [ -f "$LAST_BRANCH_FILE" ]; then
  CURRENT_BRANCH=$(jq -r '.branchName // empty' "$PRD_FILE" 2>/dev/null || echo "")
  LAST_BRANCH=$(cat "$LAST_BRANCH_FILE" 2>/dev/null || echo "")

  if [ -n "$CURRENT_BRANCH" ] && [ -n "$LAST_BRANCH" ] && [ "$CURRENT_BRANCH" != "$LAST_BRANCH" ]; then
    DATE=$(date +%Y-%m-%d)
    FOLDER_NAME=$(echo "$LAST_BRANCH" | sed 's|^ralph/||')
    ARCHIVE_FOLDER="$ARCHIVE_DIR/$DATE-$FOLDER_NAME"

    echo "Archiving previous run: $LAST_BRANCH → $ARCHIVE_FOLDER"
    mkdir -p "$ARCHIVE_FOLDER"
    [ -f "$PRD_FILE" ] && cp "$PRD_FILE" "$ARCHIVE_FOLDER/"
    [ -f "$PROGRESS_FILE" ] && cp "$PROGRESS_FILE" "$ARCHIVE_FOLDER/"

    echo "# Codebase Patterns" > "$PROGRESS_FILE"
    echo "(Patterns discovered during implementation will be consolidated here)" >> "$PROGRESS_FILE"
    echo "" >> "$PROGRESS_FILE"
    echo "# Iteration Log" >> "$PROGRESS_FILE"
  fi
fi

# Track current branch
if [ -f "$PRD_FILE" ]; then
  CURRENT_BRANCH=$(jq -r '.branchName // empty' "$PRD_FILE" 2>/dev/null || echo "")
  [ -n "$CURRENT_BRANCH" ] && echo "$CURRENT_BRANCH" > "$LAST_BRANCH_FILE"
fi

# Initialize progress file if missing
if [ ! -f "$PROGRESS_FILE" ]; then
  echo "# Codebase Patterns" > "$PROGRESS_FILE"
  echo "(Patterns discovered during implementation will be consolidated here)" >> "$PROGRESS_FILE"
  echo "" >> "$PROGRESS_FILE"
  echo "# Iteration Log" >> "$PROGRESS_FILE"
fi

echo "Starting Ralph Loop — Agent: $AGENT — Max: ${MAX_ITERATIONS:-unlimited}"
[ -f "$PRD_FILE" ] && echo "Stories: $(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE") remaining"

trap 'echo "Interrupted."; exit 0' INT TERM
ITERATION=0

git_commit_and_push() {
  local MSG="$1"
  cd "$SCRIPT_DIR"
  if [ -n "$(git status --porcelain)" ]; then
    git add -A
    git commit -m "$MSG" || true
    git push || echo "Warning: git push failed"
  else
    echo "No changes to commit."
  fi
}

summarize() {
  local REASON="$1"
  echo ""
  echo "==============================================================="
  echo "  Ralph Summary — $REASON after $ITERATION iterations"
  echo "==============================================================="

  kiro-cli chat --agent "$AGENT" --model "$MODEL" --no-interactive --trust-all-tools \
    "Read prd.json, progress.txt, and ralph-prompt.md. The Ralph loop just stopped ($REASON after $ITERATION iterations). Append a round summary to progress.txt with these sections:

## Round Summary ($REASON, iteration $ITERATION)

### Progress
- **Completed this round**: Which stories passed and what was implemented
- **Remaining**: Which stories are still incomplete with brief status
- **Blockers**: Any recurring failures, skipped stories, or issues needing human attention

### PoC Direction Check
- **Approach validation**: Is the current approach proving viable? Any fundamental issues discovered?
- **Surprises**: What was harder/easier than expected? Any assumptions invalidated?
- **Alternative approaches**: Did anything encountered this round suggest a better path?
- **Risk areas**: What upcoming work might expose weaknesses in the current design?
- **Open questions**: Unresolved design decisions that need human input

### Recommended Next Steps
What to do when resuming the next round.

Keep it concise but substantive. Then output RALPH_SUMMARY_DONE when finished." \
    2>&1 | tee /dev/stderr

  git_commit_and_push "chore: Ralph round summary ($REASON, iteration $ITERATION)"
}

while [ ! -f "$SCRIPT_DIR/STOP_RALPH" ]; do
  ITERATION=$((ITERATION + 1))

  if [ "$MAX_ITERATIONS" -gt 0 ] && [ "$ITERATION" -gt "$MAX_ITERATIONS" ]; then
    echo "Max iterations ($MAX_ITERATIONS) reached."
    summarize "max iterations reached"
    exit 0
  fi

  REMAINING=$(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE" 2>/dev/null || echo "?")
  echo ""
  echo "==============================================================="
  echo "  Ralph Iteration $ITERATION — $REMAINING stories remaining"
  echo "==============================================================="

  OUTPUT=$(kiro-cli chat --agent "$AGENT" --model "$MODEL" --no-interactive --trust-all-tools \
    "Read ralph-prompt.md, prd.json, and progress.txt. Pick the next incomplete story, implement it, verify, update state. One task only." \
    2>&1 | tee /dev/stderr) || true

  if echo "$OUTPUT" | grep -qE '^\s*RALPH_COMPLETE\s*$'; then
    echo ""
    echo "All stories complete! Finished at iteration $ITERATION."
    summarize "all stories complete"
    exit 0
  fi

  echo "Iteration $ITERATION complete. Continuing in 2s..."
  sleep 2
done

echo "STOP_RALPH file detected. Stopping."
rm -f "$SCRIPT_DIR/STOP_RALPH"
summarize "manual stop (STOP_RALPH)"
exit 0
