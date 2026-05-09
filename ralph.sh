#!/bin/bash
# Ralph Loop Runner — OpenSearch Analytics Engine SQL Weekend Gap Attack
#
# Dev host usage:
#   git clone <fork>
#   cd OpenSearch
#   git checkout ralph/sql-setup
#   ./ralph.sh
#
# That's it. The loop:
#   - Maintains ONE inner git worktree at ./work/ (gitignored).
#   - On each iteration, picks the next unfinished story from prd.json (by
#     priority), checks out that story's branch INSIDE ./work/, drives an
#     agent to implement and commit in ./work/, then switches ./work/ to the
#     next tier branch.
#   - Loop state (prd.json, progress.txt) stays on the ralph/sql-setup
#     branch checkout (this directory).
#
# Usage:
#   ./ralph.sh                   # start / resume the loop
#   ./ralph.sh --agent general
#   ./ralph.sh --max 5
#   ./ralph.sh --only US-T0      # force a single story
#   touch STOP_RALPH             # graceful stop
#
# Env overrides (optional):
#   RALPH_WORK_DIR     absolute path for the inner worktree (default: ./work)
#
# git push is DISABLED by default per the weekend plan. Set RALPH_PUSH=1 to
# enable pushing tier commits after each iteration.

set -e

AGENT="sisyphus"
MODEL="claude-opus-4.6-1m"
MAX_ITERATIONS=0
ONLY_STORY=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --agent)       AGENT="$2"; shift 2 ;;
    --agent=*)     AGENT="${1#*=}"; shift ;;
    --max)         MAX_ITERATIONS="$2"; shift 2 ;;
    --max=*)       MAX_ITERATIONS="${1#*=}"; shift ;;
    --only)        ONLY_STORY="$2"; shift 2 ;;
    --only=*)      ONLY_STORY="${1#*=}"; shift ;;
    -h|--help)
      sed -n '2,32p' "$0"
      exit 0 ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then MAX_ITERATIONS="$1"; fi
      shift ;;
  esac
done

# --- Locate ourselves ------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRD_FILE="$SCRIPT_DIR/prd.json"
PROGRESS_FILE="$SCRIPT_DIR/progress.txt"
PROMPT_FILE="$SCRIPT_DIR/ralph-prompt.md"

for f in "$PRD_FILE" "$PROMPT_FILE"; do
  if [ ! -f "$f" ]; then
    echo "ERROR: required file missing: $f" >&2
    exit 1
  fi
done

# Confirm we're at a git repo root (ralph/sql-setup branch or any clone of it)
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ]; then
  echo "ERROR: $SCRIPT_DIR is not inside a git repository." >&2
  exit 1
fi

WORK_DIR="${RALPH_WORK_DIR:-$SCRIPT_DIR/work}"

# --- Progress file ---------------------------------------------------------
if [ ! -f "$PROGRESS_FILE" ]; then
  printf "# Codebase Patterns\n(Patterns discovered during implementation will be consolidated here)\n\n# Iteration Log\n" > "$PROGRESS_FILE"
fi

# --- Inner-worktree management ---------------------------------------------
ensure_work_worktree() {
  local target_branch="$1"

  # Resolve the branch in three passes so we can lazy-create from main when
  # neither the local nor the remote knows about it yet.
  #   1. local ref exists       → use as-is
  #   2. remote ref on origin   → fetch into a matching local branch
  #   3. otherwise              → create locally from main / origin/main
  local local_exists=0 remote_exists=0
  if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/$target_branch"; then
    local_exists=1
  fi
  if [ "$local_exists" = "0" ]; then
    if git -C "$REPO_ROOT" ls-remote --heads origin "$target_branch" 2>/dev/null | grep -q .; then
      remote_exists=1
    fi
  fi

  if [ "$local_exists" = "0" ] && [ "$remote_exists" = "1" ]; then
    echo "Branch $target_branch not local — fetching from origin ..."
    git -C "$REPO_ROOT" fetch origin "$target_branch:$target_branch"
    local_exists=1
  fi

  if [ "$local_exists" = "0" ]; then
    # Choose a base: prefer origin/main (freshest), fall back to local main.
    local base
    if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/remotes/origin/main"; then
      base="origin/main"
    elif git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/main"; then
      base="main"
    else
      echo "ERROR: neither 'main' nor 'origin/main' exists; cannot create $target_branch." >&2
      exit 1
    fi
    echo "Branch $target_branch does not exist yet — creating from $base"
    git -C "$REPO_ROOT" branch --no-track "$target_branch" "$base"
  fi

  if [ ! -d "$WORK_DIR" ]; then
    echo "Creating inner worktree at $WORK_DIR -> $target_branch"
    git -C "$REPO_ROOT" worktree add "$WORK_DIR" "$target_branch" >/dev/null
    return
  fi

  # Existing worktree: ensure it's on the right branch, with a clean tree.
  local current
  current=$(git -C "$WORK_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
  if [ "$current" = "$target_branch" ]; then return; fi

  if [ -n "$(git -C "$WORK_DIR" status --porcelain 2>/dev/null)" ]; then
    echo "ERROR: $WORK_DIR has uncommitted changes; refusing to switch branches." >&2
    echo "       Commit or discard them manually before continuing." >&2
    exit 1
  fi
  echo "Switching $WORK_DIR: $current -> $target_branch"
  git -C "$WORK_DIR" checkout "$target_branch" >/dev/null
}

# --- Story selection -------------------------------------------------------
next_story_id() {
  if [ -n "$ONLY_STORY" ]; then
    jq -r --arg id "$ONLY_STORY" '.userStories[] | select(.id == $id and .passes == false) | .id' "$PRD_FILE" | head -n1
  else
    jq -r '[.userStories[] | select(.passes == false)] | sort_by(.priority) | .[0].id // empty' "$PRD_FILE"
  fi
}

# --- Local (no-push) commit of loop state files ----------------------------
git_commit_loop_state() {
  local MSG="$1"
  if git -C "$REPO_ROOT" status --porcelain -- "$PRD_FILE" "$PROGRESS_FILE" 2>/dev/null | grep -q .; then
    git -C "$REPO_ROOT" add -- "$PRD_FILE" "$PROGRESS_FILE" 2>/dev/null || true
    git -C "$REPO_ROOT" commit -m "$MSG" -- "$PRD_FILE" "$PROGRESS_FILE" 2>/dev/null || true
    if [ "${RALPH_PUSH:-0}" = "1" ]; then
      git -C "$REPO_ROOT" push 2>/dev/null || echo "(push failed; continuing)"
    fi
  fi
}

summarize() {
  local REASON="$1"
  echo ""
  echo "==============================================================="
  echo "  Ralph Summary — $REASON after $ITERATION iterations"
  echo "==============================================================="
  kiro-cli chat --agent "$AGENT" --model "$MODEL" --no-interactive --trust-all-tools \
    "Read $PRD_FILE, $PROGRESS_FILE, and $PROMPT_FILE. The Ralph loop just stopped ($REASON after $ITERATION iterations). Append a round summary to $PROGRESS_FILE with sections: ## Round Summary ($REASON, iteration $ITERATION) / ### Progress (completed / remaining / blockers, per-branch commit counts via 'git log --oneline main..<branch>') / ### Per-branch state / ### PoC Direction Check / ### Recommended Next Steps. DO NOT git push. Output RALPH_SUMMARY_DONE when finished." \
    2>&1 | tee /dev/stderr
  git_commit_loop_state "chore: Ralph round summary ($REASON, iteration $ITERATION)"
}

# --- Banner ----------------------------------------------------------------
echo "Repo root:   $REPO_ROOT"
echo "Work dir:    $WORK_DIR"
echo "Agent:       $AGENT"
echo "Max iters:   ${MAX_ITERATIONS:-unlimited}"
[ -n "$ONLY_STORY" ] && echo "Restricted:  $ONLY_STORY"
echo "Push:        $([ "${RALPH_PUSH:-0}" = "1" ] && echo enabled || echo DISABLED)"
REMAINING=$(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE")
echo "Stories remaining: $REMAINING"

trap 'echo "Interrupted."; exit 0' INT TERM
ITERATION=0

while [ ! -f "$SCRIPT_DIR/STOP_RALPH" ]; do
  ITERATION=$((ITERATION + 1))

  if [ "$MAX_ITERATIONS" -gt 0 ] && [ "$ITERATION" -gt "$MAX_ITERATIONS" ]; then
    echo "Max iterations ($MAX_ITERATIONS) reached."
    summarize "max iterations reached"
    exit 0
  fi

  STORY_ID="$(next_story_id)"
  if [ -z "$STORY_ID" ]; then
    echo "No incomplete stories remain."
    summarize "all stories complete"
    exit 0
  fi

  BRANCH=$(jq -r --arg id "$STORY_ID" '.userStories[] | select(.id == $id) | .branchName' "$PRD_FILE")
  ensure_work_worktree "$BRANCH"

  REMAINING=$(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE")
  echo ""
  echo "==============================================================="
  echo "  Ralph Iteration $ITERATION — $REMAINING stories remaining"
  echo "  Active story: $STORY_ID   branch: $BRANCH"
  echo "  Work dir:     $WORK_DIR"
  echo "==============================================================="

  OUTPUT=$(kiro-cli chat --agent "$AGENT" --model "$MODEL" --no-interactive --trust-all-tools \
    "Read $PROMPT_FILE, $PRD_FILE, and $PROGRESS_FILE. Active story for this iteration: $STORY_ID on branch $BRANCH. cd into the worktree at $WORK_DIR for ALL code work, builds, tests, and commits; it is already on $BRANCH. Edit loop state (prd.json / progress.txt) only in $SCRIPT_DIR. One task only. DO NOT git push — local commits only per the weekend plan." \
    2>&1 | tee /dev/stderr) || true

  # Optional push of tier commits after iteration (off by default)
  if [ "${RALPH_PUSH:-0}" = "1" ]; then
    if [ -n "$(git -C "$WORK_DIR" log "origin/$BRANCH..HEAD" --oneline 2>/dev/null)" ]; then
      echo "Pushing $BRANCH to origin ..."
      git -C "$WORK_DIR" push origin "$BRANCH" 2>&1 | tail -3 || echo "(push failed; continuing)"
    fi
  fi

  # Persist any loop-state changes the agent made
  git_commit_loop_state "chore: Ralph state update after iteration $ITERATION ($STORY_ID)"

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
