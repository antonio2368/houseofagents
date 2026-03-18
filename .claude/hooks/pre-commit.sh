#!/bin/bash
# Block git commit if cargo fmt or clippy fail

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command')

# Only check git commit commands
if ! echo "$COMMAND" | rg -q 'git commit'; then
  exit 0
fi

ERRORS=""

if ! cargo fmt --check 2>&1; then
  ERRORS="cargo fmt check failed. Run 'cargo fmt' to fix formatting."
fi

if ! cargo clippy --all-targets --all-features -- -D warnings 2>&1; then
  ERRORS="${ERRORS:+$ERRORS\n}cargo clippy failed. Fix lint warnings first."
fi

if [ -n "$ERRORS" ]; then
  jq -n --arg reason "$ERRORS" '{
    "hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": $reason
    }
  }'
  exit 0
fi

exit 0
