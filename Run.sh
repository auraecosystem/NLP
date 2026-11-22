#!/usr/bin/env bash
# Runtime transcript for "a Codex usage report for a finished turn no longer stalls the app-server read loop".
#
#   run.sh <tree> <label> [load_ms ...]
#
# <tree>  an extracted, installed checkout (git archive <commit> | tar -x; CI=1 pnpm install --frozen-lockfile)
# <label> printed on every line, e.g. base or fix
# load_ms one run per value: 0 = idle loop; n > 0 = every event-loop iteration also spends n ms in a busy callback
#         while the second runtime starts its turn (default: 0 2)
#
# Replays the recorded Codex fixture provider_thread_resume (two runtimes: the second resumes the thread and
# Codex restores its usage under the first runtime's last turn) and prints, for each usage report, the fiber
# tasks / event-loop dispatches / wall ms its handler took on the read loop, and the same counts from the
# restored report to the second runtime's turn/started. The probe file is copied in and removed afterwards.
set -uo pipefail
tree=${1:?tree}; label=${2:?label}; shift 2
loads=("$@"); [ ${#loads[@]} -gt 0 ] || loads=(0 2)
here=$(cd "$(dirname "$0")" && pwd)
dest="$tree/apps/server/src/orchestration-v2/testkit/codexResumeUsageTranscript.probe.test.ts"
cp "$here/codexResumeUsageTranscript.probe.test.ts" "$dest" || exit 1
trap 'rm -f "$dest"' EXIT
cd "$tree/apps/server" || exit 1
out=$(mktemp "${TMPDIR:-/tmp}/ps011-transcript-XXXXXX")
rc=0
for load in "${loads[@]}"; do
  log=$(mktemp "${TMPDIR:-/tmp}/ps011-vitest-XXXXXX")
  PS011_OUT="$out" PS011_LABEL="$label" PS011_LOAD_MS="$load" \
    npx --no-install vp test run src/orchestration-v2/testkit/codexResumeUsageTranscript.probe.test.ts > "$log" 2>&1
  r=$?
  if [ $r -ne 0 ]; then rc=$r; echo "probe run failed (load_ms=$load, exit $r):"; tail -40 "$log"; fi
  rm -f "$log"
done
cat "$out"
rm -f "$out"
exit $
