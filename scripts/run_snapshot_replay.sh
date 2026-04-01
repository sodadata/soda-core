#!/usr/bin/env bash
#
# Run soda-core integration tests in snapshot replay mode across all datasources.
#
# Usage:
#   ./scripts/run_snapshot_replay.sh                    # run all datasources
#   ./scripts/run_snapshot_replay.sh postgres snowflake  # run specific datasources
#   REPLAY_FALLBACK=true ./scripts/run_snapshot_replay.sh  # enable fallback on mismatch
#
# Environment variables:
#   REPLAY_FALLBACK   — set to "true" to enable snapshot fallback (default: false)
#   REPLAY_LOG_LEVEL  — pytest --log-cli-level (default: WARNING)
#   REPLAY_EXTRA_ARGS — additional pytest args passed to every run
#   REPLAY_JOBS       — max parallel background jobs (default: 8)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_BIN="${REPO_ROOT}/../soda-extensions/.venv/bin"
OUTPUT_DIR="/tmp/soda_core_snapshot_replay_results"

ALL_DATASOURCES=(
  postgres snowflake bigquery databricks duckdb redshift
  fabric synapse sqlserver athena dremio oracle trino
)

# Configuration
FALLBACK="${REPLAY_FALLBACK:-false}"
LOG_LEVEL="${REPLAY_LOG_LEVEL:-WARNING}"
EXTRA_ARGS="${REPLAY_EXTRA_ARGS:-}"
MAX_JOBS="${REPLAY_JOBS:-8}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

if [[ $# -gt 0 ]]; then
  DATASOURCES=("$@")
else
  DATASOURCES=("${ALL_DATASOURCES[@]}")
fi

mkdir -p "${OUTPUT_DIR}"
rm -f "${OUTPUT_DIR}"/*.txt

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

job_count=0

wait_for_slot() {
  while [[ "$job_count" -ge "$MAX_JOBS" ]]; do
    wait -n 2>/dev/null || true
    job_count=$((job_count - 1))
  done
  return 0
}

# -------------------------------------------------------------------------
# Launch test runs
# -------------------------------------------------------------------------

echo -e "${BOLD}soda-core Snapshot Replay Verification${NC}"
echo -e "Datasources: ${CYAN}${DATASOURCES[*]}${NC}"
echo -e "Fallback: ${FALLBACK}, Log level: ${LOG_LEVEL}"
echo ""

total_runs=0

for ds in "${DATASOURCES[@]}"; do
  label="soda-core_${ds}"
  output_file="${OUTPUT_DIR}/${label}.txt"

  wait_for_slot
  (
    cd "${REPO_ROOT}"
    env \
      SODA_TEST_SNAPSHOT=replay \
      SODA_TEST_SNAPSHOT_FALLBACK="${FALLBACK}" \
      "TEST_DATASOURCE=${ds}" \
      "${VENV_BIN}/pytest" \
      soda-tests/tests/integration/ \
      --log-cli-level="${LOG_LEVEL}" \
      -v \
      ${EXTRA_ARGS} \
      > "${output_file}" 2>&1
  ) &
  job_count=$((job_count + 1))
  total_runs=$((total_runs + 1))
done

echo -e "Launched ${BOLD}${total_runs}${NC} test runs. Waiting for completion..."
wait

# -------------------------------------------------------------------------
# Collect and report results
# -------------------------------------------------------------------------

echo ""
echo -e "${BOLD}========== Results ==========${NC}"
echo ""

passed_runs=0
failed_runs=0
failed_labels=()

for output_file in "${OUTPUT_DIR}"/*.txt; do
  [[ -f "$output_file" ]] || continue
  label=$(basename "$output_file" .txt)

  summary=$(grep -E '=+ .*(passed|failed|error)' "$output_file" | tail -1 || true)
  extract_count() { local pattern="$1"; local result; result=$(echo "$summary" | grep -oE "[0-9]+ ${pattern}" | head -1 | grep -oE '[0-9]+' || echo "0"); echo "$result"; return 0; }
  n_passed=$(extract_count passed)
  n_failed=$(extract_count failed)
  n_skipped=$(extract_count skipped)
  n_errors=$(extract_count error)
  n_fallbacks=$(grep -c 'SNAPSHOT: Falling back to real DB' "$output_file" 2>/dev/null || echo "0")

  has_failure=false
  if [[ "${n_failed:-0}" -gt 0 ]] || [[ "${n_errors:-0}" -gt 0 ]]; then
    has_failure=true
  fi
  if [[ -z "$summary" ]] && grep -qE '^(ERROR|ERRORS|E )' "$output_file" 2>/dev/null; then
    has_failure=true
    n_errors="?"
  fi

  if $has_failure; then
    status="${RED}FAIL${NC}"
    failed_runs=$((failed_runs + 1))
    failed_labels+=("$label")
  else
    status="${GREEN}PASS${NC}"
    passed_runs=$((passed_runs + 1))
  fi

  fallback_info=""
  if [[ "${n_fallbacks:-0}" -gt 0 ]]; then
    fallback_info=" ${YELLOW}(${n_fallbacks} fallbacks)${NC}"
  fi

  printf "  %-45s %b  passed=%-4s failed=%-4s skipped=%-4s%b\n" \
    "$label" "$status" "${n_passed:-0}" "${n_failed:-0}" "${n_skipped:-0}" "$fallback_info"
done

echo ""
echo -e "${BOLD}Summary:${NC} ${GREEN}${passed_runs} passed${NC}, ${RED}${failed_runs} failed${NC} (out of ${total_runs} runs)"

if [[ ${#failed_labels[@]} -gt 0 ]]; then
  echo ""
  echo -e "${RED}Failed runs:${NC}"
  for label in "${failed_labels[@]}"; do
    echo "  - ${label} (see ${OUTPUT_DIR}/${label}.txt)"
  done
  echo ""
  echo -e "To investigate: ${CYAN}cat ${OUTPUT_DIR}/<label>.txt${NC}"
  exit 1
fi

echo ""
echo -e "${GREEN}All snapshot replay tests passed.${NC}"
