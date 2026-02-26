#!/bin/bash
# =============================================================================
# Data Lakehouse — Full Optimization Test Suite
#
# Tests every active optimization and clearly reports the timing impact of each.
#
# Optimizations covered:
#   #1  Spark warm-up      — first job no longer pays ~5s cold-start penalty
#   #2  Schema Redis cache — MISS → Spark (~5s) | HIT → Redis (<100ms)
#                            cache is invalidated after every ingestion write
#   #4  Redis Pub/Sub      — /wait endpoints replace client-side polling loops
#
# API endpoints used:
#   POST /api/v1/ingestion/upload          (file, userId, projectId, tableName)
#   GET  /api/v1/ingestion/status/{jobId}
#   POST /api/v1/query                     (JSON body)
#   GET  /api/v1/query/{jobId}
#   GET  /api/v1/schema/{source}           source = "projectId.tableName"
#   GET  /api/v1/schema/status/{jobId}
# =============================================================================

API_BASE="http://localhost:8080"
USER_ID="user1"
PROJECT_ID="opt_test"
TABLE="perf_data"
SOURCE="${PROJECT_ID}.${TABLE}"

# ── ANSI colours ──────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

# ── Timing helpers ────────────────────────────────────────────────────────────
ts()         { date +%s%3N; }
elapsed_ms() { echo $(( $(ts) - $1 )); }

# ── Section header ────────────────────────────────────────────────────────────
section() {
    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

pass()  { echo -e "${GREEN}  ✓ $*${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $*${NC}"; }
label() { echo -e "${DIM}  $*${NC}"; }

# ── Timing store ──────────────────────────────────────────────────────────────
declare -A T
record() { T["$1"]=$2; }   # record <key> <ms>

# ── Wait for ingestion via /wait endpoint (long-poll, no polling loop) ────────
wait_ingestion() {
    local job=$1 timeout=${2:-300}
    label "Waiting (long-poll): $job  (timeout ${timeout}s)"
    local resp status
    resp=$(curl -s --max-time "$timeout" "${API_BASE}/api/v1/ingestion/status/${job}/wait?timeoutSec=${timeout}")
    status=$(echo "$resp" | jq -r '.status // "UNKNOWN"')
    case "$status" in
        COMPLETED) pass "COMPLETED"; return 0 ;;
        FAILED)    warn "FAILED"; echo "$resp" | jq '.'; return 1 ;;
        *)         warn "Timed out or unexpected: $status"; return 1 ;;
    esac
}

# ── Wait for query via /wait endpoint ─────────────────────────────────────────
wait_query() {
    local job=$1 timeout=${2:-300}
    label "Waiting (long-poll): $job  (timeout ${timeout}s)"
    local resp status
    resp=$(curl -s --max-time "$timeout" "${API_BASE}/api/v1/query/${job}/wait?timeoutSec=${timeout}")
    status=$(echo "$resp" | jq -r '.status // "UNKNOWN"')
    case "$status" in
        COMPLETED) pass "COMPLETED"; return 0 ;;
        FAILED)    warn "FAILED"; echo "$resp" | jq '.'; return 1 ;;
        *)         warn "Timed out or unexpected: $status"; return 1 ;;
    esac
}

# ── Wait for schema via /wait endpoint ────────────────────────────────────────
wait_schema() {
    local job=$1 timeout=${2:-60}
    label "Waiting (long-poll): $job  (timeout ${timeout}s)"
    local resp status
    resp=$(curl -s --max-time "$timeout" "${API_BASE}/api/v1/schema/status/${job}/wait?timeoutSec=${timeout}")
    status=$(echo "$resp" | jq -r '.status // "UNKNOWN"')
    case "$status" in
        COMPLETED) pass "COMPLETED"; return 0 ;;
        FAILED)    warn "FAILED"; echo "$resp" | jq '.'; return 1 ;;
        *)         warn "Timed out or unexpected: $status"; return 1 ;;
    esac
}

# ── Submit a schema job, wait, echo worker ms, return ms via stdout ───────────
do_schema() {
    local desc=$1
    label "$desc"
    local t0; t0=$(ts)
    local resp job
    resp=$(curl -s "${API_BASE}/api/v1/schema/${SOURCE}")
    job=$(echo "$resp" | jq -r '.jobId')
    local http_ms; http_ms=$(elapsed_ms $t0)
    label "jobId: $job  (HTTP ${http_ms} ms)"

    local t1; t1=$(ts)
    wait_schema "$job"
    local worker_ms; worker_ms=$(elapsed_ms $t1)

    local msg
    msg=$(curl -s "${API_BASE}/api/v1/schema/status/${job}" | jq -r '.message // ""')
    echo -e "${GREEN}  ⏱  worker: ${worker_ms} ms${NC}   ${DIM}[${msg}]${NC}"

    # Return ms via a temp file so the caller can read it without subshell
    echo "$worker_ms" > /tmp/_schema_ms
}

# =============================================================================
#  SETUP — Generate test CSVs with Python
# =============================================================================
section "Setup — Clean state + generate test CSVs"

label "Flushing stale schema cache from Redis..."
REDIS_PASS="changeme_in_production"
flush_result=$(docker exec redis redis-cli -a "$REDIS_PASS" --no-auth-warning DEL "schema:iceberg.${PROJECT_ID}.${TABLE}" 2>&1)
pass "Redis schema cache flush: ${flush_result}"

label "Dropping leftover Iceberg table via REST catalog..."
drop_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X DELETE "http://localhost:8181/v1/namespaces/${PROJECT_ID}/tables/${TABLE}")
if [ "$drop_code" = "204" ] || [ "$drop_code" = "200" ]; then
    pass "Table ${SOURCE} dropped  (HTTP ${drop_code})"
elif [ "$drop_code" = "404" ]; then
    pass "Table ${SOURCE} did not exist  (HTTP 404 — clean slate)"
else
    warn "Unexpected drop response: HTTP ${drop_code}"
fi
echo ""

t0=$(ts)
python3 - <<'PYEOF'
import csv, random

products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]

# CSV 1: 1 000 rows — initial load (5 columns)
with open("t1_initial.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "product", "price", "quantity", "date"])
    for i in range(1, 1001):
        w.writerow([i, products[i%5],
                    round(random.uniform(10, 1000), 2),
                    random.randint(1, 100),
                    f"2024-01-{(i%28)+1:02d}"])

# CSV 2: 500 rows — append (same 5 columns)
with open("t2_append.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "product", "price", "quantity", "date"])
    for i in range(1001, 1501):
        w.writerow([i, products[i%5],
                    round(random.uniform(10, 1000), 2),
                    random.randint(1, 100),
                    f"2024-02-{(i%28)+1:02d}"])

# CSV 3: 100 rows with an EXTRA column — triggers schema invalidation
with open("t3_new_column.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "product", "price", "quantity", "date", "region"])
    for i in range(1501, 1601):
        w.writerow([i, products[i%5],
                    round(random.uniform(10, 1000), 2),
                    random.randint(1, 100),
                    f"2024-03-{(i%28)+1:02d}",
                    random.choice(["EU", "US", "APAC"])])

print("  t1_initial.csv    : 1 000 rows, 5 cols")
print("  t2_append.csv     :   500 rows, 5 cols")
print("  t3_new_column.csv :   100 rows, 6 cols  (adds 'region')")
PYEOF
echo -e "${GREEN}  ⏱  generated in $(elapsed_ms $t0) ms${NC}"

# =============================================================================
#  OPTIMIZATION #1 — Spark Warm-up
# =============================================================================
section "Optimization #1 — Spark Warm-up"
echo -e "  ${DIM}SparkEngine.warmUp() runs 'SELECT 1' right after getOrCreate()."
echo -e "  First real job no longer pays a ~5 s cold-start penalty.${NC}"
echo ""

echo -e "${BOLD}  T1  Initial ingestion (1 000 rows) — first job on a warm session${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/ingestion/upload" \
  -F "file=@t1_initial.csv" \
  -F "userId=${USER_ID}" -F "projectId=${PROJECT_ID}" -F "tableName=${TABLE}")
job=$(echo "$resp" | jq -r '.jobId')
record "t1_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t1_http]} ms)"

t0=$(ts)
wait_ingestion "$job"
record "t1_worker" "$(elapsed_ms $t0)"
echo -e "${GREEN}  ⏱  worker: ${T[t1_worker]} ms${NC}"
echo ""

echo -e "${BOLD}  T2  Append ingestion (500 rows) — subsequent job${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/ingestion/upload" \
  -F "file=@t2_append.csv" \
  -F "userId=${USER_ID}" -F "projectId=${PROJECT_ID}" -F "tableName=${TABLE}")
job=$(echo "$resp" | jq -r '.jobId')
record "t2_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t2_http]} ms)"

t0=$(ts)
wait_ingestion "$job"
record "t2_worker" "$(elapsed_ms $t0)"
echo -e "${GREEN}  ⏱  worker: ${T[t2_worker]} ms${NC}"

# =============================================================================
#  OPTIMIZATION #2 — Schema Redis Cache
# =============================================================================
section "Optimization #2 — Schema Redis Cache"
echo -e "  ${DIM}Key: 'schema:iceberg.{project}.{table}'  |  No TTL — event-driven invalidation."
echo -e "  query-worker caches after Spark fetch."
echo -e "  ingestion-worker calls DEL after every successful table write.${NC}"
echo ""

echo -e "${BOLD}  T3  Schema MISS — first request, cache is empty → Spark${NC}"
do_schema "schema request #1"
record "t3_miss" "$(cat /tmp/_schema_ms)"
echo ""

echo -e "${BOLD}  T4  Schema HIT  — same table, served instantly from Redis${NC}"
do_schema "schema request #2"
record "t4_hit" "$(cat /tmp/_schema_ms)"
echo ""

echo -e "${BOLD}  T5  Ingest CSV with new column 'region' — triggers cache invalidation${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/ingestion/upload" \
  -F "file=@t3_new_column.csv" \
  -F "userId=${USER_ID}" -F "projectId=${PROJECT_ID}" -F "tableName=${TABLE}")
job=$(echo "$resp" | jq -r '.jobId')
record "t5_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t5_http]} ms)"

t0=$(ts)
wait_ingestion "$job"
record "t5_worker" "$(elapsed_ms $t0)"
echo -e "${GREEN}  ⏱  worker: ${T[t5_worker]} ms  ${DIM}(schema cache DEL called by ingestion-worker)${NC}"
echo ""

echo -e "${BOLD}  T6  Schema MISS after invalidation — re-fetches updated schema from Iceberg${NC}"
do_schema "schema request #3  (post-invalidation, expects new 'region' column)"
record "t6_post_ingest" "$(cat /tmp/_schema_ms)"
echo ""
label "Updated column list (should now include 'region'):"
# Issue one more schema request and dump the column list
tmp_job=$(curl -s "${API_BASE}/api/v1/schema/${SOURCE}" | jq -r '.jobId')
tmp_resp=$(curl -s --max-time 30 "${API_BASE}/api/v1/schema/status/${tmp_job}/wait?timeoutSec=30")
echo "$tmp_resp" \
  | jq -r '.resultData // "[]"' \
  | jq -r '.[] | "      • \(.name)  (\(.type))"'
echo ""

echo -e "${BOLD}  T7  Schema HIT  — re-cached after post-invalidation Spark fetch${NC}"
do_schema "schema request #4"
record "t7_hit2" "$(cat /tmp/_schema_ms)"

# =============================================================================
#  QUERIES — All run on a warm Spark session
# =============================================================================
section "Queries — warm Spark session"

echo -e "${BOLD}  T8  SELECT * LIMIT 100${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/query" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"${SOURCE}\",\"select\":[{\"column\":\"*\"}],\"limit\":100}")
job=$(echo "$resp" | jq -r '.jobId')
record "t8_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t8_http]} ms)"

t0=$(ts)
wait_query "$job"
record "t8_worker" "$(elapsed_ms $t0)"
rows=$(curl -s "${API_BASE}/api/v1/query/${job}" | jq -r '.rowCount // "?"')
echo -e "${GREEN}  ⏱  worker: ${T[t8_worker]} ms  — ${rows} rows${NC}"
echo ""

echo -e "${BOLD}  T9  WHERE price > 500  ORDER BY price DESC  LIMIT 50${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/query" \
  -H "Content-Type: application/json" \
  -d "{
    \"source\":\"${SOURCE}\",
    \"select\":[
      {\"column\":\"id\"},{\"column\":\"product\"},
      {\"column\":\"price\"},{\"column\":\"quantity\"}
    ],
    \"filters\":[{\"column\":\"price\",\"operator\":\">\",\"value\":500}],
    \"orderBy\":[{\"column\":\"price\",\"direction\":\"desc\"}],
    \"limit\":50
  }")
job=$(echo "$resp" | jq -r '.jobId')
record "t9_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t9_http]} ms)"

t0=$(ts)
wait_query "$job"
record "t9_worker" "$(elapsed_ms $t0)"
rows=$(curl -s "${API_BASE}/api/v1/query/${job}" | jq -r '.rowCount // "?"')
echo -e "${GREEN}  ⏱  worker: ${T[t9_worker]} ms  — ${rows} rows${NC}"
echo ""

echo -e "${BOLD}  T10 GROUP BY product  (avg price, total qty, row count)${NC}"
t0=$(ts)
resp=$(curl -s -X POST "${API_BASE}/api/v1/query" \
  -H "Content-Type: application/json" \
  -d "{
    \"source\":\"${SOURCE}\",
    \"select\":[
      {\"column\":\"product\"},
      {\"column\":\"price\",    \"aggregation\":\"avg\",   \"as\":\"avg_price\"},
      {\"column\":\"quantity\", \"aggregation\":\"sum\",   \"as\":\"total_qty\"},
      {\"column\":\"id\",       \"aggregation\":\"count\", \"as\":\"row_count\"}
    ],
    \"groupBy\":[\"product\"],
    \"orderBy\":[{\"column\":\"avg_price\",\"direction\":\"desc\"}]
  }")
job=$(echo "$resp" | jq -r '.jobId')
record "t10_http" "$(elapsed_ms $t0)"
label "jobId: $job  (HTTP ${T[t10_http]} ms)"

t0=$(ts)
wait_query "$job"
record "t10_worker" "$(elapsed_ms $t0)"
echo -e "${GREEN}  ⏱  worker: ${T[t10_worker]} ms${NC}"
label "Results:"
curl -s "${API_BASE}/api/v1/query/${job}" \
  | jq -r '.resultData // "[]"' \
  | jq -r '.[] | "      \(.product)  avg=$\(.avg_price | floor)  qty=\(.total_qty)  n=\(.row_count)"'

# =============================================================================
#  RESULTS SUMMARY
# =============================================================================
section "Results Summary"

fmt_row() { printf "  %-46s  %8s ms  %s\n" "$1" "$2" "$3"; }
sep()     { printf "  %-46s  %8s    %s\n"  "──────────────────────────────────────────────" "────────" "──────────────────────────────────"; }

printf "  ${BOLD}%-46s  %8s    %s${NC}\n" "Test" "Worker" "Notes"
sep

echo ""
echo -e "  ${BOLD}── Optimization #1: Spark Warm-up ──${NC}"
fmt_row "T1  Initial ingestion (1 000 rows)"  "${T[t1_worker]}"  "first job — warm session, no cold start"
fmt_row "T2  Append ingestion  (500 rows)"    "${T[t2_worker]}"  "subsequent job"

echo ""
echo -e "  ${BOLD}── Optimization #2: Schema Redis Cache ──${NC}"
fmt_row "T3  Schema  MISS  (→ Spark)"         "${T[t3_miss]}"         "cache empty, fetches from Iceberg"
fmt_row "T4  Schema  HIT   (→ Redis)"         "${T[t4_hit]}"          "served from Redis — polling interval ~1 s"
fmt_row "T5  Ingestion invalidation trigger"  "${T[t5_worker]}"       "DEL called after write"
fmt_row "T6  Schema  MISS  (post-ingest)"     "${T[t6_post_ingest]}"  "re-fetched: updated schema now cached"
fmt_row "T7  Schema  HIT   (re-cached)"       "${T[t7_hit2]}"         "served from Redis again"

echo ""
echo -e "  ${BOLD}── Queries (warm Spark) ──${NC}"
fmt_row "T8  SELECT * LIMIT 100"              "${T[t8_worker]}"   ""
fmt_row "T9  WHERE price>500 ORDER BY"        "${T[t9_worker]}"   ""
fmt_row "T10 GROUP BY product"                "${T[t10_worker]}"  ""

sep
echo ""

# Cache speedup
if [ -n "${T[t3_miss]}" ] && [ -n "${T[t4_hit]}" ] && [ "${T[t4_hit]}" -gt 0 ]; then
    speedup=$(python3 -c "print(f'{${T[t3_miss]} / ${T[t4_hit]}:.0f}x')" 2>/dev/null || echo "?x")
    echo -e "${GREEN}${BOLD}  Schema cache:  ${T[t3_miss]} ms  →  ${T[t4_hit]} ms   (~${speedup} faster on cache HIT)${NC}"
fi
echo ""

# =============================================================================
#  Cleanup
# =============================================================================
section "Cleanup"
rm -f t1_initial.csv t2_append.csv t3_new_column.csv /tmp/_schema_ms
echo "  Removed temporary CSV files."
echo ""
