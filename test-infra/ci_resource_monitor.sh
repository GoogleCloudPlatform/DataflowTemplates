#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Samples CI runner resource usage until killed.
# Usage: ci_resource_monitor.sh <output_dir> [interval_seconds]
# Writes:
#   host.txt               machine facts (cores, memory, disk, docker)
#   host_samples.csv       one row per sample: CPU, memory, swap, disk, containers, java processes
#   container_samples.csv  one row per running container per sample (docker stats + image)

out="$1"
interval="${2:-10}"
mkdir -p "$out"

{
  echo "hostname=$(hostname)"
  echo "nproc=$(nproc)"
  grep -E '^(MemTotal|SwapTotal):' /proc/meminfo
  echo "disk=$(df -h --output=size,used,avail "$PWD" | tail -1)"
  docker info --format 'docker_cpus={{.NCPU}} docker_mem_bytes={{.MemTotal}} docker_containers={{.Containers}}' 2>/dev/null
  echo "started=$(date -u +%FT%TZ)"
} > "$out/host.txt"

echo "ts,load1,cpu_busy_pct,mem_used_mb,mem_total_mb,swap_used_mb,disk_used_gb,containers_running,containers_cpu_pct,containers_mem_mb,java_procs,java_rss_mb" \
  > "$out/host_samples.csv"
echo "ts,container,image,cpu_pct,mem_mb" > "$out/container_samples.csv"

read_cpu() {
  read -r _ u n s i w q sq st _ < /proc/stat
  echo "$((u + n + s + i + w + q + sq + st)) $((i + w))"
}

read -r prev_total prev_idle <<< "$(read_cpu)"
while true; do
  sleep "$interval"
  ts=$(date -u +%FT%TZ)
  read -r total idle <<< "$(read_cpu)"
  busy=$(awk -v dt=$((total - prev_total)) -v di=$((idle - prev_idle)) \
    'BEGIN { printf "%.1f", (dt > 0) ? 100 * (dt - di) / dt : 0 }')
  prev_total=$total
  prev_idle=$idle
  load1=$(cut -d' ' -f1 /proc/loadavg)
  mem=$(awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} /^SwapTotal:/{st=$2} /^SwapFree:/{sf=$2}
    END { printf "%d,%d,%d", (t - a) / 1024, t / 1024, (st - sf) / 1024 }' /proc/meminfo)
  mem_used=${mem%%,*}
  rest=${mem#*,}
  mem_total=${rest%%,*}
  swap_used=${rest#*,}
  disk=$(df -BG --output=used "$PWD" | tail -1 | tr -dc '0-9')
  images=$(timeout 30 docker ps --format '{{.ID}},{{.Image}}' 2>/dev/null || true)
  stats=$(timeout 30 docker stats --no-stream --format '{{.ID}},{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null || true)
  # Output: name,image,cpu_pct,mem_mb
  parsed=$(awk -F, 'NR == FNR { img[$1] = $2; next } NF >= 4 {
      cpu = $3; gsub("%", "", cpu)
      split($4, m, " / "); v = m[1]; mb = v + 0
      if (v ~ /GiB/) mb = mb * 1024; else if (v ~ /KiB/) mb = mb / 1024; else if (v ~ /[0-9]B$/) mb = mb / 1048576
      image = ($1 in img) ? img[$1] : "unknown"
      printf "%s,%s,%.1f,%.1f\n", $2, image, cpu, mb
    }' <(echo "$images") <(echo "$stats"))
  if [ -n "$parsed" ]; then
    echo "$parsed" | sed "s/^/$ts,/" >> "$out/container_samples.csv"
  fi
  containers=$(echo "$parsed" | awk -F, 'NF >= 4 { n++; c += $3; m += $4 } END { printf "%d,%.1f,%.1f", n, c, m }')
  java=$(ps -C java -o rss= 2>/dev/null | awk '{ s += $1; n++ } END { printf "%d,%d", n, s / 1024 }')
  echo "$ts,$load1,$busy,$mem_used,$mem_total,$swap_used,$disk,$containers,$java" >> "$out/host_samples.csv"
done
