#!/bin/bash
# Load the nftables table metadata_guard: only the allowed system services may reach the instance metadata address.
# Usage: metadata_guard <allowed-units-file> [<cgroup-root>]
# nftables binds a cgroup match to the cgroup that exists when the rule loads, and systemd gives a unit a new cgroup each time it starts.
# Each allowed unit therefore runs this script before it starts, and the script writes a rule for every allowed unit that has a cgroup now.
set -euo pipefail

address=169.254.169.254
units_file=${1:?usage: metadata_guard <allowed-units-file> [<cgroup-root>]}
cgroup_root=${2:-/sys/fs/cgroup}
unit_pattern='^[A-Za-z0-9@_.:-]+\.service$'
attempts=5

running_units() {
  local unit
  while IFS= read -r unit || [ -n "$unit" ]; do
    case "$unit" in '' | '#'*) continue ;; esac
    if ! [[ "$unit" =~ $unit_pattern ]]; then
      echo "metadata_guard: refusing the unit name in $units_file" >&2
      exit 2
    fi
    if [ -d "$cgroup_root/system.slice/$unit" ]; then
      printf '%s\n' "$unit"
    fi
  done <"$units_file"
}

ruleset() {
  local unit
  printf 'table inet metadata_guard\ndelete table inet metadata_guard\ntable inet metadata_guard {\n'
  printf '\tchain output {\n\t\ttype filter hook output priority filter; policy accept;\n'
  while IFS= read -r unit; do
    if [ -n "$unit" ]; then
      printf '\t\tip daddr %s socket cgroupv2 level 2 "system.slice/%s" accept\n' "$address" "$unit"
    fi
  done <<<"$1"
  printf '\t\tip daddr %s drop\n\t}\n' "$address"
  printf '\tchain forward {\n\t\ttype filter hook forward priority filter; policy accept;\n\t\tip daddr %s drop\n\t}\n}\n' "$address"
}

# Another allowed unit may start while this script runs, so repeat until the units that run now are the units in the loaded table.
loaded=""
for _ in $(seq "$attempts"); do
  units=$(running_units)
  ruleset "$units" | nft -f -
  loaded=$units
  if [ "$(running_units)" = "$loaded" ]; then
    exit 0
  fi
done
echo "metadata_guard: the allowed units kept changing; the table holds the rules of the last scan" >&2
exit 1
