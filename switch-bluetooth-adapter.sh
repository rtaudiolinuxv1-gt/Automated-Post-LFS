#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./switch-bluetooth-adapter.sh list
  ./switch-bluetooth-adapter.sh status
  ./switch-bluetooth-adapter.sh select <hciX|MAC> [--keep-others]

Examples:
  ./switch-bluetooth-adapter.sh list
  ./switch-bluetooth-adapter.sh select hci1
  ./switch-bluetooth-adapter.sh select 11:22:33:44:55:66 --keep-others

Notes:
  - By default, "select" powers off all other controllers to force apps onto
    the chosen adapter.
  - Use --keep-others if you only want to select/power on one controller and
    leave the others alone.
  - If bluetoothctl fails, make sure Bluetooth and D-Bus are running.
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

list_with_hciconfig() {
  local default_mac="${1:-}"
  awk -v default_mac="$default_mac" '
    /^[a-z0-9]+:/ {
      gsub(":", "", $1)
      hci=$1
      next
    }
    /BD Address:/ {
      mac=$3
      default_flag=(toupper(mac)==toupper(default_mac) ? " [default]" : "")
      print hci " " mac default_flag
    }
  ' < <(hciconfig -a 2>/dev/null)
}

default_controller_mac() {
  local line
  while IFS= read -r line; do
    case "$line" in
      *"[default]"*)
        set -- $line
        if [ "${1:-}" = "Controller" ] && [ -n "${2:-}" ]; then
          echo "$2"
          return 0
        fi
        ;;
    esac
  done < <(bluetoothctl list 2>/dev/null || true)
  return 1
}

list_controllers() {
  require_command bluetoothctl
  local default_mac=""
  if default_mac="$(default_controller_mac)"; then
    :
  else
    default_mac=""
  fi

  if command -v hciconfig >/dev/null 2>&1; then
    if ! list_with_hciconfig "$default_mac"; then
      echo "Unable to read Bluetooth controllers with hciconfig" >&2
      exit 1
    fi
    return 0
  fi

  bluetoothctl list
}

resolve_controller_mac() {
  local target="$1"
  if [[ "$target" =~ ^([[:xdigit:]]{2}:){5}[[:xdigit:]]{2}$ ]]; then
    echo "$target"
    return 0
  fi

  if command -v hciconfig >/dev/null 2>&1; then
    awk -v wanted="$target" '
      /^[a-z0-9]+:/ {
        gsub(":", "", $1)
        hci=$1
        next
      }
      /BD Address:/ {
        if (hci == wanted) {
          print $3
          found=1
          exit 0
        }
      }
      END {
        if (!found) {
          exit 1
        }
      }
    ' < <(hciconfig -a 2>/dev/null)
    return $?
  fi

  return 1
}

all_controller_macs() {
  if command -v hciconfig >/dev/null 2>&1; then
    awk '
      /BD Address:/ {
        print $3
      }
    ' < <(hciconfig -a 2>/dev/null)
  else
    bluetoothctl list 2>/dev/null | awk '/^Controller / { print $2 }'
  fi
}

run_bluetoothctl_commands() {
  local commands="$1"
  printf '%s\n' "$commands" | bluetoothctl >/dev/null
}

show_status() {
  require_command bluetoothctl
  list_controllers
  echo
  local default_mac=""
  if default_mac="$(default_controller_mac)"; then
    echo "Current default controller: $default_mac"
    bluetoothctl show "$default_mac" 2>/dev/null || true
  else
    echo "No default controller reported by bluetoothctl"
  fi
}

select_controller() {
  require_command bluetoothctl
  local target="$1"
  local keep_others="${2:-0}"
  local target_mac=""

  if ! target_mac="$(resolve_controller_mac "$target")"; then
    echo "Could not resolve controller: $target" >&2
    echo >&2
    echo "Available controllers:" >&2
    list_controllers >&2 || true
    exit 1
  fi

  local commands=""
  if [ "$keep_others" != "1" ]; then
    while IFS= read -r mac; do
      [ -n "$mac" ] || continue
      if [ "${mac^^}" = "${target_mac^^}" ]; then
        continue
      fi
      commands="${commands}select ${mac}
power off
"
    done < <(all_controller_macs)
  fi

  commands="${commands}select ${target_mac}
power on
show
"

  run_bluetoothctl_commands "$commands"
  echo "Selected controller: $target_mac"
  if [ "$keep_others" != "1" ]; then
    echo "Other controllers were powered off."
  fi
}

main() {
  if [ $# -lt 1 ]; then
    usage
    exit 1
  fi

  case "$1" in
    list)
      list_controllers
      ;;
    status)
      show_status
      ;;
    select)
      [ $# -ge 2 ] || {
        usage
        exit 1
      }
      local keep_others=0
      if [ "${3:-}" = "--keep-others" ] || [ "${2:-}" = "--keep-others" ]; then
        keep_others=1
      fi
      select_controller "$2" "$keep_others"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
