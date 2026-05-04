#!/bin/sh
# preremove — stop and disable dugdale on real removal, but NOT across upgrades.
# deb calls prerm with $1 = remove | upgrade | deconfigure | failed-upgrade.
set -e

case "$1" in
  remove|deconfigure|0)
    if command -v systemctl >/dev/null 2>&1; then
      systemctl disable --now dugdale.service >/dev/null 2>&1 || true
    fi
    ;;
esac
exit 0
