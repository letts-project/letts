#!/bin/sh
# postremove — reload systemd; on purge drop the system user/group.
# Leaves /var/lib/letts (SQLite state) intact on purpose.
# deb calls postrm with $1 = remove | purge | upgrade | ...
set -e

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi

if [ "$1" = "purge" ]; then
  userdel  letts        >/dev/null 2>&1 || true
  groupdel letts        >/dev/null 2>&1 || true
  echo "letts purged. NOTE: /var/lib/letts (state) left intact — remove manually if desired."
fi
exit 0
