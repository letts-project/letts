#!/bin/sh
# postinstall — create the letts system user and group, reload systemd.
# Does NOT enable/start dugdale (no config yet). deb calls this with
# $1=configure; rpm with $1=1|2. Idempotent.
set -e

getent group letts        >/dev/null 2>&1 || groupadd --system letts
getent passwd letts       >/dev/null 2>&1 || \
  useradd --system --gid letts --no-create-home \
          --home-dir /var/lib/letts --shell /usr/sbin/nologin letts

mkdir -p /etc/letts/dugdale /var/lib/letts
# /var/lib/letts is a shared container — per-instance data_dirs
# (/var/lib/letts/<name>, incl. the default's /var/lib/letts/default) are created
# and owned by systemd (StateDirectory=) as each service's User=. Keep the
# container world-traversable so non-letts service users can reach their subdir.
chmod 0755 /var/lib/letts

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
  systemctl try-restart dugdale.service || true   # no-op on fresh install
fi
