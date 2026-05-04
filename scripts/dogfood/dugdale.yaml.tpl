listen: 127.0.0.1:${PORT}
data_dir: ${DATA_DIR}

log:
  level: info
  format: text
  output: stderr

auth:
  tokens:
    - dogfood-dispatch

admin:
  tokens:
    - dogfood-admin

exec:
  enabled: true
  allow_shell: false
  tokens:
    - dogfood-exec

cleanup:
  success_ttl: 1h
  failed_ttl: 24h
  staging_ttl: 1h
  sweep_interval: 30s

limits:
  max_dispatch_body_size: 1MiB
  max_apply_body_size: 1MiB
  max_staging_upload_size: 100MiB
  max_event_line_size: 1MiB
  default_kill_grace: 2s
  reader_post_exit_grace: 2s
