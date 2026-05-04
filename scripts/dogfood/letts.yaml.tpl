auth:
  token: dogfood-dispatch
  admin_token: dogfood-admin
  exec_token: dogfood-exec

dugdales:
  - id: local
    host: 127.0.0.1
    port: ${PORT}
    mission_dir: ${MISSION_DIR}
    runtime:
      command_template: ["php", "{mission_path}"]
      mission_path_template: "{mission}.php"
      validate_mission_file: true
    lanes:
      fast:   {concurrency: 4}
      normal: {concurrency: 2}
      slow:   {concurrency: 1}
      manual: {concurrency: 1}
