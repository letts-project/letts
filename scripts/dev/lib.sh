# lib.sh — sourced by the dev scripts. Provides:
#   DUGDALE_URL       — base URL (default http://127.0.0.1:7180)
#   DUGDALE_DISP_TOK  — dispatch Bearer token (default dev-disp-token)
#   DUGDALE_ADMIN_TOK — admin Bearer token  (default dev-admin-token)
#   gen_uuidv7        — print a fresh UUIDv7 to stdout
#   curl_disp         — curl with the dispatch Bearer header
#   curl_admin        — curl with the admin Bearer header
#
# Usage:  . "$(dirname "$0")/lib.sh"

DUGDALE_URL="${DUGDALE_URL:-http://127.0.0.1:7180}"
DUGDALE_DISP_TOK="${DUGDALE_DISP_TOK:-dev-disp-token}"
DUGDALE_ADMIN_TOK="${DUGDALE_ADMIN_TOK:-dev-admin-token}"

# UUIDv7 generation — Python because dugdale validates the canonical RFC 9562
# form (^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$),
# which `uuidgen` (UUIDv4) doesn't satisfy.
gen_uuidv7() {
    python3 -c "
import secrets, time, uuid
ts = int(time.time() * 1000)
b = bytearray(ts.to_bytes(6, 'big') + secrets.token_bytes(10))
b[6] = (b[6] & 0x0f) | 0x70   # version 7
b[8] = (b[8] & 0x3f) | 0x80   # variant 10
print(uuid.UUID(bytes=bytes(b)))
"
}

curl_disp() { curl -sS -H "Authorization: Bearer ${DUGDALE_DISP_TOK}" "$@"; }
curl_admin() { curl -sS -H "Authorization: Bearer ${DUGDALE_ADMIN_TOK}" "$@"; }
