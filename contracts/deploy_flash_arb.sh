#!/usr/bin/env bash
# Деплой FlashArb на Base. Читает ~/arbitraj/.env (родитель каталога contracts/).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Нет файла $ROOT/.env"
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

RPC="${RPC_URL_DRPC:-${BASE_HTTP:-}}"
KEY="${DEPLOYER_PRIVATE_KEY:-${TRADER_PRIVATE_KEY:-}}"

if [[ -z "$RPC" ]]; then
  echo "В .env нужен RPC_URL_DRPC или BASE_HTTP (HTTPS URL Base)."
  exit 1
fi
if [[ -z "$KEY" ]]; then
  echo "В .env нужен DEPLOYER_PRIVATE_KEY или TRADER_PRIVATE_KEY."
  exit 1
fi

echo "RPC (первые 40 символов): ${RPC:0:40}..."
echo "Длина ключа (символов): ${#KEY}"

cd "$ROOT/contracts"
forge create src/FlashArb.sol:FlashArb \
  --rpc-url "$RPC" \
  --private-key "$KEY" \
  --constructor-args 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5

echo
echo "Скопируй Deployed to: ... в .env как FLASH_ARB_CONTRACT=0x..."
