#!/usr/bin/env bash
# Снять факты с VDS: кто слушает порты, где отвечает eth_chainId (Base = 0x2105).
# Запуск: bash scripts/inspect_rpc_ports.sh
# С именами процессов в ss: sudo bash scripts/inspect_rpc_ports.sh

set -u

echo "=============================================="
echo "1) TCP LISTEN (все интерфейсы)"
echo "=============================================="
if command -v ss >/dev/null 2>&1; then
  ss -tlnp 2>/dev/null || ss -tln
else
  netstat -tlnp 2>/dev/null || netstat -tln
fi

echo ""
echo "=============================================="
echo "2) Строки LISTEN с типичными портами (8545 HTTP, 8546 WS, 8551 …)"
echo "=============================================="
if command -v ss >/dev/null 2>&1; then
  ss -tln 2>/dev/null | grep -E ':8545|:8546|:8547|:8551|:30303|:8745|:18545|:18546' || echo "  (нет совпадений в ss -tln)"
else
  netstat -tln 2>/dev/null | grep -E ':8545|:8546|:8547|:8551|:30303|:8745|:18545|:18546' || true
fi

echo ""
echo "=============================================="
echo "3) eth_chainId по HTTP (Base mainnet = 0x2105 = 8453)"
echo "=============================================="
probe_http() {
  local host="$1" port="$2"
  local url="http://${host}:${port}"
  local body='{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}'
  local out
  out=$(curl -sS --max-time 3 -X POST "$url" -H 'Content-Type: application/json' -d "$body" 2>/dev/null) || return
  if echo "$out" | grep -q '"result"'; then
    echo "  $url -> $out"
  fi
}

for p in 8545 8546 8547 8551 8745 18545; do
  probe_http "127.0.0.1" "$p"
done

echo ""
echo "=============================================="
echo "4) rpc_modules (если отвечает 127.0.0.1:8545)"
echo "=============================================="
mods=$(curl -sS --max-time 3 -X POST "http://127.0.0.1:8545" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"rpc_modules","params":[],"id":1}' 2>/dev/null) || true
if echo "$mods" | grep -q '"result"'; then
  echo "  $mods"
else
  echo "  (нет ответа на :8545 или не JSON-RPC)"
fi

echo ""
echo "=============================================="
echo "5) Подсказка: где искать флаги ноды"
echo "=============================================="
echo "  systemctl status <имя-сервиса-ноды>"
echo "  systemctl cat <имя-сервиса-ноды>   # ExecStart с --ws.port / --http.port"
echo "  docker ps && docker inspect <container>"
echo ""
echo "Ищите в ExecStart: http.port, ws.port, ws, rpc, 8545, 8546."
