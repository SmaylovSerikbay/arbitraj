#!/bin/bash
set -euo pipefail

# =============================================================
#  Base Mainnet Node Setup (Reth + op-node)
#  Для арбитраж-бота: локальный RPC + WSS на localhost
#  Чистый Ubuntu VDS — ставит всё с нуля
# =============================================================

echo "============================================"
echo " Base Node Setup — Reth (pruned) + op-node"
echo " Для чистого Ubuntu сервера"
echo "============================================"

# --- 0. Базовые зависимости ---
echo "[0/7] Установка базовых пакетов..."
apt-get update -qq
apt-get install -y -qq curl wget git jq zstd ca-certificates gnupg lsb-release

# --- 1. Docker ---
if ! command -v docker &>/dev/null; then
  echo "[1/7] Установка Docker..."
  curl -fsSL https://get.docker.com | sh
  systemctl enable docker && systemctl start docker
  echo "  ✓ Docker установлен: $(docker --version)"
else
  echo "[1/7] Docker уже установлен: $(docker --version)"
fi

if ! docker compose version &>/dev/null; then
  echo "ОШИБКА: docker compose не найден."
  echo "Установка Docker Compose plugin..."
  apt-get install -y docker-compose-plugin
fi
echo "  ✓ Docker Compose: $(docker compose version)"

# --- 2. Клонируем base/node ---
BASE_DIR="/opt/base-node"
if [ -d "$BASE_DIR" ]; then
  echo "[2/7] $BASE_DIR уже существует, обновляем..."
  cd "$BASE_DIR" && git pull || true
else
  echo "[2/7] Клонируем base/node..."
  git clone https://github.com/base/node.git "$BASE_DIR"
fi
cd "$BASE_DIR"

# --- 3. Создаём директорию данных ---
echo "[3/7] Подготовка директории данных..."
mkdir -p ./reth-data

# --- 4. Настройка .env ---
echo "[4/7] Настройка .env.mainnet..."

# Проверяем, заданы ли L1 endpoints
if grep -q '^OP_NODE_L1_ETH_RPC=$' .env.mainnet 2>/dev/null; then
  echo ""
  echo "╔════════════════════════════════════════════════════════╗"
  echo "║  ТРЕБУЕТСЯ: L1 (Ethereum mainnet) RPC endpoints       ║"
  echo "╠════════════════════════════════════════════════════════╣"
  echo "║  Нужно 2 URL от одного провайдера:                    ║"
  echo "║                                                        ║"
  echo "║  1) Ethereum Execution RPC (HTTP)                      ║"
  echo "║     Пример: https://eth-mainnet.g.alchemy.com/v2/KEY  ║"
  echo "║                                                        ║"
  echo "║  2) Ethereum Beacon API (HTTP)                         ║"
  echo "║     Пример: https://eth-mainnet.g.alchemy.com/v2/KEY  ║"
  echo "║                                                        ║"
  echo "║  Бесплатные варианты:                                  ║"
  echo "║  • Alchemy: alchemy.com (бесплатно, ETH mainnet app)  ║"
  echo "║  • dRPC: drpc.org (уже есть аккаунт)                  ║"
  echo "║  • QuickNode: quicknode.com (бесплатный тир)           ║"
  echo "╚════════════════════════════════════════════════════════╝"
  echo ""
  read -rp "Ethereum L1 Execution RPC URL: " L1_RPC
  read -rp "Ethereum L1 Beacon API URL: " L1_BEACON
  
  sed -i "s|^OP_NODE_L1_ETH_RPC=.*|OP_NODE_L1_ETH_RPC=${L1_RPC}|" .env.mainnet
  sed -i "s|^OP_NODE_L1_BEACON=.*|OP_NODE_L1_BEACON=${L1_BEACON}|" .env.mainnet
  sed -i "s|^OP_NODE_L1_BEACON_ARCHIVER=.*|OP_NODE_L1_BEACON_ARCHIVER=${L1_BEACON}|" .env.mainnet
  
  # Определяем тип провайдера
  if echo "$L1_RPC" | grep -qi "alchemy"; then
    sed -i 's|^OP_NODE_L1_RPC_KIND=.*|OP_NODE_L1_RPC_KIND="alchemy"|' .env.mainnet
  elif echo "$L1_RPC" | grep -qi "drpc"; then
    sed -i 's|^OP_NODE_L1_RPC_KIND=.*|OP_NODE_L1_RPC_KIND="standard"|' .env.mainnet
  elif echo "$L1_RPC" | grep -qi "quicknode"; then
    sed -i 's|^OP_NODE_L1_RPC_KIND=.*|OP_NODE_L1_RPC_KIND="quicknode"|' .env.mainnet
  elif echo "$L1_RPC" | grep -qi "infura"; then
    sed -i 's|^OP_NODE_L1_RPC_KIND=.*|OP_NODE_L1_RPC_KIND="infura"|' .env.mainnet
  else
    sed -i 's|^OP_NODE_L1_RPC_KIND=.*|OP_NODE_L1_RPC_KIND="standard"|' .env.mainnet
  fi
  
  echo "✓ L1 endpoints сохранены в .env.mainnet"
else
  echo "  L1 endpoints уже настроены."
fi

# Устанавливаем HOST_DATA_DIR
export HOST_DATA_DIR="./reth-data"
export CLIENT="reth"
export NETWORK_ENV=".env.mainnet"

# Создаём .env файл для docker-compose
cat > .env <<ENVEOF
HOST_DATA_DIR=./reth-data
CLIENT=reth
NETWORK_ENV=.env.mainnet
ENVEOF

# --- 5. Скачиваем snapshot ---
echo "[5/7] Проверка snapshot..."
if [ -d "./reth-data/db" ] || [ -d "./reth-data/static_files" ]; then
  echo "  Данные уже есть, пропуск скачивания snapshot."
else
  echo ""
  echo "  Скачиваем pruned snapshot Base mainnet..."
  echo "  Это ~200-400 GB, займёт время в зависимости от скорости сети."
  echo "  (wget -c поддерживает докачку при обрыве)"
  echo ""
  
  SNAPSHOT_URL=$(curl -sL https://mainnet-reth-pruned-snapshots.base.org/latest)
  echo "  Snapshot: $SNAPSHOT_URL"
  wget -c "https://mainnet-reth-pruned-snapshots.base.org/${SNAPSHOT_URL}" -O snapshot.tar.zst
  
  echo "[6/7] Распаковка snapshot..."
  apt-get install -y zstd || true
  tar -I zstd -xvf snapshot.tar.zst
  
  # Перемещаем данные в reth-data
  if [ -d "./reth" ]; then
    mv ./reth/* ./reth-data/ 2>/dev/null || true
    rm -rf ./reth
  fi
  
  echo "  ✓ Snapshot распакован"
  echo "  Можно удалить архив: rm -f snapshot.tar.zst"
fi

# --- 7. Открываем порты ---
echo "[7/7] Настройка портов..."
if command -v ufw &>/dev/null; then
  ufw allow 30303/tcp 2>/dev/null || true
  ufw allow 30303/udp 2>/dev/null || true
  ufw allow 9222/tcp 2>/dev/null || true
  ufw allow 9222/udp 2>/dev/null || true
  echo "  ✓ Порты 30303, 9222 открыты (UFW)"
fi

echo ""
echo "============================================"
echo "  ГОТОВО! Для запуска ноды:"
echo "============================================"
echo ""
echo "  cd $BASE_DIR"
echo "  docker compose up -d --build"
echo ""
echo "  Проверить синхронизацию:"
echo "  curl -s -d '{\"id\":0,\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\":[]}' \\"
echo "    -H 'Content-Type: application/json' http://localhost:8545 | jq"
echo ""
echo "  Проверить отставание:"
echo '  echo "Behind: $(( ($(date +%s) - $(curl -s -d '\''{"id":0,"jsonrpc":"2.0","method":"optimism_syncStatus"}'\'' -H '\''Content-Type: application/json'\'' http://localhost:7545 | jq -r .result.unsafe_l2.timestamp)) / 60 )) minutes"'
echo ""
echo "  Логи:"
echo "  docker compose logs -f --tail 50"
echo ""
echo "  После синхронизации, обновить .env бота:"
echo "  BASE_HTTP=http://localhost:8545"
echo "  BASE_WSS=ws://localhost:8546"
echo ""
