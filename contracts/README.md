# FlashArb (Aave V3 flash loan на Base)

## Сборка (Foundry)

```bash
cd contracts
forge install foundry-rs/forge-std
forge build
```

### Деплой с VDS (рекомендуется)

Из корня репозитория `arbitraj/` — скрипт сам делает `source .env` и подставляет `BASE_HTTP`, если нет `RPC_URL_DRPC`, и `TRADER_PRIVATE_KEY`, если нет `DEPLOYER_PRIVATE_KEY`:

```bash
chmod +x contracts/deploy_flash_arb.sh
./contracts/deploy_flash_arb.sh
```

Скрипт деплоя (`script/DeployFlashArb.s.sol`) зависит от `forge-std`.

## Деплой на Base mainnet

Пул Aave V3 Base: `0xA238Dd80C259a72e81d7e4664a9801593F98d1c5` (единственный аргумент конструктора).

**Важно:** владелец контракта = адрес деплоя. Используйте тот же кошелёк, что и `TRADER_PRIVATE_KEY` в боте, либо вызовите `transferOwnership` на адрес трейдера.

### Windows (PowerShell)

```powershell
cd contracts
forge create src/FlashArb.sol:FlashArb `
  --broadcast `
  --rpc-url $env:RPC_URL_DRPC `
  --private-key $env:DEPLOYER_PRIVATE_KEY `
  --constructor-args 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5
```

### bash

```bash
forge create src/FlashArb.sol:FlashArb \
  --broadcast \
  --rpc-url "$RPC_URL_DRPC" \
  --private-key "$DEPLOYER_PRIVATE_KEY" \
  --constructor-args 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5
```

Сохраните адрес контракта в `.env`: `FLASH_ARB_CONTRACT=0x...`

### Через `forge script` (альтернатива)

```bash
cd contracts
forge script script/DeployFlashArb.s.sol:DeployFlashArb --rpc-url "$RPC_URL_DRPC" --broadcast
```

## Альтернатива без Foundry

Установите `solc` 0.8.20 и скомпилируйте `src/FlashArb.sol` с оптимизатором; задеплойте байткод через Cast/MetaMask.

## Вывод прибыли

После сделок WETH остаётся на контракте. Владелец вызывает `withdrawToken(WETH, type(uint256).max)` (через Etherscan или cast).
