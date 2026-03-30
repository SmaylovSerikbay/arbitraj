// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console2} from "forge-std/Script.sol";
import {FlashArb} from "../src/FlashArb.sol";

/// @dev Требуется: `forge install foundry-rs/forge-std --no-commit`
/// Запуск: `forge script script/DeployFlashArb.s.sol:DeployFlashArb --rpc-url $RPC --broadcast`
contract DeployFlashArb is Script {
    address internal constant AAVE_POOL_BASE = 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5;

    function run() external {
        uint256 pk = vm.envUint("PRIVATE_KEY");
        vm.startBroadcast(pk);
        FlashArb c = new FlashArb(AAVE_POOL_BASE);
        console2.log("FlashArb:", address(c));
        vm.stopBroadcast();
    }
}
