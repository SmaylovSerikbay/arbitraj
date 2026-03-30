// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title FlashArb — Aave V3 flash loan (simple) + two-leg arb on Base
/// @notice Buy via arbitrary router calldata (recipient MUST be this contract).
///         Sell via UniswapV2-style swapExactTokensForTokens or UniV3 exactInputSingle using full quote balance.

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IPool {
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

interface IFlashLoanSimpleReceiver {
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool);
}

interface ISwapRouterV3 {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

interface IUniV2Router {
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

contract FlashArb is IFlashLoanSimpleReceiver {
    address public immutable POOL;
    address public owner;

    event ArbExecuted(address indexed asset, uint256 amount, uint256 premium, address quoteToken);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    error NotOwner();
    error NotPool();
    error BadInitiator();
    error BuyFailed();
    error TaxOrSlippage();
    error SellFailed();
    error ProfitTooLow();
    error TransferFailed();

    modifier onlyOwner() {
        _onlyOwner();
        _;
    }

    function _onlyOwner() internal view {
        if (msg.sender != owner) revert NotOwner();
    }

    constructor(address pool_) {
        if (pool_ == address(0)) revert();
        POOL = pool_;
        owner = msg.sender;
        emit OwnershipTransferred(address(0), msg.sender);
    }

    function transferOwnership(address newOwner) external onlyOwner {
        if (newOwner == address(0)) revert();
        emit OwnershipTransferred(owner, newOwner);
        owner = newOwner;
    }

    /// @notice Pull ERC20 profits (use type(uint256).max for full balance)
    function withdrawToken(address token, uint256 amount) external onlyOwner {
        uint256 bal = IERC20(token).balanceOf(address(this));
        uint256 sendAmt = amount == type(uint256).max ? bal : amount;
        if (sendAmt == 0) return;
        if (!IERC20(token).transfer(owner, sendAmt)) revert TransferFailed();
    }

    /// @param v3Sell If true, sellRouter must be UniV3 SwapRouter02-compatible with exactInputSingle
    /// @param v3Fee Pool fee tier when v3Sell (e.g. 3000)
    function executeArb(
        address asset,
        uint256 amount,
        address buyRouter,
        bytes calldata buyCalldata,
        address sellRouter,
        address quoteToken,
        bool v3Sell,
        uint24 v3Fee,
        uint256 minWethOut,
        uint256 minTokenAfterBuy,
        uint256 minProfitWei,
        uint256 deadline
    ) external onlyOwner {
        bytes memory inner = abi.encode(
            buyRouter,
            buyCalldata,
            sellRouter,
            quoteToken,
            v3Sell,
            v3Fee,
            minWethOut,
            minTokenAfterBuy,
            minProfitWei,
            deadline
        );
        IPool(POOL).flashLoanSimple(address(this), asset, amount, inner, 0);
    }

    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        if (msg.sender != POOL) revert NotPool();
        if (initiator != address(this)) revert BadInitiator();

        (
            address buyRouter,
            bytes memory buyCalldata,
            address sellRouter,
            address quoteToken,
            bool v3Sell,
            uint24 v3Fee,
            uint256 minWethOut,
            uint256 minTokenAfterBuy,
            uint256 minProfitWei,
            uint256 deadline
        ) = abi.decode(params, (address, bytes, address, address, bool, uint24, uint256, uint256, uint256, uint256));

        if (!IERC20(asset).approve(buyRouter, amount)) revert BuyFailed();
        (bool okBuy,) = buyRouter.call(buyCalldata);
        if (!okBuy) revert BuyFailed();

        uint256 tBal = IERC20(quoteToken).balanceOf(address(this));
        if (tBal < minTokenAfterBuy) revert TaxOrSlippage();

        // Reset allowance for weird tokens, then set exact balance
        IERC20(quoteToken).approve(sellRouter, 0);
        if (!IERC20(quoteToken).approve(sellRouter, tBal)) revert SellFailed();

        if (!v3Sell) {
            address[] memory path = new address[](2);
            path[0] = quoteToken;
            path[1] = asset;
            try IUniV2Router(sellRouter).swapExactTokensForTokens(tBal, minWethOut, path, address(this), deadline) returns (
                uint256[] memory
            ) {
                // ok
            } catch {
                revert SellFailed();
            }
        } else {
            try
                ISwapRouterV3(sellRouter).exactInputSingle(
                    ISwapRouterV3.ExactInputSingleParams({
                        tokenIn: quoteToken,
                        tokenOut: asset,
                        fee: v3Fee,
                        recipient: address(this),
                        deadline: deadline,
                        amountIn: tBal,
                        amountOutMinimum: minWethOut,
                        sqrtPriceLimitX96: 0
                    })
                )
            returns (uint256) {
                // ok
            } catch {
                revert SellFailed();
            }
        }

        uint256 owe = amount + premium;
        uint256 wethBal = IERC20(asset).balanceOf(address(this));
        if (wethBal < owe + minProfitWei) revert ProfitTooLow();

        if (!IERC20(asset).approve(POOL, owe)) revert ProfitTooLow();

        emit ArbExecuted(asset, amount, premium, quoteToken);
        return true;
    }

    receive() external payable {}
}
