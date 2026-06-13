"""A small, honest bar-by-bar backtester.

Design choices that matter:
- No lookahead: a signal computed from data through bar i's close is acted on
  starting bar i+1 (positions are the signals shifted by one bar). A strategy
  cannot trade on the same bar it just saw close.
- Costs are real: fees + slippage are charged on traded notional every time the
  position changes (a flip from long to short pays both sides).
- Long, short, and flat are all supported via a target position in [-1, 1],
  where 1 means fully long, -1 fully short, 0 flat.

The model is normalized (position 1 == 100% of current equity); returns compound
into the equity curve. This matches the "any OHLCV, model the costs, don't
curve-fit" approach.
"""

from dataclasses import dataclass
from typing import List

import pandas as pd

from . import metrics


@dataclass
class Trade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: int  # +1 long, -1 short
    entry_price: float
    exit_price: float
    return_pct: float  # net of costs, over the bars the position was held
    bars: int
    is_open: bool = False  # still held at the end of the data (no exit observed)


@dataclass
class BacktestResult:
    equity: pd.Series
    returns: pd.Series      # net per-bar returns
    positions: pd.Series    # position held during each bar (post-shift)
    trades: List[Trade]
    metrics: dict

    def report(self) -> str:
        m = self.metrics

        def fmt(value, pct=False):
            if value != value:  # NaN
                return f"{'n/a':>10}"
            if value in (float("inf"), float("-inf")):
                return f"{value:>10.1f}"  # 'inf' / '-inf'
            return f"{value:>10.2%}" if pct else f"{value:>10.2f}"

        return (
            f"{'return':<14}{fmt(m['total_return'], pct=True)}\n"
            f"{'CAGR':<14}{fmt(m['cagr'], pct=True)}\n"
            f"{'Sharpe':<14}{fmt(m['sharpe'])}\n"
            f"{'Sortino':<14}{fmt(m['sortino'])}\n"
            f"{'max drawdown':<14}{fmt(m['max_drawdown'], pct=True)}\n"
            f"{'win rate':<14}{fmt(m['win_rate'], pct=True)}\n"
            f"{'profit factor':<14}{fmt(m['profit_factor'])}\n"
            f"{'expectancy':<14}{fmt(m['expectancy'], pct=True)}\n"
            f"{'trades':<14}{m['num_trades']:>10d}\n"
            f"{'exposure':<14}{fmt(m['exposure'], pct=True)}\n"
            f"{'final equity':<14}{m['final_equity']:>10.2f}"
        )


def _extract_trades(close, positions, net, index) -> List[Trade]:
    """Walk the position series and build one Trade per constant nonzero run.

    A run ends when the position returns to flat or flips sign. The trade return
    is the compounded net return over the run, so it already includes costs and
    ties out to the equity curve.
    """
    trades: List[Trade] = []
    pos_vals = positions.values
    n = len(pos_vals)
    i = 0
    while i < n:
        side_val = pos_vals[i]
        if side_val == 0:
            i += 1
            continue
        side = 1 if side_val > 0 else -1
        start = i
        # Extend while the sign stays the same.
        while i < n and (pos_vals[i] > 0) == (side > 0) and pos_vals[i] != 0:
            i += 1
        end = i - 1  # last bar of the run
        seg_net = net.iloc[start:end + 1]
        ret = float((1 + seg_net).prod() - 1)
        # The position is entered at the close of the bar before the run (that's
        # where the first held bar's return is measured from), and held through
        # close[end]. end == n-1 means the data ended with the position still on.
        entry_price = float(close.iloc[start - 1]) if start > 0 else float(close.iloc[start])
        trades.append(Trade(
            entry_time=index[start],
            exit_time=index[end],
            side=side,
            entry_price=entry_price,
            exit_price=float(close.iloc[end]),
            return_pct=ret,
            bars=end - start + 1,
            is_open=(end == n - 1),
        ))
    return trades


def run_backtest(
    df: pd.DataFrame,
    strategy,
    *,
    fee_bps: float = 10.0,
    slippage_bps: float = 0.0,
    initial_capital: float = 10_000.0,
) -> BacktestResult:
    """Run ``strategy`` over OHLCV ``df`` and return equity, trades, and metrics.

    Args:
        df: OHLCV indexed by a DatetimeIndex; must contain a 'close' column.
        strategy: object with ``generate_signals(df) -> Series`` of target
            positions in [-1, 1].
        fee_bps: fee per side in basis points (10 bps = 0.10%).
        slippage_bps: slippage per side in basis points.
        initial_capital: starting equity.
    """
    if "close" not in df.columns:
        raise ValueError("df must have a 'close' column")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("df must be indexed by a DatetimeIndex")
    if len(df) == 0:
        raise ValueError("df is empty")
    if not df.index.is_monotonic_increasing:
        raise ValueError("df index must be sorted ascending (call data.from_dataframe)")

    close = df["close"].astype(float)
    if close.isna().any():
        raise ValueError("close has NaN values; clean or forward-fill before backtesting")

    raw = strategy.generate_signals(df)
    raw = raw.reindex(df.index).fillna(0.0).clip(-1.0, 1.0)
    # Snap float-residual positions (e.g. 1e-13) to flat so they don't open
    # phantom micro-trades.
    raw = raw.where(raw.abs() > 1e-9, 0.0)

    # No lookahead: hold bar i's position based on the signal from bar i-1.
    positions = raw.shift(1).fillna(0.0)

    bar_returns = close.pct_change().fillna(0.0)
    gross = positions * bar_returns

    # Cost on traded notional whenever the position changes (per side).
    turnover = positions.diff().abs().fillna(positions.abs())
    cost_rate = (fee_bps + slippage_bps) / 10_000.0
    cost = turnover * cost_rate

    # Floor the per-bar loss at -100%: with |position| <= 1 a worse move means
    # ruin (e.g. a short when price more than doubles in a bar). This keeps
    # equity >= 0, so it can't flip sign and stage a fake "recovery".
    net = (gross - cost).clip(lower=-1.0)
    equity = (1 + net).cumprod() * initial_capital

    trades = _extract_trades(close, positions, net, df.index)
    # Realized stats exclude a still-open final trade (no exit cost charged yet).
    closed_returns = [t.return_pct for t in trades if not t.is_open]
    # Drop the structural first bar (always flat, zero return) from the risk
    # metrics so it doesn't bias Sharpe/Sortino on short windows.
    report = metrics.summarize(equity, net.iloc[1:], positions.iloc[1:], closed_returns)

    return BacktestResult(equity=equity, returns=net, positions=positions,
                          trades=trades, metrics=report)
