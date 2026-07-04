"""Reference policy contracts: join, skew-pull, inventory bands, replace discipline."""

from dataclasses import dataclass

from hyperliquid_pipeline.sim.policy import ReferenceOfiPolicy, _RollingOfi

T0 = 1_700_000_000_000


@dataclass
class _View:
    bid_px: str = "100"
    bid_sz: float = 2.0
    ask_px: str = "101"
    ask_sz: float = 2.0
    crossed: bool = False

    def best_bid(self):
        return (self.bid_px, self.bid_sz)

    def best_ask(self):
        return (self.ask_px, self.ask_sz)

    def mid(self):
        return (float(self.bid_px) + float(self.ask_px)) / 2

    def is_crossed(self):
        return self.crossed


def test_joins_both_touches_when_flat_and_quiet():
    policy = ReferenceOfiPolicy(quote_size=1.0)
    actions = policy.on_block(_View(), inventory=0.0, open_orders=[], t_ms=T0, fills=[])
    placed = {(a.side, a.px) for a in actions if a.kind == "place"}
    assert placed == {("B", "100"), ("A", "101")}


def test_keeps_resting_order_at_desired_price():
    policy = ReferenceOfiPolicy(quote_size=1.0)
    open_orders = [{"order_id": 7, "side": "B", "px": "100", "sz": 1.0}]
    actions = policy.on_block(_View(), 0.0, open_orders, T0, [])
    # bid already resting at the touch: no cancel (queue priority preserved),
    # no duplicate place; only the missing ask is placed
    assert all(a.order_id != 7 for a in actions if a.kind == "cancel")
    placed = [(a.side, a.px) for a in actions if a.kind == "place"]
    assert placed == [("A", "101")]


def test_replaces_when_touch_moves():
    policy = ReferenceOfiPolicy(quote_size=1.0)
    open_orders = [{"order_id": 7, "side": "B", "px": "99", "sz": 1.0}]
    actions = policy.on_block(_View(bid_px="100"), 0.0, open_orders, T0, [])
    cancels = [a.order_id for a in actions if a.kind == "cancel"]
    placed = [(a.side, a.px) for a in actions if a.kind == "place"]
    assert cancels == [7]
    assert ("B", "100") in placed


def test_inventory_band_stops_one_side():
    policy = ReferenceOfiPolicy(quote_size=1.0, inventory_limit=3.0)
    actions = policy.on_block(_View(), inventory=3.5, open_orders=[], t_ms=T0, fills=[])
    placed = {(a.side, a.px) for a in actions if a.kind == "place"}
    assert placed == {("A", "101")}  # too long: no more bids


def test_strong_buy_pressure_pulls_the_ask():
    # warmup=0 so the normalizer trusts the first flicker (test convenience)
    policy = ReferenceOfiPolicy(quote_size=1.0, skew_cut=1.5, ofi_warmup=0)
    # blocks with growing bid size at the same px -> positive OFI events
    for i in range(5):
        view = _View(bid_sz=2.0 + i)
        actions = policy.on_block(view, 0.0, [], T0 + i * 100, [])
    placed = {a.side for a in actions if a.kind == "place"}
    assert placed == {"B"}  # ask pulled: buy pressure runs over resting asks


def test_no_quotes_on_crossed_book():
    policy = ReferenceOfiPolicy(quote_size=1.0)
    open_orders = [{"order_id": 3, "side": "A", "px": "101", "sz": 1.0}]
    actions = policy.on_block(_View(crossed=True), 0.0, open_orders, T0, [])
    assert [a.kind for a in actions] == ["cancel"]  # pull everything, place nothing


def test_width_policy_quotes_behind_the_touch():
    from hyperliquid_pipeline.sim.policy import WidthPolicy

    policy = WidthPolicy(quote_size=1.0, width_ticks=2)
    view = _View(bid_px="100.5", ask_px="100.6")  # tick inferred: 0.1
    actions = policy.on_block(view, 0.0, [], T0, [])
    placed = {(a.side, a.px) for a in actions if a.kind == "place"}
    assert placed == {("B", "100.3"), ("A", "100.8")}  # 2 ticks behind each touch


def test_width_zero_joins_the_touch():
    from hyperliquid_pipeline.sim.policy import WidthPolicy

    policy = WidthPolicy(quote_size=1.0, width_ticks=0)
    actions = policy.on_block(_View(bid_px="100.5", ask_px="100.6"), 0.0, [], T0, [])
    placed = {(a.side, a.px) for a in actions if a.kind == "place"}
    assert placed == {("B", "100.5"), ("A", "100.6")}


def test_width_policy_skews_asymmetrically_on_buy_pressure():
    from hyperliquid_pipeline.sim.policy import WidthPolicy

    policy = WidthPolicy(quote_size=1.0, width_ticks=2, skew_gain=1.0, ofi_warmup=0)
    actions = []
    for i in range(5):  # growing bid size at same px -> positive OFI, signal -> +3
        view = _View(bid_px="100.5", ask_px="100.6", bid_sz=2.0 + i)
        actions = policy.on_block(view, 0.0, [], T0 + i * 100, [])
    placed = dict((a.side, a.px) for a in actions if a.kind == "place")
    # buy pressure: bid tightens toward the touch (offset clamps at 0 = join),
    # ask backs off to width+shift = 5 ticks: 100.6 + 0.5 = 101.1, plain string
    assert placed["B"] == "100.5"
    assert placed["A"] == "101.1"


def test_width_policy_funding_tilt_prefers_short():
    from hyperliquid_pipeline.sim.policy import WidthPolicy

    policy = WidthPolicy(quote_size=1.0, width_ticks=2, funding_tilt_ticks=1.0)
    actions = policy.on_block(_View(bid_px="100.5", ask_px="100.6"), 0.0, [], T0, [])
    placed = dict((a.side, a.px) for a in actions if a.kind == "place")
    assert placed["B"] == "100.2"  # bid backs off one extra tick
    assert placed["A"] == "100.7"  # ask tightens one tick


def test_width_policy_inventory_band():
    from hyperliquid_pipeline.sim.policy import WidthPolicy

    policy = WidthPolicy(quote_size=1.0, width_ticks=1, inventory_limit=2.0)
    actions = policy.on_block(_View(), inventory=2.5, open_orders=[], t_ms=T0, fills=[])
    assert {a.side for a in actions if a.kind == "place"} == {"A"}


def test_rolling_ofi_warmup_suppresses_early_signal():
    ofi = _RollingOfi(window_ms=1000, warmup=5)
    view = _View()
    assert ofi.update(view, T0) == 0.0
    strong = _View(bid_sz=50.0)
    assert ofi.update(strong, T0 + 100) == 0.0  # warmup still holding
