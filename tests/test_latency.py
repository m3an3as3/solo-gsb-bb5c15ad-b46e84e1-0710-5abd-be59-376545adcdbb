"""响应时效分析的无第三方依赖验证（python3 tests/test_latency.py）。

不导入 app.main（其依赖 fastapi，本地最小环境可能缺失），而是直接在 SQLite 上
复刻服务端落库的事件流（时钟/组/揭示/回执/答复/重置），调用 app.latency
.build_report 校验：

1. 逐组逐卡时延：首次回执/首次答复与揭示的虚拟毫秒差；定向卡只计目标组；
2. 揭示时的目标组口径：揭示后加入的组不计入、揭示前删除的组不计入；
3. 暂停时长不计入：暂停期间的回执只算冻结虚拟时间；
4. 撤销回执/重新回执/修改答复不改首次响应时间（只认第一条事件）；
5. 重置后从新轮统计：各轮报告互相独立，超出当前轮次返回 None；
6. 事件水位固定：水位之后的事件不混入；未响应为 None（未达），
   中位数只取已响应样本、空样本为 None（无数据）。
"""
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db as dbmod  # noqa: E402
from app import latency as lat  # noqa: E402


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(dbmod.SCHEMA)
    return conn


def add_event(conn, room_id, t, type_, payload):
    cur = conn.execute(
        "INSERT INTO events (room_id,type,payload,created_at) VALUES (?,?,?,?)",
        (room_id, type_, json.dumps(payload, ensure_ascii=False), t))
    conn.commit()
    return cur.lastrowid


def make_room(conn, code="LAT001"):
    cur = conn.execute(
        "INSERT INTO rooms (code,name,status,elapsed_ms,last_resume_at,created_at)"
        " VALUES (?,?, 'paused', 0, NULL, ?)",
        (code, "时效演练", time.time()))
    conn.commit()
    return cur.lastrowid


def add_group(conn, rid, name, join_code, t):
    gid = conn.execute(
        "INSERT INTO groups (room_id,name,join_code,created_at) VALUES (?,?,?,?)",
        (rid, name, join_code, t)).lastrowid
    conn.commit()
    add_event(conn, rid, t, "group_added",
              {"id": gid, "name": name, "join_code": join_code})
    return gid


def add_card(conn, rid, seq, trigger, title, group_id):
    cur = conn.execute(
        "INSERT INTO cards (room_id,seq,trigger_seconds,title,group_id)"
        " VALUES (?,?,?,?,?)", (rid, seq, trigger, title, group_id))
    conn.commit()
    return cur.lastrowid


def reveal(conn, rid, card_id, seq, trigger, title, group_id, t, revealed_at_ms):
    """复刻 ticker 揭示：更新卡片并落 card_revealed 事件。"""
    conn.execute("UPDATE cards SET revealed=1, revealed_at_ms=? WHERE id=?",
                 (revealed_at_ms, card_id))
    conn.commit()
    return add_event(conn, rid, t, "card_revealed", {
        "id": card_id, "seq": seq, "trigger_seconds": trigger, "title": title,
        "content": "", "group_id": group_id, "revealed": True,
        "revealed_at_ms": revealed_at_ms, "answers_locked": False})


def ack(conn, rid, card_id, gid, t, acked=True):
    return add_event(conn, rid, t, "receipt_updated",
                     {"card_id": card_id, "group_id": gid, "acked": acked})


def answer(conn, rid, card_id, gid, t, revision):
    return add_event(conn, rid, t, "answer_updated", {
        "card_id": card_id, "group_id": gid, "body": f"v{revision}",
        "revision": revision, "updated_at": t})


def clock(conn, rid, t, action, elapsed_ms):
    status = {"started": "running", "resumed": "running",
              "paused": "paused"}[action]
    return add_event(conn, rid, t, "clock",
                     {"action": action, "status": status, "elapsed_ms": elapsed_ms})


def high_water(conn, rid):
    return conn.execute(
        "SELECT COALESCE(MAX(id),0) m FROM events WHERE room_id=?", (rid,)).fetchone()["m"]


def report(conn, rid, round_idx, water=None):
    room = conn.execute("SELECT * FROM rooms WHERE id=?", (rid,)).fetchone()
    return lat.build_report(conn, room, water if water is not None else high_water(conn, rid),
                            round_idx)


def group_of(rep, gid):
    return next((g for g in rep["groups"] if g["group_id"] == gid), None)


def card_of(grp, card_id):
    return next((c for c in grp["cards"] if c["card_id"] == card_id), None)


def test_basic_latency_and_targets():
    """逐组逐卡时延、定向卡只计目标组、两类中位数。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn)
        g1 = add_group(conn, rid, "红队", "AAA111", t0 + 1)
        g2 = add_group(conn, rid, "蓝队", "BBB222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "全员卡", None)
        c2 = add_card(conn, rid, 2, 20, "定向红队", g1)
        clock(conn, rid, t0 + 10, "started", 0)  # 墙钟 t0+10 = 虚拟 0

        # c1 全员卡揭示于虚拟 10s：红队 4s 后回执、30s 后答复；蓝队 9s 后回执、不答复
        reveal(conn, rid, c1, 1, 10, "全员卡", None, t0 + 20, 10000)
        ack(conn, rid, c1, g1, t0 + 24)
        ack(conn, rid, c1, g2, t0 + 29)
        answer(conn, rid, c1, g1, t0 + 40, 1)
        # c2 定向红队揭示于虚拟 20s：红队 5s 后回执，不答复；蓝队不是目标组
        reveal(conn, rid, c2, 2, 20, "定向红队", g1, t0 + 30, 20000)
        ack(conn, rid, c2, g1, t0 + 35)

        rep = report(conn, rid, 0)
        assert rep["round"] == 0 and rep["current_round"] == 0
        assert rep["high_water_event_id"] == high_water(conn, rid)

        r1 = group_of(rep, g1)
        assert r1["group_name"] == "红队" and r1["present"] is True
        a1 = card_of(r1, c1)
        assert a1["receipt_ms"] == 4000 and a1["answer_ms"] == 20000
        assert a1["receipt_at_ms"] == 14000 and a1["answer_at_ms"] == 30000
        a2 = card_of(r1, c2)
        assert a2["receipt_ms"] == 5000 and a2["answer_ms"] is None  # 答复未达
        # 中位数：回执样本 [4000, 5000] → 4500；答复样本 [20000] → 20000
        assert r1["median_receipt_ms"] == 4500
        assert r1["median_answer_ms"] == 20000

        r2 = group_of(rep, g2)
        b1 = card_of(r2, c1)
        assert b1["receipt_ms"] == 9000 and b1["answer_ms"] is None
        assert card_of(r2, c2) is None  # 定向卡不计入非目标组
        assert r2["median_receipt_ms"] == 9000
        assert r2["median_answer_ms"] is None  # 空样本 = 无数据
        conn.close()
    print("ok: per-group per-card latency, directed targets, medians")


def test_targets_at_reveal_time():
    """目标组按揭示时刻确定：之后加入/之前删除的组不计入该卡样本。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT002")
        g1 = add_group(conn, rid, "一队", "G11111", t0 + 1)
        g2 = add_group(conn, rid, "二队", "G22222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "卡一", None)
        c2 = add_card(conn, rid, 2, 20, "卡二", None)
        c3 = add_card(conn, rid, 3, 30, "卡三", None)
        clock(conn, rid, t0 + 10, "started", 0)

        reveal(conn, rid, c1, 1, 10, "卡一", None, t0 + 20, 10000)  # 目标 = {g1,g2}
        g3 = add_group(conn, rid, "三队", "G33333", t0 + 25)         # 揭示后才加入
        reveal(conn, rid, c2, 2, 20, "卡二", None, t0 + 30, 20000)  # 目标 = {g1,g2,g3}
        add_event(conn, rid, t0 + 35, "group_removed", {"id": g2})  # 揭示前删除
        reveal(conn, rid, c3, 3, 30, "卡三", None, t0 + 40, 30000)  # 目标 = {g1,g3}

        # 各组都回执：时延应只按各自目标卡统计
        ack(conn, rid, c1, g1, t0 + 22)
        ack(conn, rid, c1, g2, t0 + 23)
        ack(conn, rid, c2, g3, t0 + 32)
        ack(conn, rid, c3, g1, t0 + 42)
        ack(conn, rid, c3, g3, t0 + 43)

        rep = report(conn, rid, 0)
        r1, r2, r3 = group_of(rep, g1), group_of(rep, g2), group_of(rep, g3)
        assert [c["card_id"] for c in r1["cards"]] == [c1, c2, c3]
        assert [c["card_id"] for c in r2["cards"]] == [c1, c2]   # c3 揭示前已删除
        assert [c["card_id"] for c in r3["cards"]] == [c2, c3]   # c1 揭示后才加入
        assert r2["present"] is False                            # 已删除组仍列出样本
        assert r2["group_name"] == "二队"
        # 时延核对
        assert card_of(r1, c1)["receipt_ms"] == 2000
        assert card_of(r2, c1)["receipt_ms"] == 3000
        assert card_of(r3, c2)["receipt_ms"] == 2000
        assert card_of(r1, c3)["receipt_ms"] == 2000
        assert card_of(r3, c3)["receipt_ms"] == 3000
        conn.close()
    print("ok: target groups fixed at reveal time (late join / early removal excluded)")


def test_pause_excluded():
    """暂停时长不计入：暂停期间的回执只按冻结虚拟时间计。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT003")
        g1 = add_group(conn, rid, "红队", "PAU111", t0 + 1)
        g2 = add_group(conn, rid, "蓝队", "PAU222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "卡", None)
        clock(conn, rid, t0, "started", 0)              # 墙钟 t0 = 虚拟 0
        reveal(conn, rid, c1, 1, 10, "卡", None, t0 + 10, 10000)
        clock(conn, rid, t0 + 20, "paused", 20000)      # 虚拟冻结在 20s
        # 墙钟过去 80 秒（暂停中）回执：虚拟时间仍是 20s → 时延 10s
        ack(conn, rid, c1, g1, t0 + 100)
        clock(conn, rid, t0 + 200, "resumed", 20000)    # 继续：虚拟从 20s 走
        ack(conn, rid, c1, g2, t0 + 205)                # 虚拟 25s → 时延 15s

        rep = report(conn, rid, 0)
        assert card_of(group_of(rep, g1), c1)["receipt_ms"] == 10000
        assert card_of(group_of(rep, g2), c1)["receipt_ms"] == 15000
        conn.close()
    print("ok: paused wall-clock duration excluded from latency")


def test_unack_and_edit_keep_first_response():
    """撤销/重新回执、修改答复均不改首次响应时间（只认第一条事件）。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT004")
        g1 = add_group(conn, rid, "红队", "UNA111", t0 + 1)
        g2 = add_group(conn, rid, "蓝队", "UNA222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "卡", None)
        clock(conn, rid, t0, "started", 0)
        reveal(conn, rid, c1, 1, 10, "卡", None, t0 + 10, 10000)

        # 红队：回执 → 撤销 → 重新回执；答复 v1 → v2。首次时间不变
        ack(conn, rid, c1, g1, t0 + 12)                  # 首次回执 虚拟 12s
        ack(conn, rid, c1, g1, t0 + 13, acked=False)     # 撤销
        ack(conn, rid, c1, g1, t0 + 15)                  # 重新回执（不影响首次）
        answer(conn, rid, c1, g1, t0 + 20, 1)            # 首次答复 虚拟 20s
        answer(conn, rid, c1, g1, t0 + 30, 2)            # 修改（不影响首次）
        # 蓝队：先撤销（无回执可撤，防御）再回执
        ack(conn, rid, c1, g2, t0 + 11, acked=False)
        ack(conn, rid, c1, g2, t0 + 14)                  # 首次回执 虚拟 14s

        rep = report(conn, rid, 0)
        a1 = card_of(group_of(rep, g1), c1)
        assert a1["receipt_ms"] == 2000 and a1["receipt_at_ms"] == 12000
        assert a1["answer_ms"] == 10000 and a1["answer_at_ms"] == 20000
        a2 = card_of(group_of(rep, g2), c1)
        assert a2["receipt_ms"] == 4000
        conn.close()
    print("ok: un-ack / re-ack / answer edits keep first response time")


def test_reset_starts_new_round():
    """重置后从新轮统计：各轮报告独立；超出当前轮次返回 None。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT005")
        g1 = add_group(conn, rid, "红队", "RST111", t0 + 1)
        g2 = add_group(conn, rid, "蓝队", "RST222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "卡", None)
        # 第一轮：红队 2s 回执
        clock(conn, rid, t0 + 10, "started", 0)
        reveal(conn, rid, c1, 1, 10, "卡", None, t0 + 20, 10000)
        ack(conn, rid, c1, g1, t0 + 22)
        clock(conn, rid, t0 + 30, "paused", 20000)
        add_event(conn, rid, t0 + 35, "reset",
                  {"action": "reset", "status": "idle", "elapsed_ms": 0})
        # 复刻 reset 路由：收回揭示、清空回执/答复
        conn.execute("UPDATE cards SET revealed=0, revealed_at_ms=NULL WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM receipts WHERE room_id=?", (rid,))
        conn.execute("DELETE FROM answers WHERE room_id=?", (rid,))
        conn.commit()
        # 第二轮：同一张卡重新揭示，红队 15s 才回执，蓝队不响应
        clock(conn, rid, t0 + 40, "started", 0)
        reveal(conn, rid, c1, 1, 10, "卡", None, t0 + 50, 10000)
        ack(conn, rid, c1, g1, t0 + 65)

        rep0 = report(conn, rid, 0)
        assert rep0["current_round"] == 1
        a0 = card_of(group_of(rep0, g1), c1)
        assert a0["receipt_ms"] == 2000          # 第一轮统计不受第二轮影响
        b0 = card_of(group_of(rep0, g2), c1)
        assert b0["receipt_ms"] is None          # 蓝队第一轮未达

        rep1 = report(conn, rid, 1)
        a1 = card_of(group_of(rep1, g1), c1)
        assert a1["receipt_ms"] == 15000         # 新一轮从重新揭示起算
        b1 = card_of(group_of(rep1, g2), c1)
        assert b1["receipt_ms"] is None          # 未响应 = 未达（不用墙钟补足）
        assert group_of(rep1, g1)["median_receipt_ms"] == 15000
        assert group_of(rep1, g2)["median_receipt_ms"] is None

        assert report(conn, rid, 2) is None      # 超出当前轮次
        conn.close()
    print("ok: reset starts a fresh round; rounds are independent")


def test_water_level_and_unreached():
    """水位之后的事件不混入；全未响应时明细为未达、中位数为无数据。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT006")
        g1 = add_group(conn, rid, "红队", "WAT111", t0 + 1)
        g2 = add_group(conn, rid, "蓝队", "WAT222", t0 + 2)
        c1 = add_card(conn, rid, 1, 10, "卡一", None)
        c2 = add_card(conn, rid, 2, 20, "卡二", None)
        clock(conn, rid, t0 + 10, "started", 0)
        e_rev1 = reveal(conn, rid, c1, 1, 10, "卡一", None, t0 + 20, 10000)
        reveal(conn, rid, c2, 2, 20, "卡二", None, t0 + 30, 20000)
        ack(conn, rid, c1, g1, t0 + 24)          # 水位之后（相对 e_rev1）
        answer(conn, rid, c1, g1, t0 + 26, 1)

        # 水位取在 c1 揭示事件：之后的揭示/回执/答复一律不混入
        rep = report(conn, rid, 0, water=e_rev1)
        r1 = group_of(rep, g1)
        assert [c["card_id"] for c in r1["cards"]] == [c1]
        a1 = card_of(r1, c1)
        assert a1["receipt_ms"] is None and a1["answer_ms"] is None
        assert a1["receipt_at_ms"] is None and a1["answer_at_ms"] is None
        assert r1["median_receipt_ms"] is None and r1["median_answer_ms"] is None
        r2 = group_of(rep, g2)
        assert card_of(r2, c1)["receipt_ms"] is None
        assert r2["median_receipt_ms"] is None

        # 完整水位：红队回执 4s、答复 6s；卡二两组均未达 → 中位数只取已响应样本
        full = report(conn, rid, 0)
        fr1 = group_of(full, g1)
        assert card_of(fr1, c1)["receipt_ms"] == 4000
        assert card_of(fr1, c1)["answer_ms"] == 6000
        assert card_of(fr1, c2)["receipt_ms"] is None
        assert fr1["median_receipt_ms"] == 4000   # 未达样本不计入中位数
        assert fr1["median_answer_ms"] == 6000
        conn.close()
    print("ok: pinned water level; unreached shown as 未达; medians skip empty samples")


def test_median_even_samples():
    """偶数样本中位数取中间两值平均；只取已响应样本。"""
    with tempfile.TemporaryDirectory() as d:
        conn = open_db(os.path.join(d, "t.db"))
        t0 = 1000.0
        rid = make_room(conn, "LAT007")
        g1 = add_group(conn, rid, "红队", "MED111", t0 + 1)
        cards = [add_card(conn, rid, i + 1, (i + 1) * 10, f"卡{i + 1}", None)
                 for i in range(3)]
        clock(conn, rid, t0, "started", 0)
        # 三张卡分别揭示于虚拟 10/20/30s，红队回执时延 4s / 8s / 未达
        for i, cid in enumerate(cards):
            reveal(conn, rid, cid, i + 1, (i + 1) * 10, f"卡{i + 1}", None,
                   t0 + (i + 1) * 10, (i + 1) * 10000)
        ack(conn, rid, cards[0], g1, t0 + 14)   # 时延 4000
        ack(conn, rid, cards[1], g1, t0 + 28)   # 时延 8000
        answer(conn, rid, cards[0], g1, t0 + 15, 1)  # 答复时延 5000
        answer(conn, rid, cards[1], g1, t0 + 26, 1)  # 答复时延 6000
        answer(conn, rid, cards[2], g1, t0 + 39, 1)  # 答复时延 9000

        rep = report(conn, rid, 0)
        r1 = group_of(rep, g1)
        assert r1["median_receipt_ms"] == 6000   # [4000, 8000] 均值
        assert r1["median_answer_ms"] == 6000    # [5000, 6000, 9000] 中值
        conn.close()
    print("ok: medians over responded samples only (even count averaged)")


if __name__ == "__main__":
    test_basic_latency_and_targets()
    test_targets_at_reveal_time()
    test_pause_excluded()
    test_unack_and_edit_keep_first_response()
    test_reset_starts_new_round()
    test_water_level_and_unreached()
    test_median_even_samples()
    print("ALL LATENCY TESTS PASSED")
