"""响应时效分析（主持人专用）：逐组逐卡的首次回执 / 首次答复虚拟时延。

一致性：调用方（main.py 的路由）必须在 state_lock 内、与所有写操作同一把锁
先读取水位 high_water（events 表 MAX(id)），本模块只消费 id <= high_water
的事件——与复盘包导出同一水位口径，分析期间到达的新事件绝不混入。

统计口径：
- 轮次与虚拟时间完全由时钟事件（clock/reset）重建，直接复用复盘导出的
  时间线重放（export._build_timeline）：每次 reset 结束上一轮并开启新一轮；
  事件的虚拟毫秒由其发生时时钟所处的运行段推导，暂停期间冻结——
  **暂停时长天然不计入任何时延**。
- 每张卡以「该轮揭示事件」确定目标组：定向卡 = 目标组；全员卡 = 揭示事件
  之前已加入且尚未被删除的全部参与组（揭示之后才加入的组不计入本卡样本；
  揭示后才删除的组仍保留其样本，名字取事件流中最后一次命名）。
- 首次回执 = 该轮内、揭示事件之后的第一条 receipt_updated(acked=true)；
  首次答复 = 该轮内、揭示事件之后的第一条 answer_updated。
  **撤销回执 / 重新回执 / 修改答复均不改变首次响应时间**（只认第一条）。
- 时延 = 首次响应虚拟毫秒 − 揭示虚拟毫秒（revealed_at_ms 为权威值）；
  重放推导与权威值之间可能存在亚毫秒级偏差，差值不为负（按 0 计）。
  水位内未响应记为「未达」（None），**不用墙钟或当前时钟补足时延**。
- 中位数只取已响应样本；空样本为 None（面板显示「无数据」）。
"""

from __future__ import annotations

import sqlite3
import statistics

from . import export as reviewmod


def _median_or_none(samples: list[int]) -> float | None:
    """中位数只取已响应样本；空样本返回 None（面板显示「无数据」）。"""
    if not samples:
        return None
    return statistics.median(samples)


def build_report(conn: sqlite3.Connection, room: sqlite3.Row,
                 high_water: int, round_idx: int) -> dict | None:
    """生成指定轮次的响应时效报告；round_idx 超出已有轮次时返回 None（路由转 404）。

    只在 state_lock 下调用，且 high_water 为进入临界区后读取的 events MAX(id)。
    """
    room_id = room["id"]
    rows = conn.execute(
        "SELECT id, type, payload, created_at FROM events"
        " WHERE room_id = ? AND id <= ? ORDER BY id",
        (room_id, high_water),
    ).fetchall()
    # 复用时钟事件重放重建轮次与虚拟时间（与复盘包导出同一口径）；
    # 轮次末端的 end_ms 这里用不到，elapsed_now 传 0 即可。
    _, rounds_meta, timeline = reviewmod._build_timeline(rows, 0)
    current = len(rounds_meta) - 1
    if round_idx > current:
        return None
    # 同一轮内虚拟时间随事件 id 单调不减，(round, vtime, id) 排序与 id 排序同序；
    # 仍显式按 id 排，保证「第一条事件」语义与组成员重放严格按发生次序。
    timeline.sort(key=lambda it: it["id"])

    present: dict[int, bool] = {}          # 重放游标处各组是否存在
    names: dict[int, str] = {}             # 各组最近一次命名（含已删除组）
    reveals: dict[int, dict] = {}          # card_id -> 所选轮次的揭示信息
    first_receipt: dict[tuple[int, int], int] = {}  # (card_id, group_id) -> 首次回执虚拟毫秒
    first_answer: dict[tuple[int, int], int] = {}   # (card_id, group_id) -> 首次答复虚拟毫秒

    for it in timeline:
        etype = it["type"]
        p = it["payload"]
        if etype == "group_added":
            present[p["id"]] = True
            names[p["id"]] = p.get("name") or f"#{p['id']}"
        elif etype == "group_renamed":
            names[p["id"]] = p.get("name") or names.get(p["id"], f"#{p['id']}")
        elif etype == "group_removed":
            present[p["id"]] = False
        elif etype == "card_revealed":
            if it["round"] != round_idx:
                continue
            card_id = p["id"]
            if card_id in reveals:
                continue  # 同一轮内一张卡至多揭示一次（防御性）
            if p.get("group_id") is not None:
                targets = [p["group_id"]]  # 定向卡：目标组（被引用的组不可删除）
            else:
                # 全员卡：揭示事件之前已加入且尚未被删除的全部参与组
                targets = sorted(gid for gid, ok in present.items() if ok)
            reveals[card_id] = {
                "card_id": card_id,
                "seq": p.get("seq"),
                "title": p.get("title") or "",
                "group_id": p.get("group_id"),
                "revealed_at_ms": p.get("revealed_at_ms") or 0,  # 权威揭示虚拟毫秒
                "event_id": it["id"],
                "targets": targets,
            }
        elif etype == "receipt_updated":
            if it["round"] != round_idx or not p.get("acked"):
                continue  # 只认本轮的「标记已收到」；撤销事件不产生首次回执
            key = (p.get("card_id"), p.get("group_id"))
            rev = reveals.get(key[0])
            if rev is None or it["id"] <= rev["event_id"]:
                continue  # 非本轮揭示的卡片 / 揭示事件之前的回执（防御性）
            if key not in first_receipt:
                # 首次回执就此固定：之后的撤销/重新回执都不改变它
                first_receipt[key] = it["vtime_ms"]
        elif etype == "answer_updated":
            if it["round"] != round_idx:
                continue
            key = (p.get("card_id"), p.get("group_id"))
            rev = reveals.get(key[0])
            if rev is None or it["id"] <= rev["event_id"]:
                continue
            if key not in first_answer:
                # 首次答复就此固定：之后的修改（revision 递增）都不改变它
                first_answer[key] = it["vtime_ms"]

    # 按组聚合逐卡明细：只列出「揭示时是某卡目标组」的组
    groups_map: dict[int, dict] = {}
    for rev in reveals.values():
        base = rev["revealed_at_ms"]
        for gid in rev["targets"]:
            group = groups_map.setdefault(gid, {
                "group_id": gid,
                "group_name": names.get(gid, f"#{gid}"),
                "present": bool(present.get(gid)),  # 水位时刻该组是否仍存在
                "cards": [],
            })
            rec_at = first_receipt.get((rev["card_id"], gid))
            ans_at = first_answer.get((rev["card_id"], gid))
            group["cards"].append({
                "card_id": rev["card_id"],
                "seq": rev["seq"],
                "title": rev["title"],
                "group_id": rev["group_id"],  # None = 全员卡
                "revealed_at_ms": base,
                # 未响应 = None（面板显示「未达」），不用墙钟补足
                "receipt_ms": max(0, rec_at - base) if rec_at is not None else None,
                "answer_ms": max(0, ans_at - base) if ans_at is not None else None,
                "receipt_at_ms": rec_at,   # 首次回执的虚拟时刻（未达为 None）
                "answer_at_ms": ans_at,    # 首次答复的虚拟时刻（未达为 None）
            })

    groups = []
    for gid in sorted(groups_map):
        group = groups_map[gid]
        group["cards"].sort(
            key=lambda c: (c["revealed_at_ms"], c["seq"] or 0, c["card_id"]))
        # 两类中位时延：只取已响应样本，空样本为 None（面板显示「无数据」）
        group["median_receipt_ms"] = _median_or_none(
            [c["receipt_ms"] for c in group["cards"] if c["receipt_ms"] is not None])
        group["median_answer_ms"] = _median_or_none(
            [c["answer_ms"] for c in group["cards"] if c["answer_ms"] is not None])
        groups.append(group)

    return {
        "round": round_idx,
        "current_round": current,
        "high_water_event_id": high_water,
        "groups": groups,
    }
