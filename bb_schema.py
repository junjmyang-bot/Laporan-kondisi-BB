from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

TIMEZONE = "Asia/Jakarta"
REPORT_NAME = "LAPORAN KONDISI BB"
HB_UNITS = [f"HB {i}" for i in range(1, 8)]
OX_OPTIONS = ["O", "X"]
UNSTEAM_OPTIONS = ["O", "X", "ISI"]

SHEETS_COLUMNS = [
    "report_name",
    "work_date",
    "team_id",
    "shift",
    "reporter",
    "slot_time",
    "list_read_steam",
    "list_know_bb_size",
    "bb_masuk_summary",
    "steam_basket_empty",
    "steam_handover",
    "steam_prebreak_empty",
    "unsteam_status",
    "unsteam_reason",
    "hb1",
    "hb2",
    "hb3",
    "hb4",
    "hb5",
    "hb6",
    "hb7",
    "defect_flag",
    "defect_types",
    "defect_percent",
    "defect_photo_count",
    "notes",
    "keterangan",
    "idempotency_key",
    "submitted_at",
]


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def round_to_half_hour(dt: datetime) -> time:
    minute = 30 if dt.minute >= 30 else 0
    return dt.replace(minute=minute, second=0, microsecond=0).time()


def slot_times(start_time: time, slot_count: int) -> list[str]:
    base = datetime.combine(date.today(), start_time)
    return [(base + timedelta(minutes=30 * idx)).strftime("%H:%M") for idx in range(slot_count)]


def default_bb_rows() -> list[dict]:
    return [{"supplier": "", "ukuran": "", "tingkat_matang": "O", "selesai": "O"}]


def default_hb_rows() -> list[dict]:
    return [{"hb": hb, "dipakai": "X", "alasan": "", "gas": "X"} for hb in HB_UNITS]


def default_slot(slot_time_text: str) -> dict:
    return {
        "slot_time": slot_time_text,
        "list_read_steam": "O",
        "list_know_bb_size": "O",
        "bb_masuk": default_bb_rows(),
        "steam_basket_empty": "O",
        "steam_handover": "O",
        "steam_prebreak_empty": "O",
        "unsteam_status": "O",
        "unsteam_reason": "",
        "hb_rows": default_hb_rows(),
    }


def base_payload(work_date_text: str, reporter: str, team_id: str, shift: str, slot_count: int, start_time: time) -> dict:
    slots = [default_slot(t) for t in slot_times(start_time, slot_count)]
    return {
        "report_name": REPORT_NAME,
        "work_date": work_date_text,
        "team_id": team_id,
        "shift": shift,
        "reporter": reporter,
        "slots": slots,
        "defect_flag": False,
        "defect_types": "",
        "defect_percent": "",
        "defect_photo_count": 0,
        "notes": "",
        "keterangan": "",
    }


def clone_payload(payload: dict) -> dict:
    return deepcopy(payload)
