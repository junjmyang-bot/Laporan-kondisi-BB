from __future__ import annotations

import json
import os
import secrets
import threading
import uuid
from datetime import date, datetime, time
from pathlib import Path

import streamlit as st

from bb_formatters import build_sheets_rows, build_telegram_text, split_telegram_parts
from bb_integrations import (
    clear_pending_submission,
    load_pending_submission,
    process_submission,
    save_pending_submission,
)
from bb_schema import (
    HB_UNITS,
    OX_OPTIONS,
    REPORT_NAME,
    SHEETS_COLUMNS,
    TIMEZONE,
    clone_payload,
    now_local,
    round_to_half_hour,
    slot_times,
)
from bb_validation import validate_payload

PERSIST_PATH = Path('.laporan_kondisi_bb_state.json')
STATE_IO_LOCK = threading.RLock()
LOCK_TTL_SECONDS = 600
STATE_PREFIX_KEYS = (
    'g3_',
    'g4_',
    'g5_',
    'g6_',
    'g7_',
)
TEAM_PASSWORDS_ERROR: str | None = None


def _load_team_passwords() -> dict:
    global TEAM_PASSWORDS_ERROR
    raw = os.getenv('TEAM_PASSWORDS', '').strip()
    if not raw:
        TEAM_PASSWORDS_ERROR = 'TEAM_PASSWORDS belum diatur. Set dulu di secrets/env.'
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed:
            TEAM_PASSWORDS_ERROR = None
            return parsed
    except Exception:
        pass
    TEAM_PASSWORDS_ERROR = 'TEAM_PASSWORDS tidak valid. Format harus JSON object.'
    return {}


TEAM_PASSWORDS = _load_team_passwords()
TEAM_LABELS = {
    'KUPAS-1': 'Kupas team Erika',
    'KUPAS-2': 'Kupas team Elok',
    'KUPAS-3': 'Kupas Extra',
}
SLOT_STATUS_OPTIONS = ['Mulai kerja baru', 'Lanjut kerja', 'Selesai di sini']
ALASAN_CODE_MAP = {
    '1': 'proses isi',
    '2': 'ubi cilembu',
    '3': 'ubi ungu',
    '4': 'jagung',
    '5': 'steril',
    '6': 'kosong',
    '7': 'tidak dipakai',
}


def now_iso() -> str:
    return now_local().isoformat()


def _render_ox(label: str, key: str) -> str:
    return str(st.radio(label, OX_OPTIONS, index=None, horizontal=True, key=key) or "").strip().upper()


def _next_slot_hhmm(hhmm: str) -> str:
    try:
        h, m = str(hhmm).split(':', 1)
        total = (int(h) * 60 + int(m) + 30) % (24 * 60)
        return f'{total // 60:02d}:{total % 60:02d}'
    except Exception:
        return '00:00'


def _parse_hhmm_text(raw: str) -> str | None:
    text = str(raw or '').strip()
    if not text:
        return None
    try:
        h, m = text.split(':', 1)
        hh = int(h)
        mm = int(m)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f'{hh:02d}:{mm:02d}'
    except Exception:
        return None
    return None


def _slot_sort_key(hhmm: str) -> tuple[int, int]:
    parsed = _parse_hhmm_text(hhmm)
    if not parsed:
        return 99, 99
    h, m = parsed.split(':', 1)
    return int(h), int(m)


def _minutes(hhmm: str) -> int:
    parsed = _parse_hhmm_text(hhmm)
    if not parsed:
        return -1
    h, m = parsed.split(':', 1)
    return int(h) * 60 + int(m)


def _next_available_after(base_hhmm: str, existing: list[str]) -> str:
    candidate = _next_slot_hhmm(base_hhmm)
    seen = set(existing)
    for _ in range(48):
        if candidate not in seen:
            return candidate
        candidate = _next_slot_hhmm(candidate)
    return candidate


def _slot_token(hhmm: str) -> str:
    parsed = _parse_hhmm_text(hhmm)
    if not parsed:
        raw = ''.join(ch for ch in str(hhmm) if ch.isalnum()).lower()
        return f'raw_{raw or "empty"}'
    return parsed.replace(':', '')


def _migrate_session_key(new_key: str, old_key: str) -> None:
    if old_key and new_key not in st.session_state and old_key in st.session_state:
        st.session_state[new_key] = st.session_state.get(old_key)


def _request_persist() -> None:
    st.session_state['_pending_persist'] = True


def _render_start_time_input(label: str, key: str) -> str:
    raw = st.text_input(label, key=key, placeholder='HH:MM', on_change=_request_persist)
    parsed = _parse_hhmm_text(str(raw))
    if parsed:
        return parsed
    if str(raw or '').strip():
        st.warning(f'{label}: format jam harus HH:MM.')
    return '00:00'


def _ensure_group_slots(key: str, start_hhmm: str) -> list[str]:
    parsed_start = _parse_hhmm_text(start_hhmm) or '00:00'
    if key not in st.session_state or not isinstance(st.session_state.get(key), list) or not st.session_state.get(key):
        st.session_state[key] = [parsed_start]
    cleaned = [str(x) for x in st.session_state.get(key, []) if str(x).strip()]
    if not cleaned:
        cleaned = [parsed_start]
    st.session_state[key] = cleaned
    return cleaned


def _normalize_initial_slots_once() -> None:
    if st.session_state.get('_slots_normalized_once'):
        return
    changed = False
    for key in ('slots_3', 'slots_4', 'slots_5', 'slots_6', 'slots_7'):
        vals = st.session_state.get(key)
        if isinstance(vals, list) and len(vals) > 1:
            st.session_state[key] = [str(vals[0])]
            changed = True
    st.session_state['_slots_normalized_once'] = True
    if changed:
        st.rerun()


def _is_persistable_dynamic_key(key: str) -> bool:
    if not key.startswith(STATE_PREFIX_KEYS):
        return False
    transient_tokens = (
        '_add',
        '_remove',
        'ask_rm_',
        'yes_rm_',
        'no_rm_',
        'next_',
        'confirm_rm_',
    )
    return not any(token in key for token in transient_tokens)


def _persist_signature(payload: dict) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return ''


def _legacy_slot_indices(group_no: int) -> list[int]:
    out: set[int] = set()
    for key in st.session_state.keys():
        parts = str(key).split('_')
        if group_no == 3:
            if key.startswith('g3_') and len(parts) == 3 and parts[2].isdigit():
                idx = int(parts[2])
                if 0 <= idx <= 47:
                    out.add(idx)
        elif group_no == 4:
            if key.startswith('g4_note_') and len(parts) == 3 and parts[2].isdigit():
                idx = int(parts[2])
                if 0 <= idx <= 47:
                    out.add(idx)
            elif key.startswith('g4_bb_count_') and len(parts) == 4 and parts[3].isdigit():
                idx = int(parts[3])
                if 0 <= idx <= 47:
                    out.add(idx)
            elif key.startswith('g4_bb_') and len(parts) >= 5 and parts[3].isdigit():
                idx = int(parts[3])
                if 0 <= idx <= 47:
                    out.add(idx)
        elif group_no == 5:
            if key.startswith('g5_') and len(parts) == 3 and parts[2].isdigit():
                idx = int(parts[2])
                if 0 <= idx <= 47:
                    out.add(idx)
        elif group_no == 6:
            if key.startswith('g6_') and len(parts) == 3 and parts[2].isdigit():
                idx = int(parts[2])
                if 0 <= idx <= 47:
                    out.add(idx)
        elif group_no == 7:
            if key.startswith('g7_note_') and len(parts) == 3 and parts[2].isdigit():
                idx = int(parts[2])
                if 0 <= idx <= 47:
                    out.add(idx)
            elif key.startswith('g7_hb_') and len(parts) >= 5 and parts[3].isdigit():
                idx = int(parts[3])
                if 0 <= idx <= 47:
                    out.add(idx)
    return sorted(out)


def _maybe_restore_legacy_slots(slots_key: str, start_key: str, group_no: int) -> None:
    current = st.session_state.get(slots_key)
    if not isinstance(current, list):
        return
    current_clean = [str(x) for x in current if _parse_hhmm_text(str(x))]
    if len(current_clean) > 1:
        return

    legacy_idxs = _legacy_slot_indices(group_no)
    if not legacy_idxs:
        return
    needed = max(legacy_idxs) + 1
    if needed <= len(current_clean):
        return

    start_text = _parse_hhmm_text(str(st.session_state.get(start_key, ''))) or round_to_half_hour(now_local()).strftime('%H:%M')
    inferred = slot_times(_parse_time_hhmm(start_text), needed)
    if inferred:
        st.session_state[slots_key] = inferred


def _render_bb_rows(prefix: str, slot_token: str, seed_rows: list[dict], legacy_slot_idx: int | None = None) -> list[dict]:
    count_key = f'{prefix}_bb_count_{slot_token}'
    old_count_key = f'{prefix}_bb_count_{legacy_slot_idx}' if legacy_slot_idx is not None else ''
    seed_count = len(seed_rows) if isinstance(seed_rows, list) and seed_rows else 1
    if count_key not in st.session_state:
        if old_count_key and old_count_key in st.session_state:
            try:
                seed_count = int(st.session_state.get(old_count_key, seed_count))
            except Exception:
                pass
        st.session_state[count_key] = max(1, min(10, seed_count))
    row_count = int(st.session_state[count_key])
    rc1, rc2, rc3 = st.columns([1, 1, 4])
    if rc1.button('Tambah supplier', key=f'{count_key}_add', disabled=row_count >= 10):
        st.session_state[count_key] = row_count + 1
        st.rerun()
    if rc2.button('Hapus supplier', key=f'{count_key}_remove', disabled=row_count <= 1):
        st.session_state[count_key] = row_count - 1
        st.rerun()
    rc3.caption(f'Jumlah supplier pada jam ini: {row_count}')

    out: list[dict] = []
    for row_idx in range(row_count):
        seed = seed_rows[row_idx] if row_idx < len(seed_rows) and isinstance(seed_rows[row_idx], dict) else {}
        k_supplier = f'{prefix}_bb_supplier_{slot_token}_{row_idx}'
        k_ukuran = f'{prefix}_bb_ukuran_{slot_token}_{row_idx}'
        k_matang = f'{prefix}_bb_matang_{slot_token}_{row_idx}'
        k_selesai = f'{prefix}_bb_selesai_{slot_token}_{row_idx}'
        old_k_supplier = f'{prefix}_bb_supplier_{legacy_slot_idx}_{row_idx}' if legacy_slot_idx is not None else ''
        old_k_ukuran = f'{prefix}_bb_ukuran_{legacy_slot_idx}_{row_idx}' if legacy_slot_idx is not None else ''
        old_k_matang = f'{prefix}_bb_matang_{legacy_slot_idx}_{row_idx}' if legacy_slot_idx is not None else ''
        old_k_selesai = f'{prefix}_bb_selesai_{legacy_slot_idx}_{row_idx}' if legacy_slot_idx is not None else ''
        _migrate_session_key(k_matang, old_k_matang)
        _migrate_session_key(k_selesai, old_k_selesai)
        if k_supplier not in st.session_state:
            if old_k_supplier and old_k_supplier in st.session_state:
                st.session_state[k_supplier] = str(st.session_state.get(old_k_supplier, '')).strip()
            else:
                st.session_state[k_supplier] = str(seed.get('supplier', '')).strip()
        if k_ukuran not in st.session_state:
            if old_k_ukuran and old_k_ukuran in st.session_state:
                st.session_state[k_ukuran] = str(st.session_state.get(old_k_ukuran, '')).strip()
            else:
                st.session_state[k_ukuran] = str(seed.get('ukuran', '')).strip()

        st.markdown(f'BB #{row_idx + 1}')
        c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
        supplier = c1.text_input('Supplier', key=k_supplier, on_change=_request_persist)
        ukuran = c2.text_input('Ukuran', key=k_ukuran, on_change=_request_persist)
        matang = c3.radio('Tingkat matang', OX_OPTIONS, index=None, horizontal=True, key=k_matang)
        selesai = c4.radio('Selesai', OX_OPTIONS, index=None, horizontal=True, key=k_selesai)
        out.append(
            {
                'supplier': supplier.strip(),
                'ukuran': ukuran.strip(),
                'tingkat_matang': str(matang or '').strip().upper(),
                'selesai': str(selesai or '').strip().upper(),
            }
        )
    return out


def _render_hb_rows(prefix: str, slot_token: str, seed_rows: list[dict], legacy_slot_idx: int | None = None) -> list[dict]:
    seed_map: dict[str, dict] = {}
    for row in seed_rows or []:
        if isinstance(row, dict):
            hb_name = str(row.get('hb', '')).strip()
            if hb_name:
                seed_map[hb_name] = row

    out: list[dict] = []
    h0, h1, h2, h3 = st.columns([1.1, 1, 2.2, 1])
    h0.markdown('**HB**')
    h1.markdown('**Dipakai**')
    h2.markdown('**Alasan / status**')
    h3.markdown('**Gas**')
    for hb_name in HB_UNITS:
        key_slug = hb_name.lower().replace(' ', '')
        seed = seed_map.get(hb_name, {})
        k_d = f'{prefix}_hb_dipakai_{slot_token}_{key_slug}'
        k_a = f'{prefix}_hb_alasan_{slot_token}_{key_slug}'
        k_g = f'{prefix}_hb_gas_{slot_token}_{key_slug}'
        old_k_d = f'{prefix}_hb_dipakai_{legacy_slot_idx}_{key_slug}' if legacy_slot_idx is not None else ''
        old_k_a = f'{prefix}_hb_alasan_{legacy_slot_idx}_{key_slug}' if legacy_slot_idx is not None else ''
        old_k_g = f'{prefix}_hb_gas_{legacy_slot_idx}_{key_slug}' if legacy_slot_idx is not None else ''
        if old_k_d and k_d not in st.session_state and old_k_d in st.session_state:
            st.session_state[k_d] = st.session_state.get(old_k_d)
        if k_a not in st.session_state:
            if old_k_a and old_k_a in st.session_state:
                st.session_state[k_a] = str(st.session_state.get(old_k_a, '')).strip()
            else:
                st.session_state[k_a] = str(seed.get('alasan', '')).strip()
        if old_k_g and k_g not in st.session_state and old_k_g in st.session_state:
            st.session_state[k_g] = st.session_state.get(old_k_g)

        c0, c1, c2, c3 = st.columns([1.1, 1, 2.2, 1])
        c0.markdown(hb_name)
        dipakai = c1.radio('Dipakai', OX_OPTIONS, index=None, horizontal=True, key=k_d, label_visibility='collapsed')
        alasan_raw = c2.text_input('Alasan / status', key=k_a, label_visibility='collapsed', on_change=_request_persist)
        alasan = ALASAN_CODE_MAP.get(str(alasan_raw).strip().lower(), str(alasan_raw).strip())
        gas = c3.radio('Gas', OX_OPTIONS, index=None, horizontal=True, key=k_g, label_visibility='collapsed')
        out.append(
            {
                'hb': hb_name,
                'dipakai': str(dipakai or '').strip().upper(),
                'alasan': alasan.strip(),
                'gas': str(gas or '').strip().upper(),
            }
        )
    return out


def _scope_key(work_date: str, team_id: str) -> str:
    return f'{work_date}::{team_id}'


def _coerce_scope_record(existing: object) -> dict:
    if isinstance(existing, dict) and 'data' in existing:
        data = existing.get('data', {})
        history = existing.get('lock_history', [])
        try:
            version = int(existing.get('version', 0))
        except Exception:
            version = 0
        return {
            'data': data if isinstance(data, dict) else {},
            'version': version,
            'lock': existing.get('lock'),
            'lock_history': history if isinstance(history, list) else [],
        }
    if isinstance(existing, dict):
        # Legacy storage format: raw payload dict without wrapper.
        return {'data': existing, 'version': 0, 'lock': None, 'lock_history': []}
    return {'data': {}, 'version': 0, 'lock': None, 'lock_history': []}


def _write_state_atomically(raw: dict) -> None:
    tmp_path = PERSIST_PATH.with_suffix('.tmp')
    tmp_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp_path, PERSIST_PATH)


def load_persisted_state() -> dict:
    with STATE_IO_LOCK:
        if not PERSIST_PATH.exists():
            return {}
        try:
            return json.loads(PERSIST_PATH.read_text(encoding='utf-8'))
        except Exception:
            return {}


def get_scope_record(work_date: str, team_id: str) -> dict:
    raw = load_persisted_state()
    key = _scope_key(work_date, team_id)
    scoped = raw.get('scopes', {}).get(key, {})
    return _coerce_scope_record(scoped)


def load_scoped_state(work_date: str, team_id: str) -> dict:
    return get_scope_record(work_date, team_id).get('data', {})


def save_scoped_state(work_date: str, team_id: str, payload: dict, expected_version: int | None = None) -> tuple[bool, int]:
    with STATE_IO_LOCK:
        raw = load_persisted_state()
        key = _scope_key(work_date, team_id)
        scopes = raw.get('scopes')
        if not isinstance(scopes, dict):
            scopes = {}

        rec = _coerce_scope_record(scopes.get(key, {}))
        current_version = int(rec.get('version', 0))
        current_lock = rec.get('lock')
        lock_history = rec.get('lock_history', [])

        if expected_version is not None and current_version != expected_version:
            return False, current_version

        scopes[key] = {
            'data': payload,
            'version': current_version + 1,
            'lock': current_lock,
            'lock_history': lock_history,
        }
        raw['scopes'] = scopes
        _write_state_atomically(raw)
        return True, current_version + 1


def _lock_is_active(lock: dict | None) -> bool:
    if not isinstance(lock, dict):
        return False
    hb = lock.get('heartbeat_iso')
    if not hb:
        return False
    try:
        heartbeat = datetime.fromisoformat(hb)
    except Exception:
        return False
    return (now_local() - heartbeat).total_seconds() <= LOCK_TTL_SECONDS


def _lock_remaining_seconds(lock: dict | None) -> int:
    if not isinstance(lock, dict):
        return 0
    hb = lock.get('heartbeat_iso')
    if not hb:
        return 0
    try:
        heartbeat = datetime.fromisoformat(hb)
    except Exception:
        return 0
    remain = LOCK_TTL_SECONDS - int((now_local() - heartbeat).total_seconds())
    return max(0, remain)


def get_scope_lock(work_date: str, team_id: str) -> dict | None:
    return get_scope_record(work_date, team_id).get('lock')


def acquire_scope_lock(work_date: str, team_id: str, operator: str, force: bool = False) -> tuple[bool, str]:
    with STATE_IO_LOCK:
        raw = load_persisted_state()
        key = _scope_key(work_date, team_id)
        scopes = raw.get('scopes')
        if not isinstance(scopes, dict):
            scopes = {}
        rec = _coerce_scope_record(scopes.get(key, {}))
        lock = rec.get('lock')
        active = _lock_is_active(lock)
        token = st.session_state.get('lock_token')
        if not token:
            token = str(uuid.uuid4())
            st.session_state['lock_token'] = token

        if active and not force:
            same_owner = lock.get('token') == token
            if not same_owner:
                return False, f"Saat ini dipakai oleh {lock.get('owner', 'tidak diketahui')}"

        now = now_iso()
        new_lock = {
            'owner': operator,
            'token': token,
            'acquired_iso': lock.get('acquired_iso', now) if isinstance(lock, dict) else now,
            'heartbeat_iso': now,
        }
        lock_history = rec.get('lock_history', [])
        if force:
            lock_history.append({'action': 'takeover', 'at': now, 'by': operator, 'from': (lock or {}).get('owner', '-')})
        elif not active:
            lock_history.append({'action': 'acquire', 'at': now, 'by': operator})

        scopes[key] = {
            'data': rec.get('data', {}),
            'version': int(rec.get('version', 0)),
            'lock': new_lock,
            'lock_history': lock_history,
        }
        raw['scopes'] = scopes
        _write_state_atomically(raw)
        return True, 'Kunci berhasil diambil'


def refresh_scope_lock(work_date: str, team_id: str) -> None:
    operator = st.session_state.get('operator_name', '').strip()
    if not operator:
        return
    lock = get_scope_lock(work_date, team_id)
    token = st.session_state.get('lock_token')
    if not isinstance(lock, dict):
        return
    if lock.get('token') != token:
        return
    acquire_scope_lock(work_date, team_id, operator, force=False)


def _session_owns_scope_lock(work_date: str, team_id: str) -> bool:
    lock = get_scope_lock(work_date, team_id)
    token = st.session_state.get('lock_token')
    if not isinstance(lock, dict) or not token:
        return False
    return _lock_is_active(lock) and lock.get('token') == token


def _parse_time_hhmm(text: str) -> time:
    try:
        h, m = str(text).split(':', 1)
        return time(int(h), int(m))
    except Exception:
        return round_to_half_hour(now_local())


def init_state() -> None:
    if 'team_id' not in st.session_state:
        st.session_state['team_id'] = 'KUPAS-1'
    if 'work_date' not in st.session_state:
        st.session_state['work_date'] = now_local().date().isoformat()
    if '_loaded_scope' not in st.session_state:
        st.session_state['_loaded_scope'] = ''
    if 'authenticated_scope' not in st.session_state:
        st.session_state['authenticated_scope'] = ''
    if 'scope_version' not in st.session_state:
        st.session_state['scope_version'] = None
    if 'operator_name' not in st.session_state:
        st.session_state['operator_name'] = ''
    if 'submission_id' not in st.session_state:
        st.session_state['submission_id'] = None
    if 'telegram_root_message_id' not in st.session_state:
        st.session_state['telegram_root_message_id'] = None
    if '_submitting' not in st.session_state:
        st.session_state['_submitting'] = False
    if '_scope_conflict' not in st.session_state:
        st.session_state['_scope_conflict'] = False
    if 'shift_select' not in st.session_state:
        st.session_state['shift_select'] = 'Shift 1'
    if 'start_time_3' not in st.session_state:
        st.session_state['start_time_3'] = round_to_half_hour(now_local()).strftime('%H:%M')
    if 'start_time_4' not in st.session_state:
        st.session_state['start_time_4'] = round_to_half_hour(now_local()).strftime('%H:%M')
    if 'start_time_5' not in st.session_state:
        st.session_state['start_time_5'] = round_to_half_hour(now_local()).strftime('%H:%M')
    if 'start_time_6' not in st.session_state:
        st.session_state['start_time_6'] = round_to_half_hour(now_local()).strftime('%H:%M')
    if 'start_time_7' not in st.session_state:
        st.session_state['start_time_7'] = round_to_half_hour(now_local()).strftime('%H:%M')
    if 'slots_3' not in st.session_state:
        st.session_state['slots_3'] = []
    if 'slots_4' not in st.session_state:
        st.session_state['slots_4'] = []
    if 'slots_5' not in st.session_state:
        st.session_state['slots_5'] = []
    if 'slots_6' not in st.session_state:
        st.session_state['slots_6'] = []
    if 'slots_7' not in st.session_state:
        st.session_state['slots_7'] = []
    if 'reporter_input' not in st.session_state:
        st.session_state['reporter_input'] = ''
    if 'defect_flag' not in st.session_state:
        st.session_state['defect_flag'] = False
    if 'defect_types' not in st.session_state:
        st.session_state['defect_types'] = ''
    if 'defect_percent' not in st.session_state:
        st.session_state['defect_percent'] = ''
    if 'defect_photo_count' not in st.session_state:
        st.session_state['defect_photo_count'] = 0
    if 'notes' not in st.session_state:
        st.session_state['notes'] = ''
    if 'keterangan' not in st.session_state:
        st.session_state['keterangan'] = ''
    if '_slots_normalized_once' not in st.session_state:
        st.session_state['_slots_normalized_once'] = False
    if '_last_persist_sig' not in st.session_state:
        st.session_state['_last_persist_sig'] = ''


def build_persist_payload() -> dict:
    static_keys = [
        'operator_name',
        'reporter_input',
        'shift_select',
        'slots_3',
        'start_time_3',
        'slots_4',
        'start_time_4',
        'slots_5',
        'start_time_5',
        'slots_6',
        'start_time_6',
        'slots_7',
        'start_time_7',
        'defect_flag',
        'defect_types',
        'defect_percent',
        'defect_photo_count',
        'notes',
        'keterangan',
        'submission_id',
        'telegram_root_message_id',
    ]
    out = {k: st.session_state.get(k) for k in static_keys if k in st.session_state}
    for key, val in st.session_state.items():
        if _is_persistable_dynamic_key(key):
            out[key] = val

    for key in ('start_time_3', 'start_time_4', 'start_time_5', 'start_time_6', 'start_time_7'):
        val = out.get(key)
        if isinstance(val, time):
            out[key] = val.strftime('%H:%M')
    return out


def persist_state_to_disk() -> None:
    team_id = str(st.session_state.get('team_id', '')).strip()
    work_date = str(st.session_state.get('work_date', '')).strip()
    if not team_id or not work_date:
        return
    expected = st.session_state.get('scope_version')
    payload = build_persist_payload()
    payload_sig = _persist_signature(payload)
    ok, new_version = save_scoped_state(work_date, team_id, payload, expected_version=expected)
    if ok:
        st.session_state['scope_version'] = new_version
        st.session_state['_scope_conflict'] = False
        st.session_state['_last_persist_sig'] = payload_sig
    else:
        # If version drift happens while this session still owns the active lock,
        # do a last-write-wins retry so user edits are not silently dropped.
        if _session_owns_scope_lock(work_date, team_id):
            retry_ok, retry_version = save_scoped_state(work_date, team_id, payload, expected_version=None)
            if retry_ok:
                st.session_state['scope_version'] = retry_version
                st.session_state['_scope_conflict'] = False
                st.session_state['_last_persist_sig'] = payload_sig
                return
        st.session_state['_scope_conflict'] = True


def sync_scope_if_needed(work_date: str, team_id: str) -> None:
    scope = f'{work_date}::{team_id}'
    if st.session_state.get('_loaded_scope') == scope:
        return

    for key in list(st.session_state.keys()):
        if key.startswith(STATE_PREFIX_KEYS):
            del st.session_state[key]

    scoped = load_scoped_state(work_date, team_id)
    rec = get_scope_record(work_date, team_id)
    for key, val in scoped.items():
        if key in {'start_time_3', 'start_time_4', 'start_time_5', 'start_time_6', 'start_time_7'}:
            if isinstance(val, time):
                st.session_state[key] = val.strftime('%H:%M')
            else:
                st.session_state[key] = _parse_hhmm_text(str(val)) or round_to_half_hour(now_local()).strftime('%H:%M')
        elif key.startswith(STATE_PREFIX_KEYS):
            if _is_persistable_dynamic_key(key):
                st.session_state[key] = val
        else:
            st.session_state[key] = val

    if not scoped:
        st.session_state['submission_id'] = None
        st.session_state['telegram_root_message_id'] = None

    _maybe_restore_legacy_slots('slots_3', 'start_time_3', 3)
    _maybe_restore_legacy_slots('slots_4', 'start_time_4', 4)
    _maybe_restore_legacy_slots('slots_5', 'start_time_5', 5)
    _maybe_restore_legacy_slots('slots_6', 'start_time_6', 6)
    _maybe_restore_legacy_slots('slots_7', 'start_time_7', 7)

    st.session_state['work_date'] = work_date
    st.session_state['team_id'] = team_id
    st.session_state['scope_version'] = int(rec.get('version', 0))
    st.session_state['_loaded_scope'] = scope
    st.session_state['_last_persist_sig'] = _persist_signature(build_persist_payload())
    st.rerun()


def main() -> None:
    st.set_page_config(page_title='Laporan Kondisi BB', layout='wide')
    init_state()
    st.markdown(
        """
        <style>
        div[data-testid="stTextInput"] input, div[data-testid="stTextArea"] textarea, div[data-testid="stNumberInput"] input {
          font-size: 1rem;
          min-height: 2.5rem;
        }
        div[data-baseweb="select"] > div {
          min-height: 2.5rem;
        }
        button[kind] {
          min-height: 2.6rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title('Laporan Kondisi BB')
    st.caption('Durasi lapor: awal masuk + setiap masukkan BB + setiap 30 menit')
    if TEAM_PASSWORDS_ERROR:
        st.warning(TEAM_PASSWORDS_ERROR)
    if not TEAM_PASSWORDS:
        st.error('Aplikasi dikunci: TEAM_PASSWORDS wajib diset sebelum dipakai.')
        st.stop()

    team_choices = list(TEAM_PASSWORDS.keys())
    saved_team = st.session_state.get('team_id', 'KUPAS-1')
    team_index = team_choices.index(saved_team) if saved_team in team_choices else 0
    team_id = st.selectbox('Tim laporan', team_choices, index=team_index, format_func=lambda x: TEAM_LABELS.get(x, x))
    date_seed = date.fromisoformat(st.session_state.get('work_date', now_local().date().isoformat()))
    work_date = st.date_input('Tanggal kerja', value=date_seed).isoformat()
    operator_name = st.text_input('Pelapor', value=st.session_state.get('operator_name', ''), key='operator_name_input')
    team_pin = st.text_input('PIN Tim', type='password', key='team_pin_input')

    scope = f'{work_date}::{team_id}'
    lock_info = get_scope_lock(work_date, team_id)
    lock_active = _lock_is_active(lock_info)
    if lock_active:
        remain = _lock_remaining_seconds(lock_info)
        st.caption(
            f"Kunci aktif: {lock_info.get('owner','-')} | heartbeat {lock_info.get('heartbeat_iso','')} | TTL sisa {remain} dtk"
        )
    lock_c1, lock_c2 = st.columns(2)
    open_clicked = lock_c1.button('Buka Tim')
    takeover_clicked = lock_c2.button('Ambil Alih Tim', disabled=not lock_active)

    if open_clicked:
        if not operator_name.strip():
            st.error('Nama operator wajib diisi.')
        elif secrets.compare_digest(team_pin, TEAM_PASSWORDS.get(team_id, '')):
            ok_lock, msg_lock = acquire_scope_lock(work_date, team_id, operator_name, force=False)
            if ok_lock:
                st.session_state['authenticated_scope'] = scope
                st.session_state['team_id'] = team_id
                st.session_state['work_date'] = work_date
                st.session_state['operator_name'] = operator_name
                st.success(f'{team_id} berhasil dibuka')
            else:
                st.error(f'{msg_lock}. Jika perlu gunakan Ambil Alih Tim')
        else:
            st.error('PIN Tim tidak valid.')

    if takeover_clicked:
        if not operator_name.strip():
            st.error('Nama operator wajib diisi.')
        elif secrets.compare_digest(team_pin, TEAM_PASSWORDS.get(team_id, '')):
            ok_take, msg_take = acquire_scope_lock(work_date, team_id, operator_name, force=True)
            if ok_take:
                st.session_state['authenticated_scope'] = scope
                st.session_state['team_id'] = team_id
                st.session_state['work_date'] = work_date
                st.session_state['operator_name'] = operator_name
                st.success(f'Ambil alih berhasil: {team_id}')
            else:
                st.error(msg_take)
        else:
            st.error('PIN Tim tidak valid untuk ambil alih.')

    if st.session_state.get('authenticated_scope') != scope:
        st.warning(f"Untuk membuka data {team_id}, masukkan PIN lalu tekan 'Buka Tim'.")
        st.stop()

    sync_scope_if_needed(work_date, team_id)
    # Do not auto-truncate persisted slots; keep user-entered history intact.
    refresh_scope_lock(work_date, team_id)
    if st.session_state.get('_scope_conflict'):
        st.warning('Data lokal berbeda versi terbaru. Muat ulang scope agar sinkron.')
        if st.button('Muat Ulang Scope'):
            st.session_state['_loaded_scope'] = ''
            st.rerun()

    pending = load_pending_submission()
    if pending:
        st.warning("Ada pending submission. Setelah koneksi normal, klik 'Kirim ke Telegram' lagi.")
        st.caption(
            f"Pending key={pending.get('idempotency_key','-')} | "
            f"attempt={pending.get('attempt_count', 0)} | "
            f"telegram_sent={pending.get('telegram_sent', False)}"
        )

    left, right = st.columns([1, 1])
    with left:
        if not st.session_state.get('reporter_input'):
            st.session_state['reporter_input'] = st.session_state.get('operator_name', '')
        reporter = st.text_input('Nama pelapor laporan', key='reporter_input', on_change=_request_persist)
    with right:
        shift = st.selectbox('Shift', ['Shift 1', 'Shift 2', 'Shift 3'], key='shift_select')
    st.caption('Autosave aktif. Tidak perlu klik simpan per slot.')

    if not st.session_state.get('submission_id'):
        st.session_state['submission_id'] = str(uuid.uuid4())

    st.subheader('3. List BB yang akan dikerjakan oleh shift ku')
    start_3 = _render_start_time_input('Jam mulai 3 (HH:MM)', 'start_time_3')
    slots_3 = _ensure_group_slots('slots_3', start_3)
    c3_1, c3_2 = st.columns([4, 1])
    custom_3 = c3_1.text_input('Tambah jam khusus 3 (HH:MM)', key='custom_slot_3', on_change=_request_persist)
    c3_2.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
    if c3_2.button('Tambah jam', key='add_custom_3'):
        parsed_3 = _parse_hhmm_text(custom_3)
        if not parsed_3:
            st.warning('Format jam khusus 3 harus HH:MM.')
        elif parsed_3 in st.session_state['slots_3']:
            st.info(f'Jam {parsed_3} sudah ada di Section 3.')
        else:
            st.session_state['slots_3'].append(parsed_3)
            st.session_state['slots_3'] = sorted(st.session_state['slots_3'], key=_slot_sort_key)
            st.rerun()
    group_3: list[dict] = []
    for idx, slot_time in enumerate(slots_3):
        slot_token = _slot_token(slot_time)
        note_key = f'g3_note_{slot_token}'
        read_key = f'g3_read_{slot_token}'
        know_key = f'g3_know_{slot_token}'
        _migrate_session_key(note_key, f'g3_note_{idx}')
        _migrate_session_key(read_key, f'g3_read_{idx}')
        _migrate_session_key(know_key, f'g3_know_{idx}')
        close_once = bool(st.session_state.pop(f'close_once_3_{slot_token}', False) or st.session_state.pop(f'close_once_3_{idx}', False))
        exp_label = f'Jam {slot_time} [saved]' if close_once else f'Jam {slot_time}'
        with st.expander(exp_label, expanded=False):
            slot_note_value = st.radio(
                f'[{slot_time}] Status slot',
                SLOT_STATUS_OPTIONS,
                index=None,
                key=note_key,
                horizontal=True,
            )
            c1, c2 = st.columns(2)
            read_val = c1.radio(f'[{slot_time}] Sudah baca list BB di steam', OX_OPTIONS, index=None, horizontal=True, key=read_key)
            know_val = c2.radio(f'[{slot_time}] Sudah tahu BB & ukuran', OX_OPTIONS, index=None, horizontal=True, key=know_key)
            a1, a2 = st.columns(2)
            if a1.button('Next slot tambah', key=f'next_3_{slot_token}'):
                slots = list(st.session_state['slots_3'])
                slots.insert(idx + 1, _next_available_after(slot_time, slots))
                st.session_state['slots_3'] = sorted(slots, key=_slot_sort_key)
                st.rerun()
            if a2.button('Hapus slot ini', key=f'ask_rm_3_{slot_token}', disabled=len(st.session_state['slots_3']) <= 1):
                st.session_state[f'confirm_rm_3_{slot_token}'] = True
            if st.session_state.get(f'confirm_rm_3_{slot_token}'):
                q1, q2 = st.columns(2)
                if q1.button('Ya, hapus', key=f'yes_rm_3_{slot_token}'):
                    st.session_state['slots_3'] = [x for i, x in enumerate(st.session_state['slots_3']) if i != idx]
                    st.session_state[f'confirm_rm_3_{slot_token}'] = False
                    st.rerun()
                if q2.button('Batal', key=f'no_rm_3_{slot_token}'):
                    st.session_state[f'confirm_rm_3_{slot_token}'] = False
                    st.rerun()
            group_3.append(
                {
                    'slot_time': slot_time,
                    'slot_note': str(slot_note_value or '').strip(),
                    'list_read_steam': str(read_val or '').strip().upper(),
                    'list_know_bb_size': str(know_val or '').strip().upper(),
                }
            )

    st.subheader('4. BB masuk')
    st.caption('supplier / ukuran / tingkat matang / selesai')
    start_4 = _render_start_time_input('Jam mulai 4 (HH:MM)', 'start_time_4')
    slots_4 = _ensure_group_slots('slots_4', start_4)
    c4_1, c4_2 = st.columns([4, 1])
    custom_4 = c4_1.text_input('Tambah jam khusus 4 (HH:MM)', key='custom_slot_4', on_change=_request_persist)
    c4_2.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
    if c4_2.button('Tambah jam', key='add_custom_4'):
        parsed_4 = _parse_hhmm_text(custom_4)
        if not parsed_4:
            st.warning('Format jam khusus 4 harus HH:MM.')
        elif parsed_4 in st.session_state['slots_4']:
            st.info(f'Jam {parsed_4} sudah ada di Section 4.')
        else:
            st.session_state['slots_4'].append(parsed_4)
            st.session_state['slots_4'] = sorted(st.session_state['slots_4'], key=_slot_sort_key)
            st.rerun()
    group_4: list[dict] = []
    for idx, slot_time in enumerate(slots_4):
        slot_token = _slot_token(slot_time)
        note_key = f'g4_note_{slot_token}'
        _migrate_session_key(note_key, f'g4_note_{idx}')
        close_once = bool(st.session_state.pop(f'close_once_4_{slot_token}', False) or st.session_state.pop(f'close_once_4_{idx}', False))
        exp_label = f'Jam {slot_time} [saved]' if close_once else f'Jam {slot_time}'
        with st.expander(exp_label, expanded=False):
            slot_note_value = st.radio(
                f'[{slot_time}] Status slot',
                SLOT_STATUS_OPTIONS,
                index=None,
                key=note_key,
                horizontal=True,
            )
            st.caption('Contoh: Darmi 1/S8 / O / O')
            bb_rows = _render_bb_rows('g4', slot_token, [], legacy_slot_idx=idx)
            a1, a2 = st.columns(2)
            if a1.button('Next slot tambah', key=f'next_4_{slot_token}'):
                slots = list(st.session_state['slots_4'])
                slots.insert(idx + 1, _next_available_after(slot_time, slots))
                st.session_state['slots_4'] = sorted(slots, key=_slot_sort_key)
                st.rerun()
            if a2.button('Hapus slot ini', key=f'ask_rm_4_{slot_token}', disabled=len(st.session_state['slots_4']) <= 1):
                st.session_state[f'confirm_rm_4_{slot_token}'] = True
            if st.session_state.get(f'confirm_rm_4_{slot_token}'):
                q1, q2 = st.columns(2)
                if q1.button('Ya, hapus', key=f'yes_rm_4_{slot_token}'):
                    st.session_state['slots_4'] = [x for i, x in enumerate(st.session_state['slots_4']) if i != idx]
                    st.session_state[f'confirm_rm_4_{slot_token}'] = False
                    st.rerun()
                if q2.button('Batal', key=f'no_rm_4_{slot_token}'):
                    st.session_state[f'confirm_rm_4_{slot_token}'] = False
                    st.rerun()
            group_4.append({'slot_time': slot_time, 'slot_note': str(slot_note_value or '').strip(), 'bb_masuk': bb_rows})

    st.subheader('5. Status keranjang ubi sudah steam')
    start_5 = _render_start_time_input('Jam mulai 5 (HH:MM)', 'start_time_5')
    slots_5 = _ensure_group_slots('slots_5', start_5)
    c5_1, c5_2 = st.columns([4, 1])
    custom_5 = c5_1.text_input('Tambah jam khusus 5 (HH:MM)', key='custom_slot_5', on_change=_request_persist)
    c5_2.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
    if c5_2.button('Tambah jam', key='add_custom_5'):
        parsed_5 = _parse_hhmm_text(custom_5)
        if not parsed_5:
            st.warning('Format jam khusus 5 harus HH:MM.')
        elif parsed_5 in st.session_state['slots_5']:
            st.info(f'Jam {parsed_5} sudah ada di Section 5.')
        else:
            st.session_state['slots_5'].append(parsed_5)
            st.session_state['slots_5'] = sorted(st.session_state['slots_5'], key=_slot_sort_key)
            st.rerun()
    group_5: list[dict] = []
    for idx, slot_time in enumerate(slots_5):
        slot_token = _slot_token(slot_time)
        note_key = f'g5_note_{slot_token}'
        empty_key = f'g5_empty_{slot_token}'
        hand_key = f'g5_hand_{slot_token}'
        break_key = f'g5_break_{slot_token}'
        _migrate_session_key(note_key, f'g5_note_{idx}')
        _migrate_session_key(empty_key, f'g5_empty_{idx}')
        _migrate_session_key(hand_key, f'g5_hand_{idx}')
        _migrate_session_key(break_key, f'g5_break_{idx}')
        close_once = bool(st.session_state.pop(f'close_once_5_{slot_token}', False) or st.session_state.pop(f'close_once_5_{idx}', False))
        exp_label = f'Jam {slot_time} [saved]' if close_once else f'Jam {slot_time}'
        with st.expander(exp_label, expanded=False):
            slot_note_value = st.radio(
                f'[{slot_time}] Status slot',
                SLOT_STATUS_OPTIONS,
                index=None,
                key=note_key,
                horizontal=True,
            )
            s1, s2, s3 = st.columns(3)
            v1 = s1.radio(f'[{slot_time}] Keranjang stainless kosong', OX_OPTIONS, index=None, horizontal=True, key=empty_key)
            v2 = s2.radio(f'[{slot_time}] Handover keranjang kosong ke steam', OX_OPTIONS, index=None, horizontal=True, key=hand_key)
            v3 = s3.radio(f'[{slot_time}] Sebelum istirahat kosongkan semua', OX_OPTIONS, index=None, horizontal=True, key=break_key)
            a1, a2 = st.columns(2)
            if a1.button('Next slot tambah', key=f'next_5_{slot_token}'):
                slots = list(st.session_state['slots_5'])
                slots.insert(idx + 1, _next_available_after(slot_time, slots))
                st.session_state['slots_5'] = sorted(slots, key=_slot_sort_key)
                st.rerun()
            if a2.button('Hapus slot ini', key=f'ask_rm_5_{slot_token}', disabled=len(st.session_state['slots_5']) <= 1):
                st.session_state[f'confirm_rm_5_{slot_token}'] = True
            if st.session_state.get(f'confirm_rm_5_{slot_token}'):
                q1, q2 = st.columns(2)
                if q1.button('Ya, hapus', key=f'yes_rm_5_{slot_token}'):
                    st.session_state['slots_5'] = [x for i, x in enumerate(st.session_state['slots_5']) if i != idx]
                    st.session_state[f'confirm_rm_5_{slot_token}'] = False
                    st.rerun()
                if q2.button('Batal', key=f'no_rm_5_{slot_token}'):
                    st.session_state[f'confirm_rm_5_{slot_token}'] = False
                    st.rerun()
            group_5.append(
                {
                    'slot_time': slot_time,
                    'slot_note': str(slot_note_value or '').strip(),
                    'steam_basket_empty': str(v1 or '').strip().upper(),
                    'steam_handover': str(v2 or '').strip().upper(),
                    'steam_prebreak_empty': str(v3 or '').strip().upper(),
                }
            )

    st.subheader('6. Status keranjang belum steam')
    start_6 = _render_start_time_input('Jam mulai 6 (HH:MM)', 'start_time_6')
    slots_6 = _ensure_group_slots('slots_6', start_6)
    c6_1, c6_2 = st.columns([4, 1])
    custom_6 = c6_1.text_input('Tambah jam khusus 6 (HH:MM)', key='custom_slot_6', on_change=_request_persist)
    c6_2.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
    if c6_2.button('Tambah jam', key='add_custom_6'):
        parsed_6 = _parse_hhmm_text(custom_6)
        if not parsed_6:
            st.warning('Format jam khusus 6 harus HH:MM.')
        elif parsed_6 in st.session_state['slots_6']:
            st.info(f'Jam {parsed_6} sudah ada di Section 6.')
        else:
            st.session_state['slots_6'].append(parsed_6)
            st.session_state['slots_6'] = sorted(st.session_state['slots_6'], key=_slot_sort_key)
            st.rerun()
    group_6: list[dict] = []
    for idx, slot_time in enumerate(slots_6):
        slot_token = _slot_token(slot_time)
        note_key = f'g6_note_{slot_token}'
        unsteam_key = f'g6_unsteam_{slot_token}'
        reason_key = f'g6_unsteam_reason_{slot_token}'
        _migrate_session_key(note_key, f'g6_note_{idx}')
        _migrate_session_key(unsteam_key, f'g6_unsteam_{idx}')
        _migrate_session_key(reason_key, f'g6_unsteam_reason_{idx}')
        close_once = bool(st.session_state.pop(f'close_once_6_{slot_token}', False) or st.session_state.pop(f'close_once_6_{idx}', False))
        exp_label = f'Jam {slot_time} [saved]' if close_once else f'Jam {slot_time}'
        with st.expander(exp_label, expanded=False):
            slot_note_value = st.radio(
                f'[{slot_time}] Status slot',
                SLOT_STATUS_OPTIONS,
                index=None,
                key=note_key,
                horizontal=True,
            )
            u1, u2 = st.columns([1, 2])
            u_status = u1.radio(f'[{slot_time}] Status', OX_OPTIONS, index=None, horizontal=True, key=unsteam_key)
            u_reason = u2.text_input(f'[{slot_time}] Alasan (jika X)', key=reason_key, on_change=_request_persist)
            a1, a2 = st.columns(2)
            if a1.button('Next slot tambah', key=f'next_6_{slot_token}'):
                slots = list(st.session_state['slots_6'])
                slots.insert(idx + 1, _next_available_after(slot_time, slots))
                st.session_state['slots_6'] = sorted(slots, key=_slot_sort_key)
                st.rerun()
            if a2.button('Hapus slot ini', key=f'ask_rm_6_{slot_token}', disabled=len(st.session_state['slots_6']) <= 1):
                st.session_state[f'confirm_rm_6_{slot_token}'] = True
            if st.session_state.get(f'confirm_rm_6_{slot_token}'):
                q1, q2 = st.columns(2)
                if q1.button('Ya, hapus', key=f'yes_rm_6_{slot_token}'):
                    st.session_state['slots_6'] = [x for i, x in enumerate(st.session_state['slots_6']) if i != idx]
                    st.session_state[f'confirm_rm_6_{slot_token}'] = False
                    st.rerun()
                if q2.button('Batal', key=f'no_rm_6_{slot_token}'):
                    st.session_state[f'confirm_rm_6_{slot_token}'] = False
                    st.rerun()
            group_6.append(
                {
                    'slot_time': slot_time,
                    'slot_note': str(slot_note_value or '').strip(),
                    'unsteam_status': str(u_status or '').strip().upper(),
                    'unsteam_reason': u_reason.strip(),
                }
            )

    st.subheader('7. Status pemakaian HB (update per 30 menit)')
    start_7 = _render_start_time_input('Jam mulai 7 (HH:MM)', 'start_time_7')
    slots_7 = _ensure_group_slots('slots_7', start_7)
    c7_1, c7_2 = st.columns([4, 1])
    custom_7 = c7_1.text_input('Tambah jam khusus 7 (HH:MM)', key='custom_slot_7', on_change=_request_persist)
    c7_2.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
    if c7_2.button('Tambah jam', key='add_custom_7'):
        parsed_7 = _parse_hhmm_text(custom_7)
        if not parsed_7:
            st.warning('Format jam khusus 7 harus HH:MM.')
        elif parsed_7 in st.session_state['slots_7']:
            st.info(f'Jam {parsed_7} sudah ada di Section 7.')
        else:
            st.session_state['slots_7'].append(parsed_7)
            st.session_state['slots_7'] = sorted(st.session_state['slots_7'], key=_slot_sort_key)
            st.rerun()
    group_7: list[dict] = []
    for idx, slot_time in enumerate(slots_7):
        slot_token = _slot_token(slot_time)
        note_key = f'g7_note_{slot_token}'
        legacy_note_key = f'g7_note_{idx}'
        if note_key not in st.session_state and legacy_note_key in st.session_state:
            st.session_state[note_key] = st.session_state.get(legacy_note_key)
        close_once = bool(st.session_state.pop(f'close_once_7_{slot_token}', False) or st.session_state.pop(f'close_once_7_{idx}', False))
        exp_label = f'Jam {slot_time} [saved]' if close_once else f'Jam {slot_time}'
        with st.expander(exp_label, expanded=False):
            slot_note_value = st.radio(
                f'[{slot_time}] Status slot',
                SLOT_STATUS_OPTIONS,
                index=None,
                key=note_key,
                horizontal=True,
            )
            st.caption('Kode cepat alasan/status: 1=proses isi, 2=ubi cilembu, 3=ubi ungu, 4=jagung, 5=steril, 6=kosong, 7=tidak dipakai. Manual input tetap bisa.')
            hb_rows = _render_hb_rows('g7', slot_token, [], legacy_slot_idx=idx)
            a1, a2 = st.columns(2)
            if a1.button('Next slot tambah', key=f'next_7_{slot_token}'):
                slots = list(st.session_state['slots_7'])
                slots.insert(idx + 1, _next_available_after(slot_time, slots))
                st.session_state['slots_7'] = sorted(slots, key=_slot_sort_key)
                st.rerun()
            if a2.button('Hapus slot ini', key=f'ask_rm_7_{slot_token}', disabled=len(st.session_state['slots_7']) <= 1):
                st.session_state[f'confirm_rm_7_{slot_token}'] = True
            if st.session_state.get(f'confirm_rm_7_{slot_token}'):
                q1, q2 = st.columns(2)
                if q1.button('Ya, hapus', key=f'yes_rm_7_{slot_token}'):
                    st.session_state['slots_7'] = [x for i, x in enumerate(st.session_state['slots_7']) if i != idx]
                    st.session_state[f'confirm_rm_7_{slot_token}'] = False
                    st.rerun()
                if q2.button('Batal', key=f'no_rm_7_{slot_token}'):
                    st.session_state[f'confirm_rm_7_{slot_token}'] = False
                    st.rerun()
            group_7.append({'slot_time': slot_time, 'slot_note': str(slot_note_value or '').strip(), 'hb_rows': hb_rows})

    st.subheader('8. Catatan')
    st.caption('Opsional. Boleh kosong.')
    notes = st.text_area(
        'Memo catatan operator',
        key='notes',
        placeholder='Tulis catatan bebas. Foto dan detail tambahan dilaporkan langsung di Telegram.',
        height=180,
        on_change=_request_persist,
    )
    st.subheader('9. Keterangan')
    st.caption('Opsional. Boleh kosong.')
    keterangan = st.text_area(
        'Keterangan masalah dari list di atas',
        key='keterangan',
        placeholder='Contoh: Jam 14:30 BB ukuran S8 kurang matang 20%, sudah lapor foto di Telegram.',
        height=140,
        on_change=_request_persist,
    )

    payload = {
        'report_name': REPORT_NAME,
        'work_date': datetime.fromisoformat(work_date).strftime('%d/%b/%Y'),
        'team_id': team_id,
        'shift': shift,
        'reporter': reporter,
        'group_3': group_3,
        'group_4': group_4,
        'group_5': group_5,
        'group_6': group_6,
        'group_7': group_7,
        'defect_flag': False,
        'defect_types': '',
        'defect_percent': '',
        'defect_photo_count': 0,
        'notes': notes,
        'keterangan': keterangan,
    }

    try:
        errors = validate_payload(payload)
    except Exception as e:
        errors = [f'Validation error: {e}']
    if errors:
        st.warning(f'Perlu perbaikan sebelum kirim: {len(errors)} poin.')

    telegram_text = build_telegram_text(payload)
    telegram_parts = split_telegram_parts(telegram_text)
    submitted_at = now_local().isoformat()
    sheets_rows = build_sheets_rows(payload, st.session_state['submission_id'], submitted_at)

    root_msg = st.session_state.get('telegram_root_message_id')
    if root_msg:
        st.caption(f'Cara kirim: perbarui pesan yang sudah ada (ID pesan={root_msg})')
    else:
        st.caption('Cara kirim: kirim pesan baru ke Telegram')

    new_cycle_clicked = st.button('Mulai siklus laporan baru')
    send_clicked = st.button('Kirim ke Telegram', type='primary', disabled=bool(errors))
    save_draft_clicked = st.button('Simpan draf lokal')

    if new_cycle_clicked:
        st.session_state['submission_id'] = str(uuid.uuid4())
        st.session_state['telegram_root_message_id'] = None
        clear_pending_submission()
        st.session_state['_pending_persist'] = True
        st.success('Siklus laporan baru dimulai.')

    if save_draft_clicked:
        st.session_state['_pending_persist'] = True
        st.success('Draf lokal tersimpan.')

    st.markdown('### Pratinjau (pesan Telegram)')
    for i, part in enumerate(telegram_parts, start=1):
        if len(telegram_parts) > 1:
            st.caption(f'Part {i}')
        st.code(part, language='markdown')

    if send_clicked:
        if st.session_state.get('_submitting'):
            st.warning('Sedang proses kirim. Tunggu selesai.')
            st.stop()
        st.session_state['_submitting'] = True
        try:
            current_rec = get_scope_record(work_date, team_id)
            live_lock = current_rec.get('lock')
            if not isinstance(live_lock, dict) or live_lock.get('token') != st.session_state.get('lock_token'):
                st.session_state['_submitting'] = False
                st.error('Kunci tim berubah. Buka Tim/Ambil Alih dulu sebelum kirim.')
                st.stop()

            live_version = int(current_rec.get('version', 0))
            session_ver = st.session_state.get('scope_version')
            if session_ver is not None and live_version != int(session_ver):
                st.session_state['scope_version'] = live_version

            record = {
                'idempotency_key': st.session_state['submission_id'],
                'submitted_at': submitted_at,
                'payload': clone_payload(payload),
                'telegram_parts': telegram_parts,
                'telegram_root_message_id': st.session_state.get('telegram_root_message_id'),
                'telegram_parts_sent': 0,
                'sheets_rows': sheets_rows,
                'telegram_sent': False,
                'attempt_count': 0,
                'last_error': '',
                'last_error_detail': '',
            }
            result = process_submission(record)
            if result.get('ok'):
                clear_pending_submission()
                st.session_state['telegram_root_message_id'] = result.get('telegram_root_message_id') or record.get('telegram_root_message_id')
                st.session_state['submission_id'] = str(uuid.uuid4())
                st.session_state['_pending_persist'] = True
                st.success('Submit selesai: Telegram + backup diproses.')
                if result.get('warning'):
                    st.warning(result['warning'])
            else:
                save_pending_submission(record)
                st.error(result.get('message', 'Submit gagal.'))
        finally:
            st.session_state['_submitting'] = False

    st.divider()
    st.caption('Env Telegram (required): TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID')
    st.caption('Env Sheets (optional/non-blocking): SHEETS_WEBHOOK_URL')
    st.caption(f'Timezone system: {TIMEZONE}')

    pending_persist = bool(st.session_state.pop('_pending_persist', False))
    if pending_persist:
        persist_state_to_disk()
    else:
        # Autosave when form data changed, so partial updates are not lost between 30-minute entries.
        if _session_owns_scope_lock(work_date, team_id) and not st.session_state.get('_scope_conflict'):
            current_sig = _persist_signature(build_persist_payload())
            if current_sig and current_sig != st.session_state.get('_last_persist_sig', ''):
                persist_state_to_disk()


if __name__ == '__main__':
    main()


