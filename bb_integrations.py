from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from urllib import error as urlerror, parse, request

PENDING_PATH = Path('.bb_pending_submission.json')


def _post_json(url: str, payload: dict, idempotency_key: str, timeout: int = 15) -> tuple[bool, int, str]:
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = request.Request(url=url, data=body, method='POST')
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    req.add_header('X-Idempotency-Key', idempotency_key)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode('utf-8', errors='replace')
            return True, int(getattr(resp, 'status', 200)), text
    except urlerror.HTTPError as e:
        detail = e.read().decode('utf-8', errors='replace') if getattr(e, 'fp', None) else str(e)
        return False, int(getattr(e, 'code', 500)), detail
    except Exception as e:
        return False, 0, str(e)


def _telegram_api(method: str, payload: dict) -> tuple[bool, str, dict]:
    token = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
    if not token:
        return False, 'TELEGRAM_BOT_TOKEN belum diatur.', {}
    url = f'https://api.telegram.org/bot{token}/{method}'
    data = parse.urlencode(payload).encode('utf-8')
    req = request.Request(url, data=data, method='POST')
    try:
        with request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode('utf-8', errors='ignore')
            parsed = json.loads(body)
            if parsed.get('ok'):
                return True, 'OK', parsed.get('result', {})
            return False, f"Galat respons Telegram: {parsed}", {}
    except urlerror.HTTPError as e:
        try:
            body = e.read().decode('utf-8', errors='ignore')
            parsed = json.loads(body)
            desc = parsed.get('description') or body
            return False, f'Telegram API HTTP {e.code}: {desc}', {}
        except Exception:
            return False, f'Telegram API HTTP {e.code}', {}
    except Exception as e:
        return False, f'Galat API Telegram: {e}', {}


def send_new_message(message: str) -> tuple[bool, str, int | None]:
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    if not chat_id:
        return False, 'TELEGRAM_CHAT_ID belum diatur.', None
    ok, msg, data = _telegram_api('sendMessage', {'chat_id': chat_id, 'text': message})
    if not ok:
        return False, msg, None
    return True, 'Pesan Telegram terkirim.', data.get('message_id')


def edit_existing_message(message_id: int, message: str) -> tuple[bool, str]:
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    if not chat_id:
        return False, 'TELEGRAM_CHAT_ID belum diatur.'
    ok, msg, _ = _telegram_api(
        'editMessageText', {'chat_id': chat_id, 'message_id': message_id, 'text': message}
    )
    if not ok:
        return False, msg
    return True, 'Pesan Telegram berhasil diperbarui.'


def send_update_reply(root_message_id: int) -> tuple[bool, str]:
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    if not chat_id:
        return False, 'TELEGRAM_CHAT_ID belum diatur.'
    ok, msg, _ = _telegram_api(
        'sendMessage',
        {
            'chat_id': chat_id,
            'text': 'Laporan sudah diperbarui.',
            'reply_to_message_id': root_message_id,
        },
    )
    if not ok:
        return False, msg
    return True, 'Balasan update terkirim.'


def _send_or_edit_telegram(parts: list[str], root_message_id: int | None) -> tuple[bool, str, int | None]:
    if not parts:
        return False, 'Konten Telegram kosong.', root_message_id

    first_part = parts[0]
    root_id = root_message_id

    if root_id:
        ok_edit, msg_edit = edit_existing_message(root_id, first_part)
        if not ok_edit:
            m = msg_edit.lower()
            if ('message to edit not found' in m) or ("can't be edited" in m) or ("message can't be edited" in m):
                ok_new, msg_new, new_root = send_new_message(first_part)
                if not ok_new:
                    return False, msg_new, root_id
                root_id = new_root
            else:
                return False, msg_edit, root_id
    else:
        ok_new, msg_new, new_root = send_new_message(first_part)
        if not ok_new:
            return False, msg_new, root_id
        root_id = new_root

    for extra in parts[1:]:
        ok_extra, msg_extra, _ = send_new_message(extra)
        if not ok_extra:
            return False, msg_extra, root_id

    if root_id:
        send_update_reply(root_id)
    return True, 'Telegram send/edit selesai.', root_id


def append_sheets_rows(rows: list[dict], idempotency_key: str) -> dict:
    url = os.getenv('SHEETS_WEBHOOK_URL', '').strip()
    if not rows:
        return {'ok': True, 'skipped': True, 'message': 'Rows kosong, skip backup.'}
    if not url:
        return {
            'ok': True,
            'skipped': True,
            'message': 'SHEETS_WEBHOOK_URL belum diatur. Telegram tetap bisa jalan.',
        }

    payload = {'rows': rows}
    ok, status, detail = _post_json(url, payload, idempotency_key)
    if not ok:
        return {
            'ok': False,
            'skipped': False,
            'message': f'Backup Sheets gagal (status={status}).',
            'detail': detail,
        }
    return {'ok': True, 'skipped': False, 'message': 'Backup Sheets berhasil.'}


def load_pending_submission() -> dict | None:
    if not PENDING_PATH.exists():
        return None
    try:
        data = json.loads(PENDING_PATH.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def save_pending_submission(record: dict) -> None:
    PENDING_PATH.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')


def clear_pending_submission() -> None:
    if PENDING_PATH.exists():
        PENDING_PATH.unlink()


def process_submission(record: dict) -> dict:
    record['attempt_count'] = int(record.get('attempt_count', 0)) + 1
    record['last_attempt_at'] = datetime.now().isoformat()

    if not record.get('telegram_sent', False):
        ok, msg, new_root = _send_or_edit_telegram(
            parts=record.get('telegram_parts', []),
            root_message_id=record.get('telegram_root_message_id'),
        )
        if not ok:
            record['last_error'] = msg
            record['last_error_detail'] = ''
            return {'ok': False, 'warning': None, 'message': msg}
        record['telegram_sent'] = True
        record['telegram_root_message_id'] = new_root

    sheets = append_sheets_rows(record.get('sheets_rows', []), record.get('idempotency_key', ''))
    if not sheets.get('ok'):
        record['last_error'] = sheets.get('message', 'Backup Sheets gagal.')
        record['last_error_detail'] = sheets.get('detail', '')
        return {'ok': False, 'warning': None, 'message': record['last_error']}

    warning = sheets.get('message') if sheets.get('skipped') else None
    return {'ok': True, 'warning': warning, 'message': 'Submit selesai.'}
