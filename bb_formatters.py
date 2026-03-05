from __future__ import annotations


def _norm_ox(value: str) -> str:
    raw = str(value or '').strip().upper()
    return raw if raw in {'O', 'X'} else '-'


def _split_lines(text: str) -> list[str]:
    out = []
    for line in str(text or '').splitlines():
        line = line.strip()
        if line:
            out.append(line)
    return out


def _display_time(value: str) -> str:
    raw = str(value or '').strip()
    return raw.replace(':', '.') if raw else '--.--'


def _display_date(value: str) -> str:
    raw = str(value or '').strip()
    return raw.replace('/', ' / ') if raw else '-'


def _format_bb_rows(rows: list[dict]) -> list[str]:
    lines = []
    for row in rows or []:
        supplier = str(row.get('supplier', '')).strip()
        ukuran = str(row.get('ukuran', '')).strip()
        matang = _norm_ox(row.get('tingkat_matang', ''))
        selesai = _norm_ox(row.get('selesai', ''))
        if not supplier and not ukuran:
            continue
        if selesai == '-':
            lines.append(f'{supplier} / {ukuran} / {matang}')
        else:
            lines.append(f'{supplier} / {ukuran} / {matang} / {selesai}')
    return lines


def _hb_cell(row: dict) -> str:
    dipakai = _norm_ox(row.get('dipakai', ''))
    alasan = str(row.get('alasan', '')).strip() or '-'
    gas = _norm_ox(row.get('gas', ''))
    return f'{dipakai}|{alasan}|{gas}'


def _time_sort_key(hhmm: str) -> tuple[int, int]:
    try:
        h, m = str(hhmm).split(':', 1)
        return int(h), int(m)
    except Exception:
        return 99, 99


def build_telegram_text(payload: dict) -> str:
    group_3 = payload.get('group_3', [])
    group_4 = payload.get('group_4', [])
    group_5 = payload.get('group_5', [])
    group_6 = payload.get('group_6', [])
    group_7 = payload.get('group_7', [])

    lines = [
        'LAPORAN KONDISI BB',
        '',
        '-> Pelapor: Petugas Lapor Kontaminan',
        '-> Durasi: Awal masuk & setiap kali masukkan bb ke kupas & 30 menit',
        '',
        f"1. Tanggal : {_display_date(payload.get('work_date', '-'))}",
        f"2. Pelapor : {str(payload.get('reporter', '-')).strip()}",
        '',
        '----------------------------------------------------------------------------------------------------',
        '3. List bb yang akan dikerjakan oleh shift ku',
        '=> sudah baca list BB di steam',
        '=> sudah tahu akan kerjakan BB apa dan ukuran apa',
    ]
    for slot in group_3:
        title = _display_time(slot.get('slot_time', '--:--'))
        lines.append('')
        lines.append(title)
        lines.append(f"{_norm_ox(slot.get('list_read_steam'))} / {_norm_ox(slot.get('list_know_bb_size'))}")

    lines.extend(
        [
            '',
            '---------------------------------------------------------------------------------------------------',
            '4. BB masuk',
            '-> jika bb matangnya benar kasih keterangan "O"',
            '-> jika bb sudah habis kasih keterangan "O"',
            '',
            '-> supplier / ukuran / tingkat matang / selesai',
            'Contoh: Riski 6 A / K / O / O',
        ]
    )
    for slot in group_4:
        bb_lines = _format_bb_rows(slot.get('bb_masuk', []))
        if not bb_lines:
            continue
        title = _display_time(slot.get('slot_time', '--:--'))
        lines.append('')
        lines.append(title)
        lines.extend(bb_lines)

    lines.extend(
        [
            '',
            '---------------------------------------------------------------------------------------------------',
            '5. Status keranjang ubi sudah steam:',
            '=> Keranjang stainless sudah kosong',
            '=> Sudah handover keranjang kosong ke steam',
            '=> sebelum istirahat sudah ksosongkan semua keranjang steam',
        ]
    )
    for slot in group_5:
        title = _display_time(slot.get('slot_time', '--:--'))
        lines.append(
            f"{title} / "
            f"{_norm_ox(slot.get('steam_basket_empty'))} / "
            f"{_norm_ox(slot.get('steam_handover'))} / "
            f"{_norm_ox(slot.get('steam_prebreak_empty'))}"
        )

    lines.extend(
        [
            '',
            '---------------------------------------------------------------------------------------------------',
            '6. Status keranjang belum steam:',
            '=> Keranjang stainless sudah diisi',
            '',
            '00.00 / Isi / alasan',
        ]
    )
    for slot in group_6:
        title = _display_time(slot.get('slot_time', '--:--'))
        lines.append(
            f"{title} / "
            f"{_norm_ox(slot.get('unsteam_status'))} / "
            f"{str(slot.get('unsteam_reason', '')).strip() or '-'}"
        )

    lines.extend(
        [
            '',
            '---------------------------------------------------------------------------------------------------',
            '7. Status pemakaian HB nya gimana? (Update per 30 menit)',
            '',
            '00:00/ dipakai / alasan / gas',
        ]
    )
    for slot in group_7:
        title = _display_time(slot.get('slot_time', '--:--'))
        lines.append('')
        lines.append(title)
        for hb in slot.get('hb_rows', []):
            hb_name = str(hb.get('hb', '')).strip()
            dipakai = _norm_ox(hb.get('dipakai'))
            alasan = str(hb.get('alasan', '')).strip() or '-'
            gas = _norm_ox(hb.get('gas'))
            lines.append(f'{hb_name} / {dipakai} / {alasan} / {gas}')

    lines.extend(
        [
            '---------------------------------------',
            '-----------------------------------------',
            '8. Catatan:',
            '-> jika ada kondisi ubi jelek (putih, bongkeng, busuk, steam tidak matang) tetap lapor sendiri',
            '',
            '-> lapor dengan foto, jumlah ubi jelek dalam persentase',
            '',
            '-> cara hitung : total bb jelek / total bb sudah masuk X 100',
            '',
            '(Diupdate 25/11/2025 // berlaku 25/11/2025)',
        ]
    )

    ket_lines = _split_lines(payload.get('keterangan', ''))
    if ket_lines:
        lines.extend(['', '9. Keterangan:'])
        lines.extend(ket_lines)

    return '\n'.join(lines).strip()


def split_telegram_parts(message: str, soft_limit: int = 3200) -> list[str]:
    text = str(message or '').strip()
    if len(text) <= soft_limit:
        return [text]

    blocks = [b.strip() for b in text.split('\n\n') if b.strip()]
    parts = []
    current = ''
    for block in blocks:
        candidate = block if not current else f'{current}\n\n{block}'
        if len(candidate) <= soft_limit:
            current = candidate
            continue
        if current:
            parts.append(current)
        current = block
    if current:
        parts.append(current)

    if len(parts) <= 1:
        return [text[i : i + soft_limit] for i in range(0, len(text), soft_limit)]

    out = []
    for idx, part in enumerate(parts, start=1):
        if idx == 1:
            out.append(part)
        else:
            out.append(f'Lanjutan laporan mulai --:-- (part {idx})\n\n{part}')
    return out


def build_sheets_rows(payload: dict, idempotency_key: str, submitted_at: str) -> list[dict]:
    group_3 = {str(x.get('slot_time', '')): x for x in payload.get('group_3', [])}
    group_4 = {str(x.get('slot_time', '')): x for x in payload.get('group_4', [])}
    group_5 = {str(x.get('slot_time', '')): x for x in payload.get('group_5', [])}
    group_6 = {str(x.get('slot_time', '')): x for x in payload.get('group_6', [])}
    group_7 = {str(x.get('slot_time', '')): x for x in payload.get('group_7', [])}

    all_times = sorted(set(group_3.keys()) | set(group_4.keys()) | set(group_5.keys()) | set(group_6.keys()) | set(group_7.keys()), key=_time_sort_key)
    rows = []
    for slot_time in all_times:
        s3 = group_3.get(slot_time, {})
        s4 = group_4.get(slot_time, {})
        s5 = group_5.get(slot_time, {})
        s6 = group_6.get(slot_time, {})
        s7 = group_7.get(slot_time, {})
        hb_map = {str(r.get('hb', '')).strip(): r for r in s7.get('hb_rows', [])}

        def hb_cell(name: str) -> str:
            return _hb_cell(hb_map.get(name, {}))

        bb_summary = '; '.join(_format_bb_rows(s4.get('bb_masuk', [])))
        rows.append(
            {
                'report_name': payload.get('report_name', 'LAPORAN KONDISI BB'),
                'work_date': payload.get('work_date', ''),
                'team_id': payload.get('team_id', ''),
                'shift': payload.get('shift', ''),
                'reporter': payload.get('reporter', ''),
                'slot_time': slot_time,
                'list_read_steam': _norm_ox(s3.get('list_read_steam', '')),
                'list_know_bb_size': _norm_ox(s3.get('list_know_bb_size', '')),
                'bb_masuk_summary': bb_summary,
                'steam_basket_empty': _norm_ox(s5.get('steam_basket_empty', '')),
                'steam_handover': _norm_ox(s5.get('steam_handover', '')),
                'steam_prebreak_empty': _norm_ox(s5.get('steam_prebreak_empty', '')),
                'unsteam_status': _norm_ox(s6.get('unsteam_status', '')),
                'unsteam_reason': str(s6.get('unsteam_reason', '')).strip(),
                'hb1': hb_cell('HB 1'),
                'hb2': hb_cell('HB 2'),
                'hb3': hb_cell('HB 3'),
                'hb4': hb_cell('HB 4'),
                'hb5': hb_cell('HB 5'),
                'hb6': hb_cell('HB 6'),
                'hb7': hb_cell('HB 7'),
                'defect_flag': bool(payload.get('defect_flag', False)),
                'defect_types': str(payload.get('defect_types', '')).strip(),
                'defect_percent': str(payload.get('defect_percent', '')).strip(),
                'defect_photo_count': int(payload.get('defect_photo_count', 0) or 0),
                'notes': str(payload.get('notes', '')).strip(),
                'keterangan': str(payload.get('keterangan', '')).strip(),
                'idempotency_key': idempotency_key,
                'submitted_at': submitted_at,
            }
        )
    return rows
