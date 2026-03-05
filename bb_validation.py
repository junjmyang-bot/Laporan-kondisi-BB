from __future__ import annotations


def _is_ox(value: str) -> bool:
    return str(value or '').strip().upper() in {'O', 'X'}


def _has_slot_note(slot: dict) -> bool:
    return str(slot.get('slot_note', '')).strip() in {'Selesai di sini', 'Mulai kerja baru', 'Lanjut kerja'}


def validate_payload(payload: dict) -> list[str]:
    errors = []
    if not str(payload.get('work_date', '')).strip():
        errors.append('Tanggal wajib diisi.')
    if not str(payload.get('reporter', '')).strip():
        errors.append('Pelapor wajib diisi.')

    group_3 = payload.get('group_3', [])
    group_4 = payload.get('group_4', [])
    group_5 = payload.get('group_5', [])
    group_6 = payload.get('group_6', [])
    group_7 = payload.get('group_7', [])

    if not group_3:
        errors.append('Section 3 minimal 1 slot.')
    if not group_4:
        errors.append('Section 4 minimal 1 slot.')
    if not group_5:
        errors.append('Section 5 minimal 1 slot.')
    if not group_6:
        errors.append('Section 6 minimal 1 slot.')
    if not group_7:
        errors.append('Section 7 minimal 1 slot.')

    for slot in group_3:
        label = slot.get('slot_time', '--:--')
        if not _has_slot_note(slot):
            errors.append(f'[{label}] Section 3: status slot wajib dipilih.')
        if not _is_ox(slot.get('list_read_steam')):
            errors.append(f'[{label}] Section 3: pilih O/X untuk "sudah baca list BB di steam".')
        if not _is_ox(slot.get('list_know_bb_size')):
            errors.append(f'[{label}] Section 3: pilih O/X untuk "sudah tahu BB & ukuran".')

    for slot in group_4:
        label = slot.get('slot_time', '--:--')
        if not _has_slot_note(slot):
            errors.append(f'[{label}] Section 4: status slot wajib dipilih.')
        for row in slot.get('bb_masuk', []):
            supplier = str(row.get('supplier', '')).strip()
            ukuran = str(row.get('ukuran', '')).strip()
            matang = str(row.get('tingkat_matang', '')).strip().upper()
            selesai = str(row.get('selesai', '')).strip().upper()
            if supplier or ukuran or matang or selesai:
                if not supplier:
                    errors.append(f'[{label}] Section 4: supplier wajib diisi.')
                if not ukuran:
                    errors.append(f'[{label}] Section 4: ukuran wajib diisi.')
                if matang not in {'O', 'X'}:
                    errors.append(f'[{label}] Section 4: tingkat matang wajib pilih O/X.')
                if selesai not in {'O', 'X'}:
                    errors.append(f'[{label}] Section 4: selesai wajib pilih O/X.')

    for slot in group_5:
        label = slot.get('slot_time', '--:--')
        if not _has_slot_note(slot):
            errors.append(f'[{label}] Section 5: status slot wajib dipilih.')
        if not _is_ox(slot.get('steam_basket_empty')):
            errors.append(f'[{label}] Section 5: keranjang stainless kosong wajib O/X.')
        if not _is_ox(slot.get('steam_handover')):
            errors.append(f'[{label}] Section 5: handover wajib O/X.')
        if not _is_ox(slot.get('steam_prebreak_empty')):
            errors.append(f'[{label}] Section 5: sebelum istirahat wajib O/X.')

    for slot in group_6:
        label = slot.get('slot_time', '--:--')
        if not _has_slot_note(slot):
            errors.append(f'[{label}] Section 6: status slot wajib dipilih.')
        unsteam = str(slot.get('unsteam_status', '')).strip().upper()
        reason = str(slot.get('unsteam_reason', '')).strip()
        if unsteam not in {'O', 'X'}:
            errors.append(f'[{label}] Section 6: status wajib O/X.')
        if unsteam == 'X' and not reason:
            errors.append(f'[{label}] Section 6: alasan wajib saat status = X.')

    for slot in group_7:
        label = slot.get('slot_time', '--:--')
        if not _has_slot_note(slot):
            errors.append(f'[{label}] Section 7: status slot wajib dipilih.')
        for hb in slot.get('hb_rows', []):
            hb_name = str(hb.get('hb', 'HB')).strip()
            dipakai = str(hb.get('dipakai', '')).strip().upper()
            gas = str(hb.get('gas', '')).strip().upper()
            alasan = str(hb.get('alasan', '')).strip()
            if dipakai not in {'O', 'X'}:
                errors.append(f'[{label}] Section 7 {hb_name}: dipakai wajib O/X.')
            if gas not in {'O', 'X'}:
                errors.append(f'[{label}] Section 7 {hb_name}: gas wajib O/X.')
            if dipakai == 'X' and not alasan:
                errors.append(f'[{label}] Section 7 {hb_name}: alasan wajib saat dipakai = X.')

    return errors
