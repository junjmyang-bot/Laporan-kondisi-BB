# Laporan Kondisi BB (Streamlit)

Mobile-first laporan operasional BB dengan pola dasar seperti aplikasi Kupas:
- Team lock: `Buka Tim` / `Ambil Alih Tim`
- State persistence per scope `(work_date + team_id)`
- Telegram submit: edit-first, fallback to new message
- Google Sheets backup append (non-blocking warning jika belum diset)
- Pending retry untuk kegagalan network/transient

## Struktur
- `app.py`: UI + lock/scope/persist + submit flow
- `bb_schema.py`: schema/default payload/columns
- `bb_validation.py`: validasi
- `bb_formatters.py`: formatter Telegram + row Sheets
- `bb_integrations.py`: Telegram API + Sheets webhook + pending queue

## Environment Variables
Required:
- `TEAM_PASSWORDS` (JSON object)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional:
- `SHEETS_WEBHOOK_URL`

Example `TEAM_PASSWORDS`:
```json
{"KUPAS-1":"abcd","KUPAS-2":"1234","KUPAS-3":"ab12"}
```

## Run
```powershell
streamlit run app.py
```

## Notes
- Timezone system default: `Asia/Jakarta`
- Local state file: `.laporan_kondisi_bb_state.json`
- Pending retry file: `.bb_pending_submission.json`
