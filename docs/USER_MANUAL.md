# Manual de usuario

Este proyecto extrae y normaliza datos desde la **API v3 de ActiveCampaign** para generar CSVs listos para análisis (tablas `dim_*` y `fact_*`).

---

## 1) Requisitos

- Python 3.10+ recomendado
- Acceso a ActiveCampaign API v3 (base URL + token)

---

## 2) Instalación

```bash
pip install -r requirements.txt
```

---

## 3) Configuración

Crea un archivo `.env` (no se sube a GitHub):

```bash
AC_BASE_URL="https://YOUR_ACCOUNT.api-us1.com"
AC_API_TOKEN="YOUR_TOKEN"
OUTPUT_DIR="data_out"
MODE="incremental"   # full | incremental
SLEEP_BETWEEN_CALLS=0.2
```

Notas:
- `MODE=full`: trae todo el histórico (más lento).
- `MODE=incremental`: trae solo lo nuevo usando checkpoint.

---

## 4) Ejecutar con notebook (VS Code Notebook / Jupyter)

1. Abre `notebooks/ac_activity_extractor.ipynb`
2. Ejecuta las celdas en orden
3. Revisa los DataFrames y los CSVs generados en `OUTPUT_DIR`

---

## 5) Ejecutar por CLI (recomendado para “producción”)

```bash
python scripts/run_ac_extraction.py --mode incremental
```

Opcional:

```bash
python scripts/run_ac_extraction.py --mode full --output data_out
```

---

## 6) Outputs

Los CSVs aparecen en `OUTPUT_DIR/`:

- `fact_activities.csv`
- `fact_email_activities.csv` (si aplica)
- `dim_*` (campaigns, automations, users, etc.)
- `run_log.json` (metadatos de la corrida)

---

## 7) Incremental: cómo funciona y cómo reanudar

El pipeline guarda un **checkpoint** (watermark) al final de cada corrida.  
Si se interrumpe, basta con volver a correr en `MODE=incremental` y retomará desde el último punto guardado.

---

## 8) Buenas prácticas

- No pongas el PC en suspensión mientras corre.
- Si tu cuenta tiene límites agresivos de API, sube `SLEEP_BETWEEN_CALLS`.
- Versiona tus outputs por fecha si los vas a comparar con BI.

---

## 9) Soporte / troubleshooting

Ver `docs/TROUBLESHOOTING.md`.
