# ActiveCampaign API Data Pipeline

Pipeline de extracción + normalización desde la **API v3 de ActiveCampaign** para construir datasets “analytics-ready” (tablas tipo *dim/fact*) desde actividades de contactos: emails, notas, tareas, campañas, automatizaciones, etc.

> ✅ Pensado como proyecto de portafolio: estructura clara, enfoque de *data engineering*, ejecución reproducible, y sin datos/credenciales reales.

---

## Qué hace

- **Extrae** actividades y objetos relacionados desde múltiples endpoints de ActiveCampaign (v3).
- **Enriquece** entidades (por ejemplo: campañas, automatizaciones, usuarios) para convertir IDs “crípticos” en nombres legibles.
- **Normaliza** a CSVs listos para BI / SQL (tablas *dim* y *fact*).
- **Soporta incremental**: re-ejecutas a diario y solo procesa lo nuevo (con checkpoint).
- **Dedup / idempotencia**: evita duplicados al hacer *append + merge* con el histórico.

---

## Engineering highlights (lo “vendible” en GitHub)

- **Paginación robusta** (cursor/offset según endpoint) para recorrer miles de registros sin romper memoria.
- **Checkpoint incremental** (watermark) para reanudar después de cortes, reinicios o errores.
- **Rate limiting & retries**: backoff/esperas configurables para no “matar” la API.
- **Enrichment con cache** para minimizar llamadas repetidas (evitar patrón N+1).
- **Logs por corrida** con `run_id`, métricas de progreso y archivos de salida versionados.
- **Diseño de datos** tipo star-schema: `dim_*` + `fact_*` para análisis.

---

## Estructura del repo

```
.
├─ notebooks/
│  └─ ac_activity_extractor.ipynb
├─ scripts/
│  └─ run_ac_extraction.py
├─ src/
│  ├─ ac_client.py
│  ├─ extractor.py
│  ├─ normalizer.py
│  └─ storage.py
├─ docs/
│  ├─ USER_MANUAL.md
│  ├─ ARCHITECTURE.md
│  ├─ TROUBLESHOOTING.md
│  └─ PRIVACY.md
├─ samples/
│  └─ sample_output_schema.csv
└─ README.md
```

---

## Requisitos

- Python 3.10+ recomendado
- Windows / macOS / Linux
- Cuenta ActiveCampaign con acceso a API v3

Instala dependencias:

```bash
pip install -r requirements.txt
```

---

## Configuración (sin exponer secretos)

Crea un archivo `.env` (NO se commitea):

```bash
AC_BASE_URL="https://YOUR_ACCOUNT.api-us1.com"
AC_API_TOKEN="YOUR_TOKEN"
OUTPUT_DIR="data_out"
MODE="incremental"   # full | incremental
START_FROM=""        # opcional: activity_id / watermark manual
SLEEP_BETWEEN_CALLS=0.2
```

---

## Cómo correr

### Opción A — Notebook (VS Code / Jupyter)

1. Abre `notebooks/ac_activity_extractor.ipynb`
2. Configura `.env`
3. Ejecuta las celdas en orden

✅ Ideal para exploración, debugging y revisar DataFrames.

### Opción B — CLI (más “ingeniería”)

```bash
python scripts/run_ac_extraction.py --mode incremental
```

Flags típicos:

```bash
python scripts/run_ac_extraction.py --mode full --output data_out
```

---

## Outputs esperados

Ejemplos (pueden variar según tu implementación):

- `data_out/fact_activities.csv`
- `data_out/fact_email_activities.csv`
- `data_out/dim_contacts.csv`
- `data_out/dim_campaigns.csv`
- `data_out/dim_automations.csv`
- `data_out/run_log.json`

---

## Consejos para corridas largas (Windows)

- **No pongas el PC en suspensión** mientras corre el notebook: se cortan las conexiones y el kernel puede quedar “colgado”.
- Si se corta, **no pasa nada**: vuelve a correr en `MODE=incremental` y retoma desde el checkpoint.

Más detalles: `docs/TROUBLESHOOTING.md`.

---

## Documentación

- Guía de uso: `docs/USER_MANUAL.md`
- Arquitectura: `docs/ARCHITECTURE.md`
- Problemas comunes: `docs/TROUBLESHOOTING.md`
- Privacidad: `docs/PRIVACY.md`

---

## Nota legal / privacidad

Este repo **no incluye** tokens, datos reales, ni CSVs con información personal.  
Si lo usas con datos reales, aplica tus políticas internas y revisa `docs/PRIVACY.md`.
