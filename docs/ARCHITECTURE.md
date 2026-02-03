# Arquitectura

Este proyecto implementa un **pipeline de extracción y normalización** desde la API v3 de ActiveCampaign. La idea es transformar eventos y actividades “operacionales” (emails, notas, tareas, campañas, automatizaciones) en **tablas analíticas** (dim/fact) listas para BI, SQL o un data warehouse.

---

## Objetivos de diseño

1. **Reproducible**: misma configuración → mismos outputs.
2. **Idempotente**: puedes re-ejecutar sin generar duplicados.
3. **Incremental**: procesar solo lo nuevo y reanudar tras cortes.
4. **Amigable con la API**: rate limiting, retries y backoff.
5. **Enrichment**: convertir IDs en nombres con lookups/caches.

---

## Componentes

### 1) `ac_client.py` (API client)
Responsable de:
- Autenticación (token)
- Requests HTTP
- Paginación (cursor/offset según endpoint)
- Retries ante errores transitorios (5xx / timeouts)
- Throttling simple (`sleep_between_calls`)

**Input:** endpoint + params  
**Output:** JSON (listas de objetos + metadatos de paginación)

### 2) `extractor.py` (orquestación de extracción)
- Define el conjunto de endpoints a consultar.
- Recolecta actividades y referencias (por ejemplo IDs de campañas/automatizaciones).
- Aplica estrategia **full** o **incremental** según configuración.

### 3) `normalizer.py` (normalización / modelado)
- Mapea JSON → tablas:
  - `fact_*` (eventos/actividades)
  - `dim_*` (catálogos: campañas, automatizaciones, usuarios, etc.)
- Limpia campos y estandariza tipos (fechas, ids, textos).

### 4) `storage.py` (persistencia)
- Escribe CSVs de salida.
- Implementa **merge/dedup** (por clave primaria) para que la corrida sea idempotente.
- Mantiene un **checkpoint** (watermark) para incremental.

---

## Flujo de ejecución (simplificado)

```mermaid
flowchart TD
  A[Load config (.env/args)] --> B[Read checkpoint]
  B --> C{Mode}
  C -->|full| D[Fetch all activities]
  C -->|incremental| E[Fetch activities > watermark]
  D --> F[Collect referenced IDs]
  E --> F
  F --> G[Fetch dimensions (campaigns, automations, users...)]
  G --> H[Normalize to dim/fact tables]
  H --> I[Write CSVs + merge/dedup]
  I --> J[Update checkpoint + run_log]
```

---

## Checkpoint incremental (watermark)

El checkpoint guarda el “último punto seguro” procesado (por ejemplo `last_activity_id` o `last_activity_date`).  
En modo incremental, la extracción parte desde ese punto y solo trae lo nuevo.

**Ventaja:** si el notebook se cierra, Windows entra en suspensión, o ocurre un error, puedes re-ejecutar y retomar sin repetir todo.

---

## Rate limiting & retries

- **Throttling**: pequeño `sleep` entre llamadas para evitar límites.
- **Retries**: ante errores transitorios, reintenta con backoff.
- **Observabilidad**: logs por `run_id` con contadores y tiempos.

---

## Enrichment y caching

Los endpoints de actividades suelen devolver IDs (campaignId, automationId, userId).  
El pipeline hace *lookups* para traducirlos a nombres, y usa cache para evitar llamadas repetidas.

---

## Salidas (modelo analítico)

- **facts**: eventos “en el tiempo”
- **dims**: catálogos “estables”

Ejemplos:
- `fact_activities.csv`
- `fact_email_activities.csv`
- `dim_campaigns.csv`
- `dim_automations.csv`

