# Troubleshooting

Esta guía cubre fallos típicos en ejecuciones largas (especialmente en **VS Code Notebook en Windows**) y cómo recuperarte sin perder el avance.

---

## 1) “Se demora demasiado / quedó pegado”

Es normal que una corrida **full** demore horas si tienes decenas de miles de actividades y además haces enrichment de campañas/automatizaciones.

Qué hacer:
- Revisa que el progreso (tqdm/logs) siga avanzando.
- Aumenta `SLEEP_BETWEEN_CALLS` si ves muchos errores 429/limit.
- Considera correr `MODE=incremental` diariamente en lugar de full.

---

## 2) ¿Puedo detenerlo?

Sí:
- En notebook: usa **Stop/Interrupt** en la celda.
- En CLI: `Ctrl + C`.

**Importante:** si tu implementación guarda checkpoint periódicamente, puedes reanudar luego con `MODE=incremental`.

---

## 3) ¿Qué pasa si pongo Windows en “suspensión”?

Mala idea durante la corrida.

Cuando el PC entra en suspensión:
- Se cortan conexiones de red
- El kernel puede quedar inestable
- Llamadas HTTP pueden fallar / colgar

Recomendación:
- Mantén el equipo activo mientras corre.
- Si se durmió igual, reinicia el kernel/terminal y re-ejecuta en `MODE=incremental`.

---

## 4) DeprecationWarning: “truth value of an empty array is ambiguous”

Esto suele venir de una condición tipo `if pd.isna(v):` cuando `v` puede ser array/serie.

Solución típica:
- Si `v` puede ser array: usa `v.size > 0` / `len(v) > 0` según corresponda.
- Si esperas escalares: fuerza a escalar antes de validar (`v = v[0]` si aplica) o normaliza el tipo.

---

## 5) 401/403 Unauthorized

- Revisa `AC_API_TOKEN`
- Revisa `AC_BASE_URL` (la “shard” correcta, por ejemplo `api-us1`, etc.)

---

## 6) Errores 429 (rate limit)

- Aumenta `SLEEP_BETWEEN_CALLS` (por ejemplo 0.5 o 1.0)
- Reduce concurrencia si implementaste paralelismo
- Evita enrichment “por registro” sin cache

---

## 7) Consejos prácticos (para que no duela)

- Empieza con una corrida corta (pocos contactos/activities).
- Verifica outputs y llaves primarias (para dedup).
- Luego corre full una vez, y después incremental diario.
