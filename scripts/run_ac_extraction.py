# -*- coding: utf-8 -*-
"""
ActiveCampaign — Extracción HISTÓRICA + INCREMENTAL + DEDUPE (sin perder info) + FULL CHATTER + AUTOMATIONS

Qué agrega esta versión:
- /contacts/{id}/automationEntryCounts (cuántas veces entró a cada automatización) 
- /contacts/{id}/contactAutomations   (detalle de automatizaciones activas/completadas por contacto) 
- Enriquecimiento de emailActivities con:
 - campaign_name + message_subject + automation_name (usando dimensiones /campaigns, /messages, /automations) 

Notas importantes:
- "automationEntryCounts" NO trae "mensajes enviados". Solo trae automatización + estado + contador. 
- Para ver qué email/campaña se envió y qué abrió/clickeó el contacto, usamos /emailActivities y lo enlazamos con /campaigns y /messages. 
- Paginación/filters: limit/offset y filters[...] 
"""

from __future__ import annotations

import os
import re
import json
import time
import csv
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import pandas as pd
from tqdm import tqdm


# -------------------------
# Config
# -------------------------
@dataclass
class ACConfig:
  base_url: str         # e.g. https://YOURACCOUNT.api-us1.com/api/3
  api_token: str
  page_limit: int = 100
  rate_sleep: float = 0.25   # 0.25s ≈ 4 req/s (seguro bajo límite 5 req/s)
  total_retries: int = 6
  backoff_factor: float = 0.5


# -------------------------
# Utils I/O
# -------------------------
def ensure_dir(path: str) -> None:
  os.makedirs(path, exist_ok=True)


def utc_now_iso() -> str:
  return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_int(x: Any, default: int = 0) -> int:
  try:
    return int(str(x).strip())
  except Exception:
    return default


def write_csv_utf8sig(path: str, df: pd.DataFrame) -> None:
  ensure_dir(os.path.dirname(path))
  df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)


def load_json(path: str, default: Any) -> Any:
  if not os.path.exists(path):
    return default
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)


def save_json(path: str, obj: Any) -> None:
  ensure_dir(os.path.dirname(path))
  with open(path, "w", encoding="utf-8") as f:
    json.dump(obj, f, ensure_ascii=False, indent=2)


def extract_id_from_url(url: str) -> str:
  if not url:
    return ""
  m = re.search(r"/(\d+)(?:\?|$)", str(url))
  return m.group(1) if m else ""


# -------------------------
# Dedupe persistence (delta + master/raw + master/latest)
# -------------------------
def _normalize_for_hash(v: Any) -> str:
  """Normaliza valores (incluye listas/dicts/arrays) para hash estable."""
  if v is None:
    return ""
  # NA / NaN (pd.isna puede devolver array para listas, por eso try/except)
  try:
    if pd.isna(v):
      return ""
  except Exception:
    pass
  # Timestamps
  if isinstance(v, (pd.Timestamp, datetime)):
    return v.isoformat()
  # bytes
  if isinstance(v, (bytes, bytearray)):
    try:
      return v.decode('utf-8', errors='replace')
    except Exception:
      return str(v)
  # numpy arrays u objetos similares
  if hasattr(v, 'tolist') and not isinstance(v, (str, bytes, bytearray)):
    try:
      v = v.tolist()
    except Exception:
      pass
  # colecciones -> JSON estable
  if isinstance(v, (list, dict, tuple, set)):
    if isinstance(v, (tuple, set)):
      v = list(v)
    return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
  return str(v)


def df_rowhash(df: pd.DataFrame, cols: List[str]) -> pd.Series:
  # Hash estable por contenido (sin reventar con listas/dicts/arrays)
  def _h(row: pd.Series) -> str:
    s = "||".join(_normalize_for_hash(row.get(c)) for c in cols)
    return hashlib.md5(s.encode("utf-8")).hexdigest()
  return df.apply(_h, axis=1)


def persist_table_dual(
  *,
  output_base: str,
  run_id: str,
  table_name: str,
  df_new: pd.DataFrame,
  key_cols_latest: Optional[List[str]] = None,
  updated_at_col: Optional[str] = None,
) -> None:
  """
  - runs/<run_id>/delta/<table>.csv : delta de esta ejecución
  - master/raw/<table>.csv     : acumulado dedupe por hash
  - master/latest/<table>.csv   : snapshot por key_cols_latest (último por updated_at_col o extracted_at)
  """
  if df_new is None or df_new.empty:
    return

  df_new = df_new.copy()
  df_new["run_id"] = run_id
  df_new["extracted_at"] = utc_now_iso()

  # 1) Delta
  delta_path = os.path.join(output_base, "runs", run_id, "delta", f"{table_name}.csv")
  write_csv_utf8sig(delta_path, df_new)

  # 2) Master RAW (dedupe por hash)
  raw_dir = os.path.join(output_base, "master", "raw")
  ensure_dir(raw_dir)
  raw_path = os.path.join(raw_dir, f"{table_name}.csv")

  if os.path.exists(raw_path):
    df_old = pd.read_csv(raw_path, dtype=str)
    df_all = pd.concat([df_old, df_new], ignore_index=True)
  else:
    df_all = df_new

  content_cols = [c for c in df_all.columns if c not in ("run_id", "extracted_at", "_row_hash")]
  df_all["_row_hash"] = df_rowhash(df_all, content_cols)
  df_all = df_all.drop_duplicates(subset=["_row_hash"], keep="first")
  write_csv_utf8sig(raw_path, df_all)

  # 3) Master LATEST (snapshot por key)
  if key_cols_latest:
    latest_dir = os.path.join(output_base, "master", "latest")
    ensure_dir(latest_dir)
    latest_path = os.path.join(latest_dir, f"{table_name}.csv")

    # Partimos del raw dedupe (no del delta) para evitar inconsistencias
    df_latest_src = df_all.copy()

    sort_col = updated_at_col if updated_at_col and updated_at_col in df_latest_src.columns else "extracted_at"
    # orden asc y nos quedamos con el último por key
    df_latest_src = df_latest_src.sort_values(by=sort_col, ascending=True, kind="mergesort")
    df_latest = df_latest_src.drop_duplicates(subset=key_cols_latest, keep="last")
    write_csv_utf8sig(latest_path, df_latest)


# -------------------------
# ActiveCampaign client
# -------------------------
class ACClient:
  def __init__(self, cfg: ACConfig):
    self.cfg = cfg
    self.session = requests.Session()
    self.session.headers.update({
      "accept": "application/json",
      "Api-Token": cfg.api_token,
    })

  def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = self.cfg.base_url.rstrip("/") + "/" + path.lstrip("/")
    last_err: Optional[Exception] = None
    for attempt in range(self.cfg.total_retries + 1):
      try:
        r = self.session.request(method, url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
          raise requests.HTTPError(f"HTTP {r.status_code}: {r.text[:200]}", response=r)
        r.raise_for_status()
        return r.json() if r.text else {}
      except Exception as e:
        last_err = e
        sleep_s = (self.cfg.backoff_factor * (2 ** attempt))
        time.sleep(min(30.0, sleep_s))
    raise last_err # type: ignore

  def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return self._request("GET", path, params=params)

  def get_paginated_offset(
    self,
    collection_key: str,
    path: str,
    base_params: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
  ) -> List[Dict[str, Any]]:
    """Paginación offset/limit estándar. """
    out: List[Dict[str, Any]] = []
    offset = 0
    page_limit = int(limit or self.cfg.page_limit)
    while True:
      params = dict(base_params or {})
      params.update({"limit": page_limit, "offset": offset})
      data = self.get(path, params=params)
      items = data.get(collection_key, [])
      if isinstance(items, dict):
        items = [items]
      if not items:
        break
      out.extend(items)
      if len(items) < page_limit:
        break
      offset += page_limit
      time.sleep(self.cfg.rate_sleep)
    return out

  def get_contacts_after_id(self, last_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Trae contactos nuevos con id_greater + orders[id]=ASC (mejor rendimiento para cuentas grandes). 
    """
    all_new: List[Dict[str, Any]] = []
    cursor = int(last_id or 0)
    while True:
      data = self.get(
        "/contacts",
        params={"limit": limit, "orders[id]": "ASC", "id_greater": cursor},
      )
      batch = data.get("contacts", [])
      if isinstance(batch, dict):
        batch = [batch]
      if not batch:
        break
      all_new.extend(batch)
      cursor = max(cursor, max((safe_int(c.get("id")) for c in batch), default=cursor))
      if len(batch) < limit:
        break
      time.sleep(self.cfg.rate_sleep)
    return all_new


# -------------------------
# Seed + contact universe
# -------------------------
def resolve_seed_contact_ids(
  client: ACClient,
  seed_csv_path: str,
  *,
  id_col_candidates: List[str] = ["id", "contact_id", "contactid", "ID"],
  email_col_candidates: List[str] = ["email", "Email", "EMAIL"],
  use_email_fallback: bool = True,
) -> pd.DataFrame:
  """
  Lee CSV semilla y resuelve contact_id.
  - Si hay columna id/contact_id -> usa eso.
  - Si no, y hay email -> busca /contacts?filters[email]=... (puede ser más lento).
  """
  df = pd.read_csv(seed_csv_path, dtype=str).fillna("")
  cols = list(df.columns)

  def pick_col(cands: List[str]) -> str:
    for c in cands:
      if c in cols:
        return c
    # intenta case-insensitive
    lower_map = {c.lower(): c for c in cols}
    for c in cands:
      if c.lower() in lower_map:
        return lower_map[c.lower()]
    return ""

  id_col = pick_col(id_col_candidates)
  email_col = pick_col(email_col_candidates)

  out_rows: List[Dict[str, str]] = []

  if id_col:
    for _, r in df.iterrows():
      cid = str(r.get(id_col, "")).strip()
      if cid:
        out_rows.append({"contact_id": cid, "email": str(r.get(email_col, "")).strip() if email_col else ""})
    return pd.DataFrame(out_rows).drop_duplicates(subset=["contact_id"])

  if use_email_fallback and email_col:
    for _, r in tqdm(df.iterrows(), total=len(df), desc="Resolve seed emails"):
      email = str(r.get(email_col, "")).strip()
      if not email:
        continue
      data = client.get("/contacts", params={"filters[email]": email, "limit": 1})
      contacts = data.get("contacts", [])
      if isinstance(contacts, dict):
        contacts = [contacts]
      if contacts:
        out_rows.append({"contact_id": str(contacts[0].get("id", "")).strip(), "email": email})
      time.sleep(client.cfg.rate_sleep)
    return pd.DataFrame(out_rows).drop_duplicates(subset=["contact_id"])

  return pd.DataFrame(columns=["contact_id", "email"])


# -------------------------
# Dimensions (cacheable)
# -------------------------
def load_or_refresh_dim(
  *,
  client: ACClient,
  output_base: str,
  dim_name: str,
  path: str,
  collection_key: str,
  refresh_days: int = 7,
) -> pd.DataFrame:
  """
  Guarda dims en output_base/dims/<dim_name>.csv.
  Refresca si:
   - no existe, o
   - archivo más viejo que refresh_days
  """
  dims_dir = os.path.join(output_base, "dims")
  ensure_dir(dims_dir)
  csv_path = os.path.join(dims_dir, f"{dim_name}.csv")
  meta_path = os.path.join(dims_dir, f"{dim_name}.meta.json")

  meta = load_json(meta_path, {})
  last = meta.get("fetched_at_utc")
  must_refresh = not os.path.exists(csv_path)
  if last and not must_refresh:
    try:
      dt = datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
      age_days = (datetime.now(timezone.utc) - dt).days
      must_refresh = age_days >= refresh_days
    except Exception:
      must_refresh = True

  if must_refresh:
    try:
      items = client.get_paginated_offset(collection_key, path)
      df = pd.DataFrame(items)
      write_csv_utf8sig(csv_path, df)
      save_json(meta_path, {"fetched_at_utc": utc_now_iso(), "path": path, "collection_key": collection_key})
      time.sleep(client.cfg.rate_sleep)
      return df
    except requests.HTTPError as e:
      code = getattr(getattr(e, 'response', None), 'status_code', None)
      # Algunas cuentas no tienen ciertos recursos (p.ej. /scores, /accounts). No rompemos el pipeline.
      if code == 404:
        df = pd.DataFrame()
        write_csv_utf8sig(csv_path, df)
        save_json(meta_path, {"fetched_at_utc": utc_now_iso(), "path": path, "collection_key": collection_key, "error": f"HTTP {code}"})
        return df
      raise

  return pd.read_csv(csv_path, dtype=str)


def load_all_dims(client: ACClient, output_base: str, refresh_days: int = 7) -> Dict[str, pd.DataFrame]:
  dims = {}
  dims["campaigns"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="campaigns", path="/campaigns", collection_key="campaigns", refresh_days=refresh_days) # 
  dims["messages"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="messages", path="/messages", collection_key="messages", refresh_days=refresh_days)  # 
  dims["automations"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="automations", path="/automations", collection_key="automations", refresh_days=refresh_days) # 
  dims["users"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="users", path="/users", collection_key="users", refresh_days=refresh_days)
  dims["lists"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="lists", path="/lists", collection_key="lists", refresh_days=refresh_days)
  dims["tags"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="tags", path="/tags", collection_key="tags", refresh_days=refresh_days)
  dims["fields"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="fields", path="/fields", collection_key="fields", refresh_days=refresh_days)
  # CRM extras (para traducir ids a nombres)
  # Pipelines/Stages (deals)
  dims["dealGroups"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="dealGroups", path="/dealGroups", collection_key="dealGroups", refresh_days=refresh_days)
  dims["dealStages"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="dealStages", path="/dealStages", collection_key="dealStages", refresh_days=refresh_days)
  # Accounts + Scores (si tu cuenta los usa)
  dims["accounts"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="accounts", path="/accounts", collection_key="accounts", refresh_days=refresh_days)
  dims["scores"] = load_or_refresh_dim(client=client, output_base=output_base, dim_name="scores", path="/scores", collection_key="scores", refresh_days=refresh_days)
  return dims


# -------------------------
# Contact chatter extractors
# -------------------------
STATUS_MAP_AUT = {"1": "Active", "0": "Inactive", 1: "Active", 0: "Inactive"}
HIDDEN_MAP = {"1": "Yes", "0": "No", 1: "Yes", 0: "No"}


def _safe_list(data: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
  items = data.get(key, [])
  if isinstance(items, dict):
    items = [items]
  if not isinstance(items, list):
    return []
  return [x for x in items if isinstance(x, dict)]


def run_activities(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  """Activity stream del contacto. /activities?contact={id}"""
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="Activities"):
    offset = 0
    while True:
      data = client.get("/activities", params={"contact": cid, "limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "activities")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_email_activities(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  """
  Email activities (open/click/etc) filtrado por subscriberid. 
  """
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="EmailActivities"):
    offset = 0
    while True:
      data = client.get(
        "/emailActivities",
        params={"filters[subscriberid]": cid, "limit": client.cfg.page_limit, "offset": offset},
      )
      items = _safe_list(data, "emailActivities")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_notes(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactNotes"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/notes", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "notes")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_lists(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactLists"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/contactLists", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "contactLists")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_tags(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactTags"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/contactTags", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "contactTags")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_logs(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactLogs"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/contactLogs", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "contactLogs")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_tracking_logs(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="TrackingLogs"):
    offset = 0
    while True:
      try:
        data = client.get(f"/contacts/{cid}/trackingLogs", params={"limit": client.cfg.page_limit, "offset": offset})
      except requests.HTTPError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', None)
        # 404 suele significar: sin logs / feature no activa / contacto no encontrado
        if code == 404:
          break
        raise
      items = _safe_list(data, "trackingLogs")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_bounce_logs(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="BounceLogs"):
    offset = 0
    while True:
      try:
        data = client.get(f"/contacts/{cid}/bounceLogs", params={"limit": client.cfg.page_limit, "offset": offset})
      except requests.HTTPError as e:
        code = getattr(getattr(e, 'response', None), 'status_code', None)
        # 404 suele significar: sin bounce logs / contacto no tiene bounces / contacto no existe
        if code == 404:
          break
        raise
      items = _safe_list(data, "bounceLogs")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_geo_ips(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="GeoIps"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/geoIps", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "geoIps")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_goals(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactGoals"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/contactGoals", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "contactGoals")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_data(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactData"):
    try:
      data = client.get(f"/contacts/{cid}/contactData")
      items = _safe_list(data, "contactData")
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
    except requests.HTTPError:
      pass
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_score_values(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ScoreValues"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/scoreValues", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "scoreValues")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_account_contacts(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="AccountContacts"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/accountContacts", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "accountContacts")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_tasks(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  """
  Tasks asociadas DIRECTAMENTE al contacto (reltype=Subscriber).
  """
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactTasks"):
    offset = 0
    while True:
      data = client.get(
        "/dealTasks",
        params={
          "filters[reltype]": "Subscriber",
          "filters[relid]": cid,
          "limit": client.cfg.page_limit,
          "offset": offset,
        },
      )
      items = _safe_list(data, "dealTasks")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_contact_automations(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  """
  Detalle de automatizaciones donde el contacto está/estuvo (contactAutomations).
  (Se ve también en GET /contacts/:id example) 
  """
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="ContactAutomations"):
    offset = 0
    while True:
      data = client.get(f"/contacts/{cid}/contactAutomations", params={"limit": client.cfg.page_limit, "offset": offset})
      items = _safe_list(data, "contactAutomations")
      if not items:
        break
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        rows.append(row)
      if len(items) < client.cfg.page_limit:
        break
      offset += client.cfg.page_limit
      time.sleep(client.cfg.rate_sleep)
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


def run_automation_entry_counts(client: ACClient, contact_ids: List[str]) -> pd.DataFrame:
  """
  Cuántas veces entró el contacto a cada automatización. 
  """
  rows: List[Dict[str, Any]] = []
  for cid in tqdm(contact_ids, desc="AutomationEntryCounts"):
    try:
      data = client.get(f"/contacts/{cid}/automationEntryCounts")
      items = _safe_list(data, "automationEntryCounts")
      for it in items:
        row = dict(it)
        row["contact_id"] = str(cid)
        # normalización amigable
        row["status_label"] = STATUS_MAP_AUT.get(it.get("status"), it.get("status"))
        row["hidden_label"] = HIDDEN_MAP.get(it.get("hidden"), it.get("hidden"))
        rows.append(row)
    except requests.HTTPError:
      pass
    time.sleep(client.cfg.rate_sleep)
  return pd.DataFrame(rows)


# -------------------------
# Deals bundle (deals + dealTasks + dealNotes + dealActivities)
# -------------------------
def fetch_deals_for_contact(client: ACClient, contact_id: str) -> List[Dict[str, Any]]:
  try:
    deals = client.get_paginated_offset("deals", "/deals", base_params={"filters[contact]": contact_id})
    for d in deals:
      d["contact_id"] = str(contact_id)
    return deals
  except requests.HTTPError:
    return []


def fetch_deal_notes(client: ACClient, deal_id: str, contact_id: str) -> List[Dict[str, Any]]:
  try:
    notes = client.get_paginated_offset("notes", f"/deals/{deal_id}/notes")
    for n in notes:
      n["deal_id"] = str(deal_id)
      n["contact_id"] = str(contact_id)
    return notes
  except requests.HTTPError:
    return []


def fetch_deal_tasks(client: ACClient, deal_id: str, contact_id: str) -> List[Dict[str, Any]]:
  try:
    tasks = client.get_paginated_offset("dealTasks", f"/deals/{deal_id}/dealTasks")
    for t in tasks:
      t["deal_id"] = str(deal_id)
      t["contact_id"] = str(contact_id)
    return tasks
  except requests.HTTPError:
    return []


def fetch_deal_activities(client: ACClient, deal_id: str, contact_id: str) -> List[Dict[str, Any]]:
  try:
    acts = client.get_paginated_offset("dealActivities", f"/deals/{deal_id}/dealActivities")
    for a in acts:
      a["deal_id"] = str(deal_id)
      a["contact_id"] = str(contact_id)
    return acts
  except requests.HTTPError:
    return []


def run_deals_bundle(client: ACClient, contact_ids: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  deals_rows: List[Dict[str, Any]] = []
  notes_rows: List[Dict[str, Any]] = []
  tasks_rows: List[Dict[str, Any]] = []
  acts_rows: List[Dict[str, Any]] = []

  for cid in tqdm(contact_ids, desc="Deals"):
    deals = fetch_deals_for_contact(client, cid)
    deals_rows.extend(deals)

    for d in deals:
      did = str(d.get("id", "")).strip()
      if not did:
        continue
      notes_rows.extend(fetch_deal_notes(client, did, cid))
      tasks_rows.extend(fetch_deal_tasks(client, did, cid))
      acts_rows.extend(fetch_deal_activities(client, did, cid))

    time.sleep(client.cfg.rate_sleep)

  return (
    pd.DataFrame(deals_rows),
    pd.DataFrame(notes_rows),
    pd.DataFrame(tasks_rows),
    pd.DataFrame(acts_rows),
  )


# -------------------------
# Enrichment
# -------------------------
def enrich_email_activities(
  df_email: pd.DataFrame,
  dims: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
  """
  Enriquecimiento sin perder info: agrega columnas nuevas.
  - campaign_name
  - message_subject
  - automation_id + automation_name (derivado de campaign.links.automation o seriesid)
  """
  if df_email is None or df_email.empty:
    return df_email

  df = df_email.copy()

  campaigns = dims.get("campaigns", pd.DataFrame()).copy()
  messages = dims.get("messages", pd.DataFrame()).copy()
  automations = dims.get("automations", pd.DataFrame()).copy()

  for d in (campaigns, messages, automations):
    for col in ["id", "message_id", "seriesid", "automation"]:
      if col in d.columns:
        d[col] = d[col].astype(str)

  # campaign_id column candidate
  camp_col = None
  for c in ["campaignid", "campaignId", "campaign", "campaign_id"]:
    if c in df.columns:
      camp_col = c
      break

  if camp_col and "id" in campaigns.columns:
    camp_name_map = dict(zip(campaigns["id"].astype(str), campaigns.get("name", "").astype(str)))
    df["campaign_id_norm"] = df[camp_col].astype(str)
    df["campaign_name"] = df["campaign_id_norm"].map(camp_name_map)

    # message_id from event or from campaign
    msg_id_from_event = None
    for c in ["messageid", "messageId", "message", "message_id"]:
      if c in df.columns:
        msg_id_from_event = c
        break

    if msg_id_from_event:
      df["message_id_norm"] = df[msg_id_from_event].astype(str)
    elif "message_id" in campaigns.columns:
      msg_map = dict(zip(campaigns["id"].astype(str), campaigns["message_id"].astype(str)))
      df["message_id_norm"] = df["campaign_id_norm"].map(msg_map)
    else:
      df["message_id_norm"] = ""

    # subject
    if "id" in messages.columns and "subject" in messages.columns:
      subj_map = dict(zip(messages["id"].astype(str), messages["subject"].astype(str)))
      df["message_subject"] = df["message_id_norm"].map(subj_map)
    else:
      df["message_subject"] = ""

    # automation id from campaign
    automation_id = None
    if "seriesid" in campaigns.columns:
      # en campañas de automatización, seriesid suele referenciar la automation
      df["automation_id_norm"] = df["campaign_id_norm"].map(dict(zip(campaigns["id"].astype(str), campaigns["seriesid"].astype(str))))
    else:
      df["automation_id_norm"] = ""

    # fallback: parse links.automation
    if "links" in campaigns.columns:
      # links puede venir como dict o como string; intentamos parsearlo si parece JSON
      def _get_link_automation(v: Any) -> str:
        if isinstance(v, dict):
          return extract_id_from_url(v.get("automation", ""))
        s = str(v)
        if s.startswith("{") and "automation" in s:
          try:
            obj = json.loads(s)
            if isinstance(obj, dict):
              return extract_id_from_url(obj.get("automation", ""))
          except Exception:
            return ""
        return ""
      link_map = dict(zip(campaigns["id"].astype(str), campaigns["links"].apply(_get_link_automation)))
      df.loc[df["automation_id_norm"].isin(["", "0", "None", "nan"]), "automation_id_norm"] = df["campaign_id_norm"].map(link_map)

    # automation name
    if "id" in automations.columns and "name" in automations.columns:
      aut_name_map = dict(zip(automations["id"].astype(str), automations["name"].astype(str)))
      df["automation_name"] = df["automation_id_norm"].map(aut_name_map)
    else:
      df["automation_name"] = ""

  return df


def enrich_contact_automations(
  df_ca: pd.DataFrame,
  dims: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
  if df_ca is None or df_ca.empty:
    return df_ca
  df = df_ca.copy()
  automations = dims.get("automations", pd.DataFrame()).copy()
  if "id" in automations.columns and "name" in automations.columns:
    m = dict(zip(automations["id"].astype(str), automations["name"].astype(str)))
    # campos típicos: automation o seriesid
    if "automation" in df.columns:
      df["automation_name"] = df["automation"].astype(str).map(m)
    elif "seriesid" in df.columns:
      df["automation_name"] = df["seriesid"].astype(str).map(m)
    else:
      df["automation_name"] = ""
  return df


def enrich_automation_entry_counts(
  df_aec: pd.DataFrame,
  dims: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
  if df_aec is None or df_aec.empty:
    return df_aec
  df = df_aec.copy()
  automations = dims.get("automations", pd.DataFrame()).copy()
  if "id" in automations.columns and "name" in automations.columns:
    m = dict(zip(automations["id"].astype(str), automations["name"].astype(str)))
    # en aec el id es automation_id
    if "id" in df.columns:
      df["automation_name_dim"] = df["id"].astype(str).map(m)
  return df


# -------------------------
# MART: Chatter maestro (tabla legible por contacto)
# -------------------------
def _read_best_table(output_base: str, table_name: str) -> pd.DataFrame:
  """Lee master/latest si existe, si no master/raw. Si no existe, devuelve df vacío."""
  p_latest = os.path.join(output_base, "master", "latest", f"{table_name}.csv")
  p_raw = os.path.join(output_base, "master", "raw", f"{table_name}.csv")
  if os.path.exists(p_latest):
    return pd.read_csv(p_latest, dtype=str)
  if os.path.exists(p_raw):
    return pd.read_csv(p_raw, dtype=str)
  return pd.DataFrame()


def _mk_name(first: str, last: str, fallback: str = "") -> str:
  full = f"{(first or '').strip()} {(last or '').strip()}".strip()
  return full if full else (fallback or "")


def build_chatter_master(
  *,
  output_base: str,
  run_id: str,
  dims: Dict[str, pd.DataFrame],
  contact_ids_in_run: List[str],
  max_events_per_contact: int = 2000,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """
  Construye un FACT unificado de "eventos" (chatter) y una tabla DIGEST por contacto.

  - chatter_master: 1 fila = 1 evento con columnas legibles (campaign_name, message_subject, tag_name...)
  - contact_digest: 1 fila = 1 contacto con "mensaje" resumido (sin IA, solo plantilla)

  No borra info: los RAW siguen siendo la fuente de verdad.
  """
  # --- Dim maps
  tags = dims.get("tags", pd.DataFrame())
  lists = dims.get("lists", pd.DataFrame())
  automations = dims.get("automations", pd.DataFrame())
  campaigns = dims.get("campaigns", pd.DataFrame())
  messages = dims.get("messages", pd.DataFrame())
  users = dims.get("users", pd.DataFrame())
  deal_groups = dims.get("dealGroups", pd.DataFrame())
  deal_stages = dims.get("dealStages", pd.DataFrame())
  accounts = dims.get("accounts", pd.DataFrame())
  scores = dims.get("scores", pd.DataFrame())

  tag_name_map = {}
  if not tags.empty and "id" in tags.columns:
    name_col = "tag" if "tag" in tags.columns else ("name" if "name" in tags.columns else "")
    if name_col:
      tag_name_map = dict(zip(tags["id"].astype(str), tags[name_col].astype(str)))

  list_name_map = {}
  if not lists.empty and "id" in lists.columns and "name" in lists.columns:
    list_name_map = dict(zip(lists["id"].astype(str), lists["name"].astype(str)))

  aut_name_map = {}
  if not automations.empty and "id" in automations.columns and "name" in automations.columns:
    aut_name_map = dict(zip(automations["id"].astype(str), automations["name"].astype(str)))

  camp_name_map = {}
  camp_msg_map = {}
  camp_aut_map = {}
  if not campaigns.empty and "id" in campaigns.columns:
    if "name" in campaigns.columns:
      camp_name_map = dict(zip(campaigns["id"].astype(str), campaigns["name"].astype(str)))
    if "message_id" in campaigns.columns:
      camp_msg_map = dict(zip(campaigns["id"].astype(str), campaigns["message_id"].astype(str)))
    if "seriesid" in campaigns.columns:
      camp_aut_map = dict(zip(campaigns["id"].astype(str), campaigns["seriesid"].astype(str)))
    # fallback links.automation
    if "links" in campaigns.columns:
      def _get_link_automation(v: Any) -> str:
        if isinstance(v, dict):
          return extract_id_from_url(v.get("automation", ""))
        s = str(v)
        if s.startswith("{") and "automation" in s:
          try:
            obj = json.loads(s)
            if isinstance(obj, dict):
              return extract_id_from_url(obj.get("automation", ""))
          except Exception:
            return ""
        return ""
      link_aut_map = dict(zip(campaigns["id"].astype(str), campaigns["links"].apply(_get_link_automation)))
      # solo completa donde no hay seriesid
      for k, v in link_aut_map.items():
        if k not in camp_aut_map or str(camp_aut_map.get(k, "")).strip() in ("", "0", "None", "nan"):
          camp_aut_map[k] = v

  msg_subject_map = {}
  if not messages.empty and "id" in messages.columns:
    subj_col = "subject" if "subject" in messages.columns else ("title" if "title" in messages.columns else "")
    if subj_col:
      msg_subject_map = dict(zip(messages["id"].astype(str), messages[subj_col].astype(str)))

  user_name_map = {}
  if not users.empty and "id" in users.columns:
    # nombre amigable
    fn = users.get("firstName") if "firstName" in users.columns else ""
    ln = users.get("lastName") if "lastName" in users.columns else ""
    if isinstance(fn, pd.Series) and isinstance(ln, pd.Series):
      tmp = users.copy()
      tmp["user_name"] = [
        _mk_name(str(a) if a is not None else "", str(b) if b is not None else "", fallback=str(tmp.iloc[i].get("email", "")))
        for i, (a, b) in enumerate(zip(tmp["firstName"], tmp["lastName"]))
      ]
      user_name_map = dict(zip(tmp["id"].astype(str), tmp["user_name"].astype(str)))
    elif "email" in users.columns:
      user_name_map = dict(zip(users["id"].astype(str), users["email"].astype(str)))

  stage_name_map = {}
  if not deal_stages.empty and "id" in deal_stages.columns and "title" in deal_stages.columns:
    stage_name_map = dict(zip(deal_stages["id"].astype(str), deal_stages["title"].astype(str)))

  pipeline_name_map = {}
  if not deal_groups.empty and "id" in deal_groups.columns and "title" in deal_groups.columns:
    pipeline_name_map = dict(zip(deal_groups["id"].astype(str), deal_groups["title"].astype(str)))

  account_name_map = {}
  if not accounts.empty and "id" in accounts.columns:
    nm = "name" if "name" in accounts.columns else ("account" if "account" in accounts.columns else "")
    if nm:
      account_name_map = dict(zip(accounts["id"].astype(str), accounts[nm].astype(str)))

  score_name_map = {}
  if not scores.empty and "id" in scores.columns:
    nm = "name" if "name" in scores.columns else ("title" if "title" in scores.columns else "")
    if nm:
      score_name_map = dict(zip(scores["id"].astype(str), scores[nm].astype(str)))

  # --- Base contact info
  df_contacts = _read_best_table(output_base, "contacts")
  if not df_contacts.empty and "contact_id" in df_contacts.columns:
    df_contacts["contact_id"] = df_contacts["contact_id"].astype(str)
  else:
    df_contacts = pd.DataFrame(columns=["contact_id", "email", "firstName", "lastName"])

  # --- Read fact-ish tables
  df_acts = _read_best_table(output_base, "activities")
  df_email = _read_best_table(output_base, "emailActivities_enriched")
  df_notes = _read_best_table(output_base, "contactNotes")
  df_tasks = _read_best_table(output_base, "contactTasks")
  df_logs = _read_best_table(output_base, "contactLogs")
  df_track = _read_best_table(output_base, "trackingLogs")
  df_bounce = _read_best_table(output_base, "bounceLogs")
  df_geo = _read_best_table(output_base, "geoIps")
  df_goals = _read_best_table(output_base, "contactGoals")
  df_ca = _read_best_table(output_base, "contactAutomations")
  df_deal_acts = _read_best_table(output_base, "dealActivities")
  df_deal_notes = _read_best_table(output_base, "dealNotes")
  df_deal_tasks = _read_best_table(output_base, "dealTasks")
  df_deals = _read_best_table(output_base, "deals")

  # --- Deal title map (para enriquecer eventos deal*)
  deal_title_map = {}
  deal_stage_map = {}
  deal_pipeline_map = {}
  if not df_deals.empty and "id" in df_deals.columns:
    if "title" in df_deals.columns:
      deal_title_map = dict(zip(df_deals["id"].astype(str), df_deals["title"].astype(str)))
    if "stage" in df_deals.columns:
      deal_stage_map = dict(zip(df_deals["id"].astype(str), df_deals["stage"].astype(str)))
    if "group" in df_deals.columns:
      deal_pipeline_map = dict(zip(df_deals["id"].astype(str), df_deals["group"].astype(str)))

  # --- Helpers
  def _pick_ts(row: pd.Series, candidates: List[str]) -> str:
    for c in candidates:
      if c in row and pd.notna(row[c]) and str(row[c]).strip():
        return str(row[c]).strip()
    return ""

  def _as_event_rows(df_src: pd.DataFrame, source: str) -> List[Dict[str, Any]]:
    if df_src is None or df_src.empty:
      return []

    out: List[Dict[str, Any]] = []
    # solo eventos de los contactos procesados en esta corrida (para el delta), pero master completo se arma con todos
    df_use = df_src.copy()
    if "contact_id" in df_use.columns:
      df_use["contact_id"] = df_use["contact_id"].astype(str)
    else:
      df_use["contact_id"] = ""

    # límite por contacto para no explotar memoria (tomamos los eventos mas recientes por timestamp disponible)
    ts_candidates = ["tstamp", "udate", "cdate", "lastdate", "mdate", "duedate", "created_timestamp", "updated_timestamp"]
    ts_col = next((c for c in ts_candidates if c in df_use.columns), None)
    if "contact_id" in df_use.columns and ts_col:
      try:
        df_use["_ts_sort"] = pd.to_datetime(df_use[ts_col], errors="coerce", utc=True)
        df_use = df_use.sort_values(by=["contact_id", "_ts_sort"], ascending=[True, False], kind="mergesort")
        df_use = df_use.groupby("contact_id").head(max_events_per_contact).reset_index(drop=True)
      finally:
        if "_ts_sort" in df_use.columns:
          df_use = df_use.drop(columns=["_ts_sort"], errors="ignore")

    for _, r in df_use.iterrows():
      cid = str(r.get("contact_id", "")).strip()
      rid = str(r.get("id", "")).strip() or str(r.get("_row_hash", "")).strip()

      evt = {
        "event_id": f"{source}:{rid}" if rid else f"{source}:{hashlib.md5(str(dict(r)).encode('utf-8')).hexdigest()}",
        "contact_id": cid,
        "source": source,
        "source_row_id": rid,
        "event_ts": "",
        "event_type": "",
        "title": "",
        "detail": "",
        "campaign_id": "",
        "campaign_name": "",
        "message_id": "",
        "message_subject": "",
        "automation_id": "",
        "automation_name": "",
        "list_id": "",
        "list_name": "",
        "tag_id": "",
        "tag_name": "",
        "deal_id": "",
        "deal_title": "",
        "deal_stage_id": "",
        "deal_stage_name": "",
        "pipeline_id": "",
        "pipeline_name": "",
        "user_id": "",
        "user_name": "",
        "url": "",
      }

      # ---- Source-specific mappings ----
      if source == "emailActivities_enriched":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate", "udate"])
        evt["event_type"] = str(r.get("type", "email"))
        evt["campaign_id"] = str(r.get("campaignid", r.get("campaign_id_norm", "")))
        evt["campaign_name"] = str(r.get("campaign_name", "")) or camp_name_map.get(evt["campaign_id"], "")
        evt["message_id"] = str(r.get("message_id_norm", r.get("messageid", "")))
        if not evt["message_id"] and evt["campaign_id"]:
          evt["message_id"] = camp_msg_map.get(evt["campaign_id"], "")
        evt["message_subject"] = str(r.get("message_subject", "")) or msg_subject_map.get(evt["message_id"], "")
        evt["automation_id"] = str(r.get("automation_id_norm", "")) or camp_aut_map.get(evt["campaign_id"], "")
        evt["automation_name"] = str(r.get("automation_name", "")) or aut_name_map.get(evt["automation_id"], "")
        evt["title"] = f"Email {evt['event_type']}: {evt['message_subject']}".strip(": ")
        evt["detail"] = f"Campaign: {evt['campaign_name']} | Automation: {evt['automation_name']}".strip()

      elif source == "activities":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate"])
        ref_type = str(r.get("reference_type", r.get("referenceType", "")))
        ref_action = str(r.get("reference_action", r.get("referenceAction", "")))
        ref_id = str(r.get("reference_id", r.get("referenceId", "")))
        evt["event_type"] = f"{ref_type}:{ref_action}".strip(":")
        # intentos de traducción
        if ref_type.lower().startswith("campaign"):
          evt["campaign_id"] = ref_id
          evt["campaign_name"] = camp_name_map.get(ref_id, "")
          evt["title"] = f"Campaign {ref_action}: {evt['campaign_name']}".strip(": ")
        elif ref_type.lower().startswith("message"):
          evt["message_id"] = ref_id
          evt["message_subject"] = msg_subject_map.get(ref_id, "")
          evt["title"] = f"Message {ref_action}: {evt['message_subject']}".strip(": ")
        elif ref_type.lower().startswith("automation"):
          evt["automation_id"] = ref_id
          evt["automation_name"] = aut_name_map.get(ref_id, "")
          evt["title"] = f"Automation {ref_action}: {evt['automation_name']}".strip(": ")
        else:
          evt["title"] = f"Activity {evt['event_type']}".strip()
        evt["detail"] = str(r.get("description", r.get("text", "")))
        uid = str(r.get("user", r.get("userid", "")))
        evt["user_id"] = uid
        evt["user_name"] = user_name_map.get(uid, "")

      elif source == "contactNotes":
        evt["event_ts"] = _pick_ts(r, ["cdate", "udate"])
        evt["event_type"] = "note"
        evt["title"] = "Nota" 
        evt["detail"] = str(r.get("note", r.get("text", "")))
        uid = str(r.get("userid", r.get("user", "")))
        evt["user_id"] = uid
        evt["user_name"] = user_name_map.get(uid, "")

      elif source == "contactTasks":
        evt["event_ts"] = _pick_ts(r, ["udate", "cdate", "duedate"])
        evt["event_type"] = "task"
        evt["title"] = str(r.get("title", "Tarea"))
        evt["detail"] = str(r.get("note", r.get("description", "")))
        uid = str(r.get("userid", r.get("user", "")))
        evt["user_id"] = uid
        evt["user_name"] = user_name_map.get(uid, "")

      elif source == "contactLogs":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate"])
        evt["event_type"] = "log"
        evt["title"] = str(r.get("action", "Log"))
        evt["detail"] = str(r.get("message", r.get("description", "")))

      elif source == "trackingLogs":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate"])
        evt["event_type"] = "visit"
        evt["url"] = str(r.get("url", r.get("link", "")))
        evt["title"] = f"Visita: {evt['url']}".strip()

      elif source == "bounceLogs":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate"])
        evt["event_type"] = "bounce"
        evt["title"] = "Bounce"
        evt["detail"] = str(r.get("reason", r.get("message", "")))

      elif source == "geoIps":
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate"])
        evt["event_type"] = "geo_ip"
        evt["title"] = f"IP: {str(r.get('ip', ''))}".strip()
        evt["detail"] = str(r.get("country", ""))

      elif source == "contactGoals":
        evt["event_ts"] = _pick_ts(r, ["cdate", "tstamp"])
        evt["event_type"] = "goal"
        evt["title"] = str(r.get("name", r.get("goal", "Goal")))

      elif source == "contactAutomations":
        evt["event_ts"] = _pick_ts(r, ["lastdate", "adddate", "cdate"])
        evt["event_type"] = "automation_status"
        aid = str(r.get("automation", r.get("seriesid", "")))
        evt["automation_id"] = aid
        evt["automation_name"] = str(r.get("automation_name", "")) or aut_name_map.get(aid, "")
        evt["title"] = f"Automation: {evt['automation_name']}"
        evt["detail"] = f"status={str(r.get('status', ''))}"

      elif source == "dealActivities":
        evt["event_ts"] = _pick_ts(r, ["cdate", "tstamp"])
        evt["event_type"] = "deal_activity"
        did = str(r.get("deal_id", r.get("d_id", "")))
        evt["deal_id"] = did
        evt["deal_title"] = deal_title_map.get(did, "")
        stid = str(r.get("d_stageid", "")) or deal_stage_map.get(did, "")
        evt["deal_stage_id"] = stid
        evt["deal_stage_name"] = stage_name_map.get(stid, "")
        gid = str(r.get("d_groupid", "")) or deal_pipeline_map.get(did, "")
        evt["pipeline_id"] = gid
        evt["pipeline_name"] = pipeline_name_map.get(gid, "")
        evt["title"] = f"Deal: {evt['deal_title']}"
        evt["detail"] = f"stage={evt['deal_stage_name']} action={str(r.get('dataAction',''))}"
        uid = str(r.get("userid", ""))
        evt["user_id"] = uid
        evt["user_name"] = user_name_map.get(uid, "")

      elif source == "dealNotes":
        evt["event_ts"] = _pick_ts(r, ["cdate", "udate"])
        evt["event_type"] = "deal_note"
        did = str(r.get("deal_id", ""))
        evt["deal_id"] = did
        evt["deal_title"] = deal_title_map.get(did, "")
        evt["title"] = f"Nota deal: {evt['deal_title']}"
        evt["detail"] = str(r.get("note", r.get("text", "")))

      elif source == "dealTasks":
        evt["event_ts"] = _pick_ts(r, ["udate", "cdate", "duedate"])
        evt["event_type"] = "deal_task"
        did = str(r.get("deal_id", ""))
        evt["deal_id"] = did
        evt["deal_title"] = deal_title_map.get(did, "")
        evt["title"] = f"Tarea deal: {evt['deal_title']}"
        evt["detail"] = str(r.get("title", ""))

      else:
        evt["event_ts"] = _pick_ts(r, ["tstamp", "cdate", "udate"])
        evt["event_type"] = source
        evt["title"] = source

      out.append(evt)
    return out

  # --- Build events
  events: List[Dict[str, Any]] = []
  events.extend(_as_event_rows(df_email, "emailActivities_enriched"))
  events.extend(_as_event_rows(df_acts, "activities"))
  events.extend(_as_event_rows(df_notes, "contactNotes"))
  events.extend(_as_event_rows(df_tasks, "contactTasks"))
  events.extend(_as_event_rows(df_logs, "contactLogs"))
  events.extend(_as_event_rows(df_track, "trackingLogs"))
  events.extend(_as_event_rows(df_bounce, "bounceLogs"))
  events.extend(_as_event_rows(df_geo, "geoIps"))
  events.extend(_as_event_rows(df_goals, "contactGoals"))
  events.extend(_as_event_rows(df_ca, "contactAutomations"))
  events.extend(_as_event_rows(df_deal_acts, "dealActivities"))
  events.extend(_as_event_rows(df_deal_notes, "dealNotes"))
  events.extend(_as_event_rows(df_deal_tasks, "dealTasks"))

  df_events = pd.DataFrame(events)
  if not df_events.empty:
    df_events["contact_id"] = df_events["contact_id"].astype(str)
    # merge contacto
    if not df_contacts.empty:
      keep_cols = [c for c in ["contact_id", "email", "firstName", "lastName", "phone"] if c in df_contacts.columns]
      df_events = df_events.merge(df_contacts[keep_cols], on="contact_id", how="left")
      if "firstName" in df_events.columns or "lastName" in df_events.columns:
        df_events["contact_name"] = [
          _mk_name(str(a) if a is not None else "", str(b) if b is not None else "", fallback=str(df_events.iloc[i].get("email", "")))
          for i, (a, b) in enumerate(zip(df_events.get("firstName", pd.Series([""]*len(df_events))), df_events.get("lastName", pd.Series([""]*len(df_events)))))
        ]
    # --- Normaliza timestamps y orden cronologico (local)
    if "event_ts" in df_events.columns:
      # UTC naive para ordenar de forma consistente
      df_events["event_time_utc"] = pd.to_datetime(df_events["event_ts"], errors="coerce", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
      # Hora local para lectura humana (LOCAL_TZ)
      df_events["event_time_local"] = pd.to_datetime(df_events["event_ts"], errors="coerce", utc=True).dt.tz_convert("LOCAL_TZ")
      df_events["event_time_local_str"] = df_events["event_time_local"].dt.strftime("%Y-%m-%d %H:%M:%S %z")
    else:
      df_events["event_time_utc"] = pd.NaT
      df_events["event_time_local"] = pd.NaT
      df_events["event_time_local_str"] = ""

    # Linea de chatter lista para leer (sin perder columnas base)
    def _mk_chatter_line(row: pd.Series) -> str:
      ts = str(row.get("event_time_local_str", "")).strip()
      title = str(row.get("title", "")).strip()
      detail = str(row.get("detail", "")).strip()
      url = str(row.get("url", "")).strip()
      who = str(row.get("user_name", "")).strip()
      who_txt = f" ({who})" if who else ""
      parts = [p for p in [title + who_txt, detail, url] if p and p != 'nan']
      body = " — ".join(parts)
      return f"{ts} — {body}".strip(" —") if ts else body

    df_events["chatter_line"] = df_events.apply(_mk_chatter_line, axis=1)

    # Orden cronologico: de mas antiguo a mas reciente (dentro de cada contacto)
    df_events = df_events.sort_values(by=["contact_id", "event_time_utc", "event_id"], ascending=[True, True, True], kind="mergesort", na_position="last")

  # --- Build digest (texto simple)
  # latest relations
  df_ct = _read_best_table(output_base, "contactTags")
  if not df_ct.empty:
    df_ct["tag_name"] = df_ct.get("tag", "").astype(str).map(tag_name_map) if "tag" in df_ct.columns else ""
  df_cl = _read_best_table(output_base, "contactLists")
  if not df_cl.empty:
    df_cl["list_name"] = df_cl.get("list", "").astype(str).map(list_name_map) if "list" in df_cl.columns else ""

  df_digest_rows: List[Dict[str, Any]] = []
  contact_set = set(contact_ids_in_run)
  # si incremental_only -> digest para esos contactos, si no -> para seed+new
  digest_ids = sorted(contact_set) if contact_set else sorted(df_events["contact_id"].unique().tolist()) if not df_events.empty else []

  for cid in digest_ids:
    info = df_contacts[df_contacts["contact_id"].astype(str) == str(cid)] if (not df_contacts.empty and "contact_id" in df_contacts.columns) else pd.DataFrame()
    email = str(info.iloc[0].get("email", "")) if not info.empty else ""
    name = _mk_name(str(info.iloc[0].get("firstName", "")) if not info.empty else "", str(info.iloc[0].get("lastName", "")) if not info.empty else "", fallback=email)

    # tags y listas actuales
    tags_list = []
    if not df_ct.empty:
      tmp = df_ct[df_ct.get("contact_id", "").astype(str) == str(cid)]
      # preferimos tag_name, si no existe, tag id
      if "tag_name" in tmp.columns:
        tags_list = [t for t in tmp["tag_name"].astype(str).tolist() if t and t != 'nan']
      elif "tag" in tmp.columns:
        tags_list = [t for t in tmp["tag"].astype(str).tolist() if t and t != 'nan']
      tags_list = sorted(set(tags_list))

    lists_list = []
    if not df_cl.empty:
      tmp = df_cl[df_cl.get("contact_id", "").astype(str) == str(cid)]
      if "list_name" in tmp.columns:
        lists_list = [t for t in tmp["list_name"].astype(str).tolist() if t and t != 'nan']
      elif "list" in tmp.columns:
        lists_list = [t for t in tmp["list"].astype(str).tolist() if t and t != 'nan']
      lists_list = sorted(set(lists_list))

    # automatizaciones actuales
    autos_list = []
    if not df_ca.empty:
      tmp = df_ca[df_ca.get("contact_id", "").astype(str) == str(cid)]
      if "automation_name" in tmp.columns:
        autos_list = [t for t in tmp["automation_name"].astype(str).tolist() if t and t != 'nan']
      elif "automation" in tmp.columns:
        autos_list = [aut_name_map.get(str(a), str(a)) for a in tmp["automation"].astype(str).tolist()]
      autos_list = sorted(set(autos_list))

    # últimos eventos
    lines: List[str] = []
    if not df_events.empty:
      ev = df_events[df_events["contact_id"].astype(str) == str(cid)].tail(25)
      for _, er in ev.iterrows():
        ts = str(er.get("event_ts", ""))
        ttl = str(er.get("title", ""))
        det = str(er.get("detail", ""))
        lines.append(f"- {ts} | {ttl}" + (f" | {det}" if det else ""))
    message_lines = [
      f"Contacto: {name} ({email})".strip(),
      f"Tags: {', '.join(tags_list) if tags_list else '-'}",
      f"Lists: {', '.join(lists_list) if lists_list else '-'}",
      f"Automations: {', '.join(autos_list) if autos_list else '-'}",
      "Últimos eventos:",
    ]
    message_lines.extend(lines if lines else ["- (sin eventos)"])
    message = "\n".join(message_lines)


    df_digest_rows.append({
      "contact_id": str(cid),
      "contact_name": name,
      "email": email,
      "tags": ", ".join(tags_list),
      "lists": ", ".join(lists_list),
      "automations": ", ".join(autos_list),
      "digest": message,
    })

  df_digest = pd.DataFrame(df_digest_rows)
  return df_events, df_digest


# -------------------------
# Contacts table
# -------------------------
def run_contacts_table(seed_resolved: pd.DataFrame, new_contacts: List[Dict[str, Any]]) -> pd.DataFrame:
  df_new = pd.DataFrame(new_contacts) if new_contacts else pd.DataFrame()
  df_seed = seed_resolved.copy() if seed_resolved is not None else pd.DataFrame(columns=["contact_id", "email"])
  if not df_new.empty and "id" in df_new.columns:
    df_new = df_new.rename(columns={"id": "contact_id"})
  if "contact_id" not in df_new.columns:
    df_new["contact_id"] = ""
  df_new["contact_id"] = df_new["contact_id"].astype(str)

  # merge básico para tener email del seed si el endpoint no lo trae
  if not df_seed.empty:
    df_seed["contact_id"] = df_seed["contact_id"].astype(str)
    df_out = pd.merge(df_new, df_seed[["contact_id", "email"]], on="contact_id", how="left", suffixes=("", "_seed"))
    if "email" in df_out.columns and "email_seed" in df_out.columns:
      df_out["email"] = df_out["email"].fillna("") # type: ignore
      df_out.loc[df_out["email"].astype(str).str.strip().eq(""), "email"] = df_out["email_seed"]
      df_out = df_out.drop(columns=["email_seed"])
    return df_out
  return df_new


# -------------------------
# Orchestration
# -------------------------
def run_pipeline(
  *,
  base_url: str,
  api_token: str,
  output_base: str,
  seed_csv: Optional[str] = None,
  incremental_only: bool = False,
  use_email_fallback: bool = True,
  sample_size: Optional[int] = None,
  refresh_dims_days: int = 7,
) -> None:
  ensure_dir(output_base)

  cfg = ACConfig(base_url=base_url, api_token=api_token)
  client = ACClient(cfg)

  run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
  ensure_dir(os.path.join(output_base, "runs", run_id, "delta"))

  # Estado incremental (solo para nuevos contactos)
  state_path = os.path.join(output_base, "state.json")
  state = load_json(state_path, {"max_contact_id": 0, "last_run_utc": None})
  max_from_state = safe_int(state.get("max_contact_id", 0))

  # Seed
  seed_resolved = pd.DataFrame(columns=["contact_id", "email"])
  seed_ids: List[str] = []
  max_from_seed = 0
  if seed_csv:
    seed_resolved = resolve_seed_contact_ids(client, seed_csv, use_email_fallback=use_email_fallback)
    seed_ids = seed_resolved["contact_id"].astype(str).tolist() if not seed_resolved.empty else []
    max_from_seed = max((safe_int(x) for x in seed_ids), default=0)

  # Nuevos contactos
  start_from = max(max_from_state, max_from_seed)
  new_contacts = client.get_contacts_after_id(start_from)
  new_ids = [str(c.get("id", "")).strip() for c in (new_contacts or []) if str(c.get("id", "")).strip()]

  # Universo
  if incremental_only:
    contact_ids = new_ids
  else:
    contact_ids = sorted(set(seed_ids + new_ids), key=lambda x: safe_int(x))
  contact_ids = [c for c in contact_ids if c]

  # Sample (debug)
  if sample_size and sample_size > 0 and len(contact_ids) > sample_size:
    contact_ids = contact_ids[:sample_size]

  print(f"[Run] run_id={run_id}")
  print(f"[Incremental] start_from={start_from} | nuevos={len(new_ids)} | total_a_procesar={len(contact_ids)}")

  # Dims (cache)
  dims = load_all_dims(client, output_base, refresh_days=refresh_dims_days)

  # Contacts table
  df_contacts = run_contacts_table(seed_resolved, new_contacts)
  if not df_contacts.empty and "contact_id" in df_contacts.columns:
    persist_table_dual(output_base=output_base, run_id=run_id, table_name="contacts", df_new=df_contacts, key_cols_latest=["contact_id"], updated_at_col="udate" if "udate" in df_contacts.columns else None)

  if not contact_ids:
    # actualiza estado aunque no haya nuevos (para registrar corrida)
    state["last_run_utc"] = utc_now_iso()
    save_json(state_path, state)
    print("[Done] No hay contactos para procesar.")
    return

  # Chatter
  df_acts = run_activities(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="activities", df_new=df_acts, key_cols_latest=["id"] if "id" in df_acts.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_acts.columns else None)

  df_email = run_email_activities(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="emailActivities", df_new=df_email, key_cols_latest=["id"] if "id" in df_email.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_email.columns else None)

  df_email_en = enrich_email_activities(df_email, dims)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="emailActivities_enriched", df_new=df_email_en, key_cols_latest=["id"] if "id" in df_email_en.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_email_en.columns else None)

  df_notes = run_contact_notes(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactNotes", df_new=df_notes, key_cols_latest=["id"] if "id" in df_notes.columns else ["contact_id", "_row_hash"], updated_at_col="cdate" if "cdate" in df_notes.columns else None)

  df_cl = run_contact_lists(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactLists", df_new=df_cl, key_cols_latest=["id"] if "id" in df_cl.columns else ["contact_id", "list"], updated_at_col="udate" if "udate" in df_cl.columns else None)

  df_ct = run_contact_tags(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactTags", df_new=df_ct, key_cols_latest=["id"] if "id" in df_ct.columns else ["contact_id", "tag"], updated_at_col="cdate" if "cdate" in df_ct.columns else None)

  df_logs = run_contact_logs(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactLogs", df_new=df_logs, key_cols_latest=["id"] if "id" in df_logs.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_logs.columns else None)

  df_track = run_tracking_logs(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="trackingLogs", df_new=df_track, key_cols_latest=["id"] if "id" in df_track.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_track.columns else None)

  df_bounce = run_bounce_logs(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="bounceLogs", df_new=df_bounce, key_cols_latest=["id"] if "id" in df_bounce.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_bounce.columns else None)

  df_geo = run_geo_ips(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="geoIps", df_new=df_geo, key_cols_latest=["id"] if "id" in df_geo.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_geo.columns else None)

  df_goals = run_contact_goals(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactGoals", df_new=df_goals, key_cols_latest=["id"] if "id" in df_goals.columns else ["contact_id", "_row_hash"], updated_at_col="cdate" if "cdate" in df_goals.columns else None)

  df_cdata = run_contact_data(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactData", df_new=df_cdata, key_cols_latest=["id"] if "id" in df_cdata.columns else ["contact_id", "_row_hash"], updated_at_col="updated_timestamp" if "updated_timestamp" in df_cdata.columns else None)

  df_scores = run_score_values(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="scoreValues", df_new=df_scores, key_cols_latest=["id"] if "id" in df_scores.columns else ["contact_id", "_row_hash"], updated_at_col="tstamp" if "tstamp" in df_scores.columns else None)

  df_accts = run_account_contacts(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="accountContacts", df_new=df_accts, key_cols_latest=["id"] if "id" in df_accts.columns else ["contact_id", "_row_hash"], updated_at_col="cdate" if "cdate" in df_accts.columns else None)

  df_ctasks = run_contact_tasks(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactTasks", df_new=df_ctasks, key_cols_latest=["id"] if "id" in df_ctasks.columns else ["contact_id", "_row_hash"], updated_at_col="udate" if "udate" in df_ctasks.columns else None)

  # Automations (NEW)
  df_ca = run_contact_automations(client, contact_ids)
  df_ca_en = enrich_contact_automations(df_ca, dims)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="contactAutomations", df_new=df_ca_en, key_cols_latest=["id"] if "id" in df_ca_en.columns else ["contact_id", "automation"], updated_at_col="lastdate" if "lastdate" in df_ca_en.columns else None)

  df_aec = run_automation_entry_counts(client, contact_ids)
  df_aec_en = enrich_automation_entry_counts(df_aec, dims)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="automationEntryCounts", df_new=df_aec_en, key_cols_latest=["contact_id", "id"], updated_at_col=None)

  # Deals bundle
  df_deals, df_deal_notes, df_deal_tasks, df_deal_acts = run_deals_bundle(client, contact_ids)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="deals", df_new=df_deals, key_cols_latest=["id"] if "id" in df_deals.columns else ["contact_id", "_row_hash"], updated_at_col="mdate" if "mdate" in df_deals.columns else None)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="dealNotes", df_new=df_deal_notes, key_cols_latest=["id"] if "id" in df_deal_notes.columns else ["deal_id", "_row_hash"], updated_at_col="cdate" if "cdate" in df_deal_notes.columns else None)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="dealTasks", df_new=df_deal_tasks, key_cols_latest=["id"] if "id" in df_deal_tasks.columns else ["deal_id", "_row_hash"], updated_at_col="udate" if "udate" in df_deal_tasks.columns else None)
  persist_table_dual(output_base=output_base, run_id=run_id, table_name="dealActivities", df_new=df_deal_acts, key_cols_latest=["id"] if "id" in df_deal_acts.columns else ["deal_id", "_row_hash"], updated_at_col="cdate" if "cdate" in df_deal_acts.columns else None)

  # MART: archivo maestro legible (chatter unificado + digest por contacto)
  try:
    df_chatter, df_digest = build_chatter_master(
      output_base=output_base,
      run_id=run_id,
      dims=dims,
      contact_ids_in_run=contact_ids,
      max_events_per_contact=500,
    )
    mart_dir = os.path.join(output_base, "master", "mart")
    ensure_dir(mart_dir)
    if df_chatter is not None and not df_chatter.empty:
      write_csv_utf8sig(os.path.join(mart_dir, "chatter_master.csv"), df_chatter)
      # delta del run
      write_csv_utf8sig(os.path.join(output_base, "runs", run_id, "delta", "chatter_master.csv"), df_chatter[df_chatter["contact_id"].astype(str).isin(set(contact_ids))])
    if df_digest is not None and not df_digest.empty:
      write_csv_utf8sig(os.path.join(mart_dir, "contact_digest.csv"), df_digest)
      write_csv_utf8sig(os.path.join(output_base, "runs", run_id, "delta", "contact_digest.csv"), df_digest)
      # (opcional) 1 TXT por contacto
      txt_dir = os.path.join(mart_dir, "contact_digest_txt")
      ensure_dir(txt_dir)
      for _, r in df_digest.iterrows():
        cid = str(r.get("contact_id", ""))
        msg = str(r.get("digest", ""))
        if cid and msg:
          with open(os.path.join(txt_dir, f"contact_{cid}.txt"), "w", encoding="utf-8") as f:
            f.write(msg)
  except Exception as e:
    print(f"[Warn] No se pudo construir chatter_master/digest: {e}")

  # Update state (max contact id visto)
  new_max = max([safe_int(x) for x in (seed_ids + new_ids)] or [max_from_state])
  state["max_contact_id"] = max(new_max, max_from_state)
  state["last_run_utc"] = utc_now_iso()
  save_json(state_path, state)

  print("[Done] Export terminado.")
  print(f"Salida: {output_base}")