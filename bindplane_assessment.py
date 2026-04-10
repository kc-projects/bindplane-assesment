#!/usr/bin/env python3
"""
Bindplane Platform Assessment Script v2
Reporte ejecutivo completo con HTML interactivo.
"""

import os, json, csv, sys, math
from datetime import datetime, timezone
from typing import Any
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Cargar .env
# ──────────────────────────────────────────────────────────────────────────────
_env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

BASE_URL   = os.environ.get("BINDPLANE_URL", "https://app.bindplane.com").rstrip("/")
API_KEY    = os.environ.get("BINDPLANE_API_KEY", "")
API_PREFIX = "/v1"

SESSION = requests.Session()
SESSION.headers.update({
    "X-Bindplane-Api-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
})

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
def get(path: str, params: dict | None = None, silent: bool = False) -> Any:
    url = f"{BASE_URL}{API_PREFIX}{path}"
    try:
        r = SESSION.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if r.content else {}
    except requests.HTTPError as e:
        if not silent:
            print(f"  [WARN] GET {path} → HTTP {e.response.status_code}: {e.response.text[:200]}")
        return None
    except ValueError:
        if not silent:
            print(f"  [WARN] GET {path} → respuesta no-JSON")
        return None
    except requests.RequestException as e:
        if not silent:
            print(f"  [ERROR] GET {path} → {e}")
        return None


def _detect_prefix() -> str:
    for prefix in ("/v1", ""):
        try:
            r = SESSION.get(f"{BASE_URL}{prefix}/agents", timeout=10)
            if r.status_code in (200, 401, 403):
                return prefix
        except Exception:
            pass
    return "/v1"


def _unpack(data: Any, *keys: str) -> list:
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for k in keys:
        if k in data:
            v = data[k]
            return v if isinstance(v, list) else []
    return []

# ──────────────────────────────────────────────────────────────────────────────
# Status maps
# ──────────────────────────────────────────────────────────────────────────────
AGENT_STATUS = {
    0: "unknown",
    1: "connected",
    2: "disconnected",
    3: "component_failed",
    4: "deleted",
    5: "upgrading",
    6: "error",
    7: "configuring",
    8: "not_configured",
}

# Status que indican problema
AGENT_ERROR_STATUSES = {"disconnected", "component_failed", "error",
                        "deleted", "not_configured"}

def agent_status(a: dict) -> str:
    raw = a.get("status", a.get("state", 0))
    if isinstance(raw, int):
        return AGENT_STATUS.get(raw, f"status_{raw}")
    return str(raw).lower()


def agent_has_error(a: dict) -> bool:
    st = agent_status(a)
    return st in AGENT_ERROR_STATUSES or st.startswith("status_")

# ──────────────────────────────────────────────────────────────────────────────
# Colección de datos
# ──────────────────────────────────────────────────────────────────────────────
def collect_agents_summary() -> list[dict]:
    print("  → Agentes (lista)...")
    all_agents = []
    page_size  = 500
    offset     = 0
    while True:
        data = get("/agents", params={"pageSize": page_size, "offset": offset})
        batch = _unpack(data, "agents", "items")
        if not batch:
            break
        all_agents.extend(batch)
        # Log status raw para detectar códigos desconocidos
        for a in batch:
            raw_st = a.get("status", "?")
            if isinstance(raw_st, int) and raw_st not in AGENT_STATUS:
                print(f"  [INFO] Status desconocido detectado: {raw_st} en agente {a.get('name', a.get('id','?'))}")
        if len(batch) < page_size:
            break
        offset += page_size
    print(f"  → Total agentes recuperados: {len(all_agents)}")
    return all_agents


def collect_agent_detail(agent_id: str) -> dict:
    d = get(f"/agents/{agent_id}", silent=True)
    if d and "agent" in d:
        return d["agent"]
    return d or {}


def collect_agents_full(agents: list[dict]) -> list[dict]:
    print(f"  → Detalle de {len(agents)} agente(s)...")
    result = []
    for a in agents:
        aid = a.get("id", "")
        detail = collect_agent_detail(aid) if aid else {}
        merged = {**a, **detail}
        result.append(merged)
    return result


def collect_configurations() -> list[dict]:
    print("  → Configuraciones...")
    configs = _unpack(get("/configurations"), "configurations", "items")
    detailed = []
    for cfg in configs:
        name = cfg.get("metadata", {}).get("name") or cfg.get("name", "")
        if name:
            d = get(f"/configurations/{name}", silent=True)
            if d:
                d = d.get("configuration", d)
            detailed.append(d if d else cfg)
        else:
            detailed.append(cfg)
    return detailed


def collect_destinations() -> list[dict]:
    print("  → Destinations...")
    return _unpack(get("/destinations"), "destinations", "items")


def collect_agent_versions() -> list[dict]:
    print("  → Versiones de agente...")
    return _unpack(get("/agent-versions"), "agentVersions", "items")


def collect_fleets() -> list[dict]:
    print("  → Fleets...")
    return _unpack(get("/fleets"), "fleets", "items") or []


def collect_notifications() -> list[dict]:
    print("  → Notificaciones...")
    return _unpack(get("/api/notifications", silent=True), "notifications", "items") or []


# ──────────────────────────────────────────────────────────────────────────────
# Throughput / métricas desde componentes del agente
# ──────────────────────────────────────────────────────────────────────────────
def _extract_agent_metrics(agent: dict) -> dict:
    """
    Intenta extraer bytes enviados/recibidos del detalle del agente.
    Bindplane reporta métricas de componentes en campos como:
      agent.components[].metrics / agent.metrics / agent.status.metrics
    Devuelve {"bytes_sent": int, "bytes_received": int, "logs_sent": int, "metrics_sent": int}
    """
    totals = {"bytes_sent": 0, "bytes_received": 0, "logs_sent": 0,
              "metrics_sent": 0, "traces_sent": 0, "has_data": False}

    def _scan(obj):
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            kl = k.lower()
            if isinstance(v, (int, float)) and v > 0:
                if any(x in kl for x in ("byte", "size")):
                    if "sent" in kl or "output" in kl or "export" in kl:
                        totals["bytes_sent"] += int(v)
                        totals["has_data"] = True
                    elif "recv" in kl or "input" in kl or "receive" in kl:
                        totals["bytes_received"] += int(v)
                        totals["has_data"] = True
                if "log" in kl and ("sent" in kl or "export" in kl):
                    totals["logs_sent"] += int(v)
                    totals["has_data"] = True
                if "metric" in kl and ("sent" in kl or "export" in kl):
                    totals["metrics_sent"] += int(v)
                    totals["has_data"] = True
                if "trace" in kl and ("sent" in kl or "export" in kl):
                    totals["traces_sent"] += int(v)
                    totals["has_data"] = True
            elif isinstance(v, dict):
                _scan(v)
            elif isinstance(v, list):
                for item in v:
                    _scan(item)

    _scan(agent)
    return totals


def _fmt_bytes(b: int) -> str:
    if b == 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


# ──────────────────────────────────────────────────────────────────────────────
# Análisis de configuraciones (detalle completo)
# ──────────────────────────────────────────────────────────────────────────────
def _extract_params(params: list) -> list[dict]:
    result = []
    for p in (params or []):
        if not isinstance(p, dict):
            continue
        v = p.get("value", "")
        if isinstance(v, (dict, list)):
            v = json.dumps(v)
        result.append({"name": p.get("name", ""), "value": str(v)[:200]})
    return result


def _extract_bundles(comp: dict) -> list[dict]:
    bundles = []
    for b in comp.get("bundles", []):
        bundles.append({"name": b.get("name","?"), "type": b.get("type", b.get("kind","?")),
                        "parameters": _extract_params(b.get("parameters",[]))})
    spec = comp.get("spec", {})
    for b in spec.get("bundles", spec.get("processors", spec.get("components", []))):
        if isinstance(b, dict):
            bundles.append({"name": b.get("name","?"), "type": b.get("type", b.get("kind","?")),
                            "parameters": _extract_params(b.get("parameters",[]))})
    for param in (comp.get("parameters") or []):
        if isinstance(param.get("value"), list):
            for item in param["value"]:
                if isinstance(item, dict) and ("type" in item or "kind" in item):
                    bundles.append({"name": item.get("name", param.get("name","?")),
                                    "type": item.get("type", item.get("kind","?")),
                                    "parameters": _extract_params(item.get("parameters",[]))})
    return bundles


def _clean_type(t: str) -> str:
    """Elimina el sufijo de versión ':N' del tipo. ej: windowsevents_v2:5 → windowsevents_v2"""
    return t.rsplit(":", 1)[0] if t and ":" in t else (t or "?")


def _extract_log_types_raw(params: list) -> list[str]:
    """
    Extrae logTypes desde los params RAW (antes del truncado de _extract_params).
    Busca en el valor original, que puede ser una lista de dicts con 'logType'.
    """
    log_types = []
    for p in (params or []):
        if not isinstance(p, dict):
            continue
        name = (p.get("name") or "").lower()
        val  = p.get("value", "")

        # campo directo
        if name in ("logtype", "log_type"):
            s = str(val).strip()
            if s and s not in log_types:
                log_types.append(s)
            continue

        # valor ya es lista/dict (sin truncar)
        raw = val if isinstance(val, (list, dict)) else None
        if raw is None and isinstance(val, str):
            try:
                raw = json.loads(val)
            except Exception:
                pass

        items = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
        for item in items:
            if isinstance(item, dict) and "logType" in item:
                lt = str(item["logType"]).strip()
                if lt and lt not in log_types:
                    log_types.append(lt)
    return log_types


def _logtype_from_proc_name(pname: str) -> str | None:
    """
    Extrae el logType desde el nombre de un procesador referenciado.
    Patrón: 'Log_Type_WINEVTLOG:2' → 'WINEVTLOG'
    """
    clean = pname.rsplit(":", 1)[0] if ":" in pname else pname
    if clean.startswith("Log_Type_"):
        return clean[len("Log_Type_"):]
    return None


def _parse_processor(p: dict) -> dict:
    # Bindplane usa 'displayName' como nombre visible del procesador, no 'name'
    display = (p.get("displayName") or p.get("name") or p.get("id") or "?")
    # Extraer logTypes ANTES de que _extract_params trunce los valores
    log_types = _extract_log_types_raw(p.get("parameters", []))
    # Fallback: referenced processors (displayName=None, Params=0) llevan el logType en su 'name'
    # e.g. "Log_Type_WINEVTLOG:2" → logType "WINEVTLOG"
    if not log_types:
        pname = (p.get("name") or "").strip()
        lt = _logtype_from_proc_name(pname)
        if lt:
            log_types.append(lt)
    return {
        "name":      display,
        "type":      _clean_type(p.get("type", p.get("kind", "?"))),
        "parameters":_extract_params(p.get("parameters", [])),
        "bundles":   _extract_bundles(p),
        "log_types": log_types,
    }


def _source_display_name(s: dict) -> str:
    """Sources no tienen 'name' en Bindplane — usamos type limpio + id corto."""
    t = _clean_type(s.get("type", s.get("kind", "")))
    sid = s.get("id", "")
    short_id = sid[-6:] if sid else ""
    return f"{t}" + (f" [{short_id}]" if short_id else "")


def _source_routes(s: dict) -> list[str]:
    """Extrae los destinos desde spec.sources[].routes."""
    routes = s.get("routes", {})
    dests = set()
    for _signal, route_list in routes.items():
        for route in (route_list or []):
            for comp in route.get("components", []):
                # formato: "destinations/d-SecOps-KC"
                dests.add(comp.split("/")[-1])
    return sorted(dests)


def extract_config_detail(cfg: dict) -> dict:
    spec = cfg.get("spec", {})
    meta = cfg.get("metadata", {})
    name = meta.get("name", cfg.get("name", "?"))

    sources = []
    for s in spec.get("sources", []):
        routes    = _source_routes(s)
        raw_procs = s.get("processors", [])
        parsed_procs = [_parse_processor(p) for p in raw_procs]

        # LogTypes del source: vienen del processor de standardización (raw, sin truncar)
        src_log_types = []
        for p in raw_procs:
            for lt in _extract_log_types_raw(p.get("parameters", [])):
                if lt not in src_log_types:
                    src_log_types.append(lt)
        # Fallback: referenced processors llevan el logType codificado en 'name'
        # e.g. "Log_Type_WINEVTLOG:2" → "WINEVTLOG"
        if not src_log_types:
            for p in raw_procs:
                # prefer displayName, then name
                pname = (p.get("name") or "").strip()
                lt = _logtype_from_proc_name(pname)
                if lt and lt not in src_log_types:
                    src_log_types.append(lt)
                    continue
                # Last resort: use displayName as-is
                dn = (p.get("displayName") or "").strip()
                if dn and dn not in src_log_types:
                    src_log_types.append(dn)

        sources.append({
            "name":       _source_display_name(s),
            "type":       _clean_type(s.get("type", s.get("kind", "?"))),
            "parameters": _extract_params(s.get("parameters", [])),
            "processors": parsed_procs,
            "routes":     routes,
            "log_types":  src_log_types,
        })

    processors = [_parse_processor(p) for p in spec.get("processors", [])]

    destinations = []
    for d in spec.get("destinations", []):
        # destinations en spec tienen 'name' con el valor real (ej: "SecOps-KC:6")
        dest_name = _clean_type(d.get("name") or d.get("id") or "?")
        dest_type = _clean_type(d.get("type", d.get("kind", "")))
        destinations.append({
            "name": dest_name,
            "type": dest_type or "—",
        })

    # complexity score: fuentes + todos los procesadores (top-level + inline) + bundles + destinations
    inline_procs = [p for s in sources for p in s.get("processors", [])]
    all_procs    = processors + inline_procs
    bundle_count = sum(len(p.get("bundles", [])) for p in all_procs)
    complexity   = len(sources) + len(all_procs) + bundle_count + len(destinations)

    return {
        "name":          name,
        "agent_count":   cfg.get("agentCount") or cfg.get("matchingAgents") or 0,
        "rollout_status":str(cfg.get("status",{}).get("rollout",{}).get("status","—")),
        "sources":       sources,
        "processors":    processors,
        "destinations":  destinations,
        "pipelines":     spec.get("pipelines",[]),
        "complexity_score": complexity,
        "labels":        meta.get("labels", {}),
        "last_modified": meta.get("dateModified", ""),
        "version":       meta.get("version", ""),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Inventario de LogTypes
# ──────────────────────────────────────────────────────────────────────────────
def build_logtype_inventory(configs_d: list[dict]) -> list[dict]:
    """
    Construye inventario de LogTypes leyendo el campo 'log_types'
    pre-extraído en cada source (set en extract_config_detail).
    """
    inventory = []
    seen = set()
    for cd in configs_d:
        dest_names = ", ".join(d["name"] for d in cd.get("destinations", [])) or "—"
        for s in cd.get("sources", []):
            # LogTypes del source (extraídos raw en extract_config_detail)
            log_types = list(s.get("log_types", []))
            # También de processors inline
            for p in s.get("processors", []):
                for lt in p.get("log_types", []):
                    if lt not in log_types:
                        log_types.append(lt)
            # Route-level destination override
            route_dest = ", ".join(s.get("routes", [])) or dest_names
            for lt in log_types:
                key = lt + "|" + cd["name"]
                if key in seen:
                    continue
                seen.add(key)
                inventory.append({
                    "logtype":     lt,
                    "config":      cd["name"],
                    "source_type": s["type"],
                    "destination": route_dest,
                })
    return inventory


# ──────────────────────────────────────────────────────────────────────────────
# Análisis global
# ──────────────────────────────────────────────────────────────────────────────
def analyze_agents(agents: list[dict]) -> dict:
    by_status: dict[str,int] = {}
    by_version: dict[str,int] = {}
    disconnected, outdated, component_failed = [], [], []
    total_bytes_sent = total_bytes_recv = 0
    config_throughput: dict[str,int] = {}  # config_name → bytes_sent

    versions_seen = []
    for a in agents:
        st = agent_status(a)
        by_status[st] = by_status.get(st,0) + 1
        ver = a.get("version") or a.get("agentVersion") or "unknown"
        by_version[ver] = by_version.get(ver,0) + 1
        versions_seen.append(ver)

        if st in ("disconnected", "deleted") or "offline" in st:
            disconnected.append({"id": a.get("id","?"), "name": a.get("name","?"), "status": st})
        if st in ("component_failed", "error") or st.startswith("status_"):
            component_failed.append({"id": a.get("id","?"), "name": a.get("name","?"), "status": st})

        mx = _extract_agent_metrics(a)
        total_bytes_sent += mx["bytes_sent"]
        total_bytes_recv += mx["bytes_received"]

        # Asociar throughput a la config del agente
        cfg_label = (a.get("labels") or {}).get("configuration","")
        if cfg_label:
            config_throughput[cfg_label] = config_throughput.get(cfg_label,0) + mx["bytes_sent"]

    unique_ver = {v for v in versions_seen if v != "unknown"}
    latest = "unknown"
    if len(unique_ver) > 1:
        try:
            latest = sorted(unique_ver,
                key=lambda v: [int(x) for x in v.lstrip("v").split(".")], reverse=True)[0]
            for a in agents:
                ver = a.get("version") or a.get("agentVersion") or "unknown"
                if ver not in ("unknown", latest):
                    outdated.append({"id": a.get("id","?"), "name": a.get("name","?"), "version": ver})
        except Exception:
            pass
    elif unique_ver:
        latest = list(unique_ver)[0]

    return {
        "total": len(agents),
        "by_status": by_status,
        "by_version": by_version,
        "disconnected": disconnected,
        "component_failed": component_failed,
        "outdated": outdated,
        "latest_version": latest,
        "total_bytes_sent": total_bytes_sent,
        "total_bytes_recv": total_bytes_recv,
        "config_throughput": config_throughput,
    }


def analyze_destinations(dests: list[dict]) -> dict:
    by_type: dict[str,int] = {}
    for d in dests:
        t = str(d.get("spec",{}).get("type") or d.get("type") or "unknown")
        by_type[t] = by_type.get(t,0) + 1
    return {"total": len(dests), "by_type": by_type}


def build_findings(agents_a: dict, configs_d: list[dict], dests_a: dict) -> list[dict]:
    findings = []

    if agents_a["disconnected"]:
        findings.append({"severity":"HIGH","category":"Agents",
            "finding":f"{len(agents_a['disconnected'])} agente(s) desconectado(s).",
            "detail":[x["name"] or x["id"] for x in agents_a["disconnected"]]})

    if agents_a["component_failed"]:
        findings.append({"severity":"HIGH","category":"Agents",
            "finding":f"{len(agents_a['component_failed'])} agente(s) con componente en falla.",
            "detail":[x["name"] or x["id"] for x in agents_a["component_failed"]]})

    if agents_a["outdated"]:
        findings.append({"severity":"MEDIUM","category":"Agents",
            "finding":f"{len(agents_a['outdated'])} agente(s) desactualizados (última: {agents_a['latest_version']}).",
            "detail":[f"{x['name']} ({x['version']})" for x in agents_a["outdated"]]})

    no_agents = [c["name"] for c in configs_d if c["agent_count"] == 0]
    if no_agents:
        findings.append({"severity":"LOW","category":"Configurations",
            "finding":f"{len(no_agents)} configuración(es) sin agentes asignados.",
            "detail": no_agents})

    high_complexity = [c for c in configs_d if c["complexity_score"] >= 10]
    if high_complexity:
        findings.append({"severity":"LOW","category":"Configurations",
            "finding":f"{len(high_complexity)} configuración(es) de alta complejidad (score ≥ 10).",
            "detail":[f"{c['name']} (score {c['complexity_score']})" for c in high_complexity]})

    if dests_a["total"] == 0:
        findings.append({"severity":"MEDIUM","category":"Destinations",
            "finding":"No hay destinations configurados.","detail":[]})

    configs_no_dest = [c["name"] for c in configs_d if not c["destinations"]]
    if configs_no_dest:
        findings.append({"severity":"MEDIUM","category":"Configurations",
            "finding":f"{len(configs_no_dest)} configuración(es) sin destination.",
            "detail": configs_no_dest})

    configs_no_src = [c["name"] for c in configs_d if not c["sources"]]
    if configs_no_src:
        findings.append({"severity":"LOW","category":"Configurations",
            "finding":f"{len(configs_no_src)} configuración(es) sin sources.",
            "detail": configs_no_src})

    if not findings:
        findings.append({"severity":"INFO","category":"General",
            "finding":"No se detectaron problemas. Plataforma en buen estado.","detail":[]})

    return sorted(findings, key=lambda x: {"HIGH":0,"MEDIUM":1,"LOW":2,"INFO":3}.get(x["severity"],9))


def build_report(raw: dict) -> dict:
    agents_full   = raw["agents_full"]
    configs       = raw["configurations"]
    dests         = raw["destinations"]

    agents_a  = analyze_agents(agents_full)
    dests_a   = analyze_destinations(dests)
    configs_d = [extract_config_detail(c) for c in configs]

    # Contar agentes reales por config usando el label "configuration" del agente
    agent_count_by_cfg: dict[str, int] = {}
    for ag in agents_full:
        cfg_label = (ag.get("labels") or {}).get("configuration", "")
        if cfg_label:
            agent_count_by_cfg[cfg_label] = agent_count_by_cfg.get(cfg_label, 0) + 1

    # Enriquecer configs con throughput y agent_count real
    ct = agents_a["config_throughput"]
    for cd in configs_d:
        cd["bytes_sent"]  = ct.get(cd["name"], 0)
        cd["agent_count"] = agent_count_by_cfg.get(cd["name"], cd["agent_count"])

    findings          = build_findings(agents_a, configs_d, dests_a)
    logtype_inventory = build_logtype_inventory(configs_d)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bindplane_url": BASE_URL,
        "summary": {
            "total_agents":         agents_a["total"],
            "total_configurations": len(configs),
            "total_destinations":   dests_a["total"],
            "total_sources":        sum(len(c["sources"]) for c in configs_d),
            "total_fleets":         len(raw["fleets"] or []),
            "total_bytes_sent":     agents_a["total_bytes_sent"],
            "total_bytes_recv":     agents_a["total_bytes_recv"],
        },
        "agents":             agents_a,
        "configurations":     {"total": len(configs)},
        "destinations":       dests_a,
        "configs_detail":     configs_d,
        "findings":           findings,
        "logtype_inventory":  logtype_inventory,
        "agent_versions":     [v.get("tag", v.get("version", v.get("name", str(v))))
                               for v in (raw["agent_versions"] or [])],
        "notifications":      (raw["notifications"] or [])[:10],
        "raw_agents":         agents_full,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Terminal output
# ──────────────────────────────────────────────────────────────────────────────
def print_report(r: dict) -> None:
    sep = "=" * 70
    s = r["summary"]
    a = r["agents"]
    print(f"\n{sep}")
    print("  BINDPLANE PLATFORM ASSESSMENT")
    print(f"  {r['generated_at'][:19].replace('T',' ')} UTC  |  {r['bindplane_url']}")
    print(sep)
    print(f"\n  Agentes        : {s['total_agents']}   "
          f"Configs: {s['total_configurations']}   "
          f"Destinations: {s['total_destinations']}   "
          f"Fleets: {s['total_fleets']}")
    print(f"  Bytes enviados : {_fmt_bytes(s['total_bytes_sent'])}   "
          f"Recibidos: {_fmt_bytes(s['total_bytes_recv'])}")
    print(f"\n  Agentes por estado : {a['by_status']}")
    print(f"  Versión más nueva  : {a['latest_version']}")
    if a["disconnected"]:
        print(f"  Desconectados      : {len(a['disconnected'])}")

    print(f"\n{'-'*70}  FINDINGS")
    for f in r["findings"]:
        icon = {"HIGH":"[!!]","MEDIUM":"[! ]","LOW":"[. ]","INFO":"[i ]"}.get(f["severity"],"    ")
        print(f"  {icon} {f['severity']:<6} | {f['category']:<15} | {f['finding']}")
    print(sep)


# ──────────────────────────────────────────────────────────────────────────────
# Exportes
# ──────────────────────────────────────────────────────────────────────────────
def save_json(r: dict, path: str) -> None:
    # Excluir raw_agents del JSON del reporte (está en crudo separado)
    export = {k: v for k, v in r.items() if k != "raw_agents"}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2, ensure_ascii=False)
    print(f"  [+] JSON: {path}")


def save_raw_json(raw: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2, ensure_ascii=False)
    print(f"  [+] RAW JSON: {path}")


def save_csv(r: dict, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["severity","category","finding","detail"])
        w.writeheader()
        for fi in r["findings"]:
            w.writerow({"severity":fi["severity"],"category":fi["category"],
                        "finding":fi["finding"],"detail":"; ".join(fi["detail"][:10])})
    print(f"  [+] CSV: {path}")


# ──────────────────────────────────────────────────────────────────────────────
# HTML helpers
# ──────────────────────────────────────────────────────────────────────────────
SEV_COLOR = {"HIGH":"#e53e3e","MEDIUM":"#dd6b20","LOW":"#d69e2e","INFO":"#3182ce"}
SEV_BG    = {"HIGH":"#fff5f5","MEDIUM":"#fffaf0","LOW":"#fffff0","INFO":"#ebf8ff"}

STATUS_STYLE = {
    "connected":        "background:#c6f6d5;color:#276749",
    "disconnected":     "background:#fed7d7;color:#9b2c2c",
    "component_failed": "background:#feebc8;color:#7b341e",
    "error":            "background:#fed7d7;color:#9b2c2c",
    "upgrading":        "background:#bee3f8;color:#2a4365",
    "configuring":      "background:#bee3f8;color:#2a4365",
    "deleted":          "background:#e2e8f0;color:#4a5568",
    "not_configured":   "background:#fef3c7;color:#92400e",
    "unknown":          "background:#e2e8f0;color:#718096",
}

def _badge(text: str, style: str = "background:#e2e8f0;color:#4a5568") -> str:
    return (f'<span style="{style};padding:2px 10px;border-radius:12px;'
            f'font-size:12px;font-weight:600;white-space:nowrap">{text}</span>')


def _status_badge(st: str) -> str:
    # status_N desconocido → rojo para que sea visible
    if st.startswith("status_"):
        style = "background:#fed7d7;color:#9b2c2c"
    else:
        style = STATUS_STYLE.get(st, "background:#e2e8f0;color:#718096")
    return _badge(st, style)


def _sev_badge(sev: str) -> str:
    return _badge(sev, f"background:{SEV_BG.get(sev,'#eee')};color:{SEV_COLOR.get(sev,'#333')};border:1px solid {SEV_COLOR.get(sev,'#ccc')}")


def _chart_colors(n: int) -> list[str]:
    palette = ["#4299e1","#48bb78","#ed8936","#e53e3e","#9f7aea",
               "#38b2ac","#f6ad55","#fc8181","#68d391","#63b3ed"]
    return [palette[i % len(palette)] for i in range(n)]


# ── Extracción inteligente de params por tipo de componente ─────────────────

def _extract_log_types(params: list) -> list[str]:
    """Extrae logType desde params directos o desde JSON anidado (ej: secops_field)."""
    log_types = []
    for p in (params or []):
        if not isinstance(p, dict):
            continue
        name = (p.get("name") or "").lower()
        val  = p.get("value", "")
        # campo directo
        if name in ("logtype", "log_type"):
            s = str(val).strip()
            if s and s not in log_types:
                log_types.append(s)
            continue
        # JSON anidado (secops_field u otros arrays)
        raw = val if isinstance(val, (list, dict)) else None
        if raw is None and isinstance(val, str):
            try:
                raw = json.loads(val)
            except Exception:
                pass
        items = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
        for item in items:
            if isinstance(item, dict) and "logType" in item:
                lt = str(item["logType"]).strip()
                if lt and lt not in log_types:
                    log_types.append(lt)
    return log_types


def _smart_params(comp_type: str, params: list) -> list[dict]:
    """
    Devuelve SOLO el dato más relevante por componente. Máximo 2 items.
      - secops/standardization  → logType
      - filter/regex            → patrón resumido (≤80 chars)
      - transform               → "OTTL statements"
      - batch                   → timeout + batch_size
      - memory_limiter          → limit_mib
      - cualquier otro          → nada (solo nombre+tipo ya alcanza)
    """
    if not params:
        return []

    ct = (comp_type or "").lower()
    result = []

    # ── 1. logType: siempre primero si existe ─────────────────────────────
    log_types = _extract_log_types(params)
    if log_types:
        result.append({"label": "logType", "value": " · ".join(log_types), "style": "logtype"})
        return result   # con logType es suficiente

    # ── 2. Por tipo de componente, UN solo dato clave ─────────────────────
    def _first_val(names: list) -> str | None:
        for p in params:
            if not isinstance(p, dict):
                continue
            pn = (p.get("name") or "").lower()
            if any(n in pn for n in names):
                v = p.get("value", "")
                if v in ("", None, [], {}):
                    continue
                if isinstance(v, (list, dict)):
                    try:
                        v = json.dumps(v, ensure_ascii=False)
                    except Exception:
                        v = str(v)
                return str(v)[:80].rstrip() + ("…" if len(str(v)) > 80 else "")
        return None

    if any(k in ct for k in ("filter", "regex", "grep")):
        v = _first_val(["regex", "pattern", "expr", "match", "include", "exclude", "body"])
        if v:
            result.append({"label": "filtro", "value": v, "style": "regex"})

    elif any(k in ct for k in ("transform", "ottl")):
        result.append({"label": "tipo", "value": "OTTL transform", "style": "value"})

    elif "batch" in ct:
        v = _first_val(["timeout"])
        if v:
            result.append({"label": "timeout", "value": v, "style": "value"})

    elif "memory" in ct:
        v = _first_val(["limit_mib", "limit_percentage"])
        if v:
            result.append({"label": "límite", "value": v + " MiB", "style": "value"})

    # Para cualquier otro tipo: no mostramos nada extra (nombre+tipo es suficiente)
    return result


# ── Renderizado HTML ─────────────────────────────────────────────────────────

_STYLE_MAP = {
    "logtype": "background:#fef3c7;color:#92400e;border:1px solid #f59e0b",
    "regex":   "background:#e9d8fd;color:#553c9a;border:1px solid #9f7aea",
    "key":     "background:#e2e8f0;color:#4a5568;border:1px solid #cbd5e0",
    "value":   "background:#ebf8ff;color:#2c5282;border:1px solid #90cdf4",
}


def _render_params_items(items: list) -> str:
    if not items:
        return ""
    rows = ""
    for item in items:
        style = _STYLE_MAP.get(item["style"], _STYLE_MAP["value"])
        icon  = {"logtype": "📋", "regex": "🔍", "key": "🔑", "value": "·"}.get(item["style"], "·")
        rows += (
            f'<div style="display:flex;align-items:baseline;gap:6px;margin-top:4px;flex-wrap:wrap">'
            f'<span style="font-size:10px;color:#a0aec0;white-space:nowrap;min-width:70px">{item["label"]}</span>'
            f'<span style="{style};border-radius:5px;padding:1px 8px;font-size:11px;font-weight:600;'
            f'word-break:break-all">{icon} {item["value"]}</span>'
            f'</div>'
        )
    return f'<div style="margin-top:6px;padding-left:4px">{rows}</div>'


def _params_html(comp_type: str, params: list) -> str:
    return _render_params_items(_smart_params(comp_type, params))


def _bundle_pills(bundles: list) -> str:
    if not bundles:
        return ""
    pills = "".join(
        f'<span style="background:#faf5ff;color:#553c9a;border:1px solid #d6bcfa;'
        f'border-radius:6px;padding:2px 8px;font-size:11px;font-weight:600;margin:2px">'
        f'⬡ {b.get("name","bundle")} <span style="opacity:.6;font-weight:400">({b.get("type","?")})</span></span>'
        for b in bundles
    )
    return f'<div style="margin-top:4px;display:flex;flex-wrap:wrap;gap:2px">{pills}</div>'


def _proc_block(procs: list, border_color: str = "#63b3ed", bg: str = "#ebf8ff",
                text_color: str = "#2c5282") -> str:
    if not procs:
        return '<span style="font-size:12px;color:#a0aec0;font-style:italic">ninguno</span>'
    html = ""
    for p in procs:
        # Usar log_types pre-extraídos en lugar de re-parsear params truncados
        lt_items = [{"label": "logType", "value": " · ".join(p["log_types"]), "style": "logtype"}] \
                   if p.get("log_types") else []
        # Para otros params (filter, batch, etc.) seguir usando _smart_params pero sin logtype
        other_items = [i for i in _smart_params(p.get("type", ""), p.get("parameters", []))
                       if i["style"] != "logtype"]
        all_items = lt_items + other_items
        ph = _render_params_items(all_items)
        html += (
            f'<div style="background:{bg};border-left:3px solid {border_color};'
            f'border-radius:6px;padding:8px 12px;margin-bottom:6px">'
            f'<div style="font-size:13px;font-weight:600;color:{text_color}">⚙ {p["name"]}'
            f'<span style="font-weight:400;color:#718096;font-size:11px"> — {p["type"]}</span></div>'
            f'{ph}'
            f'{_bundle_pills(p.get("bundles",[]))}'
            f'</div>'
        )
    return html


def _source_block(sources: list) -> str:
    if not sources:
        return '<span style="font-size:12px;color:#a0aec0;font-style:italic">ninguno</span>'
    html = ""
    for s in sources:
        procs    = s.get("processors", [])
        routes   = s.get("routes", [])

        # ── Processors con su displayName ──────────────────────────────────
        procs_html = ""
        if procs:
            inner = _proc_block(procs, "#68d391", "#f0fff4", "#276749")
            procs_html = (
                f'<div style="margin-top:10px;padding-left:10px;border-left:2px dashed #9ae6b4">'
                f'<div style="font-size:10px;font-weight:700;color:#48bb78;letter-spacing:.07em;'
                f'margin-bottom:6px">⚙ PROCESSORS ({len(procs)})</div>'
                f'{inner}</div>'
            )

        # ── Routes / destinos del source ───────────────────────────────────
        routes_html = ""
        if routes:
            chips = "".join(
                f'<span style="background:#feebc8;color:#7b341e;border-radius:6px;'
                f'padding:2px 8px;font-size:11px;font-weight:600;margin:2px">→ {r}</span>'
                for r in routes
            )
            routes_html = (
                f'<div style="margin-top:6px;display:flex;flex-wrap:wrap;align-items:center;gap:4px">'
                f'<span style="font-size:10px;color:#a0aec0">envía a:</span>{chips}</div>'
            )

        html += (
            f'<div style="background:#f0fff4;border-left:4px solid #48bb78;'
            f'border-radius:8px;padding:10px 14px;margin-bottom:10px">'
            # Header: tipo del source
            f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
            f'<span style="font-size:14px;font-weight:700;color:#276749">▶ {s["type"]}</span>'
            f'<span style="background:#c6f6d5;color:#276749;border-radius:10px;padding:1px 8px;'
            f'font-size:11px">{len(procs)} processor(s)</span>'
            f'</div>'
            f'{routes_html}'
            f'{procs_html}'
            f'</div>'
        )
    return html


def _pipeline_flow(cfg: dict) -> str:
    """Genera un diagrama visual de flujo Source → Processor → Destination."""
    nodes = []
    for s in cfg.get("sources", []):
        # Mostrar inline processors del source en el flow
        procs = s.get("processors", [])
        proc_label = " · ".join(p["name"] for p in procs) if procs else ""
        label = s["type"] + (f"\n[{proc_label}]" if proc_label else "")
        nodes.append(("source", s["type"], proc_label))
    for p in cfg.get("processors", []):
        nodes.append(("processor", p["name"], p["type"]))
    for d in cfg.get("destinations", []):
        nodes.append(("destination", d["name"], d["type"]))

    if not nodes:
        return ""

    colors = {"source": ("#c6f6d5","#276749","#48bb78"),
              "processor": ("#bee3f8","#2a4365","#4299e1"),
              "destination": ("#feebc8","#7b341e","#ed8936")}
    icons  = {"source":"▶","processor":"⚙","destination":"◼"}

    items = ""
    for i, (kind, name, typ) in enumerate(nodes):
        bg, fg, border = colors[kind]
        icon = icons[kind]
        # typ = proc label cuando es source, o tipo cuando es processor/destination
        sub = f'<div style="font-size:10px;color:#718096;margin-top:2px">{typ}</div>' if typ else ""
        items += (
            f'<div style="display:flex;align-items:center;gap:6px">'
            + (f'<div style="color:#cbd5e0;font-size:20px;font-weight:300">→</div>' if i > 0 else '')
            + f'<div style="background:{bg};border:2px solid {border};border-radius:8px;'
              f'padding:8px 14px;text-align:center;min-width:120px;max-width:180px">'
              f'<div style="font-size:9px;color:{fg};font-weight:700;text-transform:uppercase;letter-spacing:.06em">{icon} {kind}</div>'
              f'<div style="font-size:12px;font-weight:700;color:#2d3748;margin-top:3px;word-break:break-word">{name}</div>'
              f'{sub}'
              f'</div></div>'
        )
    return f'<div style="display:flex;align-items:center;flex-wrap:wrap;gap:4px;padding:12px;background:#f7fafc;border-radius:8px;margin-bottom:12px">{items}</div>'


def _complexity_bar(score: int) -> str:
    pct = min(100, score * 8)
    color = "#e53e3e" if score >= 10 else "#dd6b20" if score >= 6 else "#48bb78"
    return (f'<div style="display:flex;align-items:center;gap:8px">'
            f'<div style="flex:1;background:#e2e8f0;border-radius:4px;height:8px">'
            f'<div style="width:{pct}%;background:{color};height:8px;border-radius:4px"></div></div>'
            f'<span style="font-size:12px;color:{color};font-weight:700">{score}</span></div>')


# ──────────────────────────────────────────────────────────────────────────────
# Generación HTML principal
# ──────────────────────────────────────────────────────────────────────────────
def save_html(report: dict, raw: dict, path: str) -> None:
    s          = report["summary"]
    a          = report["agents"]
    d          = report["destinations"]
    findings   = report["findings"]
    configs_d  = report["configs_detail"]
    agents_raw = report.get("raw_agents", [])

    ts = report["generated_at"][:19].replace("T", " ")

    # ── Chart.js data ────────────────────────────────────────────────────────
    agent_status_labels = json.dumps(list(a["by_status"].keys()))
    agent_status_data   = json.dumps(list(a["by_status"].values()))
    agent_status_colors = json.dumps(_chart_colors(len(a["by_status"])))

    dest_labels = json.dumps(list(d["by_type"].keys()))
    dest_data   = json.dumps(list(d["by_type"].values()))
    dest_colors = json.dumps(_chart_colors(len(d["by_type"])))

    ver_labels = json.dumps(list(a["by_version"].keys())[:10])
    ver_data   = json.dumps(list(a["by_version"].values())[:10])
    ver_colors = json.dumps(_chart_colors(len(a["by_version"])))

    # Config throughput chart
    ct = {k: v for k, v in (a.get("config_throughput") or {}).items() if v > 0}
    ct_labels = json.dumps(list(ct.keys()))
    ct_data   = json.dumps(list(ct.values()))
    ct_colors = json.dumps(_chart_colors(len(ct)))

    # Severity count for donut
    sev_counts = {"HIGH":0,"MEDIUM":0,"LOW":0,"INFO":0}
    for f in findings:
        sev_counts[f["severity"]] = sev_counts.get(f["severity"],0) + 1
    sev_labels = json.dumps(list(sev_counts.keys()))
    sev_data   = json.dumps(list(sev_counts.values()))
    sev_colors = json.dumps(["#e53e3e","#dd6b20","#d69e2e","#3182ce"])

    # ── KPI cards ─────────────────────────────────────────────────────────────
    kpis = [
        ("Agentes",         s["total_agents"],         "#4299e1", "🖥"),
        ("Configuraciones", s["total_configurations"],  "#48bb78", "⚙"),
        ("Destinations",    s["total_destinations"],    "#ed8936", "📤"),
        ("Sources",         s["total_sources"],         "#9f7aea", "📥"),
        ("Fleets",          s["total_fleets"],          "#38b2ac", "🚀"),
        ("Bytes Enviados",  _fmt_bytes(s["total_bytes_sent"]),  "#e53e3e", "📊"),
    ]
    kpi_html = "".join(
        f'<div class="kpi-card" style="border-left:4px solid {c}">'
        f'<div class="kpi-icon">{icon}</div>'
        f'<div><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div></div></div>'
        for label, value, c, icon in kpis
    )

    # ── Findings table ────────────────────────────────────────────────────────
    finding_rows = "".join(
        f'<tr style="background:{SEV_BG.get(f["severity"],"#fff")}">'
        f'<td class="td">{_sev_badge(f["severity"])}</td>'
        f'<td class="td" style="color:#718096">{f["category"]}</td>'
        f'<td class="td">{f["finding"]}</td>'
        f'<td class="td" style="font-size:12px;color:#718096">'
        f'{"<br>".join(str(x) for x in f["detail"][:5])}'
        f'{"..." if len(f["detail"])>5 else ""}</td>'
        f'</tr>'
        for f in findings
    )

    # ── Agents table ──────────────────────────────────────────────────────────
    agent_rows = ""
    for ag in agents_raw[:300]:
        mx   = _extract_agent_metrics(ag)
        st   = agent_status(ag)
        ver  = ag.get("version") or ag.get("agentVersion") or "—"
        cfg  = (ag.get("labels") or {}).get("configuration","—")
        name = ag.get("name", ag.get("id","?"))
        bs   = _fmt_bytes(mx["bytes_sent"]) if mx["has_data"] else "—"
        br   = _fmt_bytes(mx["bytes_received"]) if mx["has_data"] else "—"
        agent_rows += (
            f'<tr>'
            f'<td class="td" style="font-weight:500">{name}</td>'
            f'<td class="td">{_status_badge(st)}</td>'
            f'<td class="td" style="font-size:12px">{ver}</td>'
            f'<td class="td" style="font-size:12px">{cfg}</td>'
            f'<td class="td" style="font-size:12px;text-align:right">{bs}</td>'
            f'<td class="td" style="font-size:12px;text-align:right">{br}</td>'
            f'</tr>'
        )
    if not agent_rows:
        agent_rows = '<tr><td colspan="6" class="empty">Sin agentes</td></tr>'

    # ── Configurations cards ──────────────────────────────────────────────────
    config_cards = ""
    for cd in configs_d:
        cid       = cd["name"].replace(" ","_").replace("/","_")
        flow_html = _pipeline_flow(cd)
        src_html  = _source_block(cd.get("sources",[]))
        proc_html = _proc_block(cd.get("processors",[]))
        bs        = _fmt_bytes(cd.get("bytes_sent",0))

        dest_chips = "".join(
            f'<span style="background:#feebc8;color:#7b341e;border-radius:8px;'
            f'padding:3px 10px;font-size:12px;margin:2px;display:inline-block">'
            f'◼ {dd["name"]} <span style="opacity:.6">({dd["type"]})</span></span>'
            for dd in cd.get("destinations",[])
        ) or '<span style="color:#a0aec0;font-size:12px">ninguno</span>'

        rollout_color = "#e53e3e" if cd["rollout_status"] in ("failed","error") else "#718096"

        config_cards += f"""
<div class="cfg-card">
  <div class="cfg-header" onclick="toggleCfg('{cid}')">
    <div style="display:flex;flex-direction:column;gap:4px">
      <span style="font-weight:700;font-size:15px">{cd['name']}</span>
      <span style="font-size:12px;color:#718096">
        {len(cd.get('sources',[]))} source(s) &nbsp;·&nbsp;
        {len(cd.get('processors',[])) + sum(len(s.get('processors',[])) for s in cd.get('sources',[]))} processor(s) &nbsp;·&nbsp;
        {len(cd.get('destinations',[]))} destination(s)
      </span>
    </div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      {_badge(f"{cd['agent_count']} agente(s)", "background:#c6f6d5;color:#276749")}
      {_badge(f"rollout: {cd['rollout_status']}", f"background:#e2e8f0;color:{rollout_color}")}
      {_badge(f"GB: {bs}", "background:#bee3f8;color:#2a4365") if bs != '—' else ''}
      <div style="color:#a0aec0;font-size:18px" id="arr-{cid}">▼</div>
    </div>
  </div>
  <div id="cfg-{cid}" style="display:none;padding:18px">

    <!-- Complexity -->
    <div style="margin-bottom:14px">
      <div style="font-size:11px;color:#718096;margin-bottom:4px">Complejidad de configuración</div>
      {_complexity_bar(cd['complexity_score'])}
    </div>

    <!-- Pipeline flow -->
    <div style="font-size:11px;font-weight:700;color:#718096;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Pipeline</div>
    {flow_html}

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:10px">
      <div>
        <div class="section-label">Sources</div>
        {src_html}
      </div>
      <div>
        <div class="section-label">Processors (top-level)</div>
        {proc_html}
        <div class="section-label" style="margin-top:14px">Destinations</div>
        <div style="display:flex;flex-wrap:wrap;gap:4px">{dest_chips}</div>
      </div>
    </div>
  </div>
</div>"""

    # ── LogType inventory ─────────────────────────────────────────────────────
    lt_inv = report.get("logtype_inventory", [])
    lt_rows = ""
    for row in lt_inv:
        lt_rows += (
            f'<tr>'
            f'<td class="td"><span style="background:#bee3f8;color:#2a4365;border-radius:5px;'
            f'padding:2px 8px;font-size:12px;font-weight:700">📋 {row["logtype"]}</span></td>'
            f'<td class="td" style="font-size:12px">{row["config"]}</td>'
            f'<td class="td" style="font-size:12px;color:#718096">{row["source_type"]}</td>'
            f'<td class="td" style="font-size:12px">'
            f'<span style="background:#feebc8;color:#7b341e;border-radius:5px;padding:1px 7px;font-size:11px">'
            f'→ {row["destination"]}</span></td>'
            f'</tr>'
        )

    # ── Raw JSON viewer ───────────────────────────────────────────────────────
    export_data = {k:v for k,v in report.items() if k != "raw_agents"}
    raw_json_str = json.dumps(export_data, indent=2, ensure_ascii=False)

    # ── HTML completo ─────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Bindplane Assessment — {ts}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#edf2f7;color:#2d3748;font-size:14px}}

  /* Navbar */
  .navbar{{background:linear-gradient(135deg,#1a202c,#2d3748);color:white;padding:16px 32px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 2px 8px rgba(0,0,0,.3)}}
  .navbar-title{{font-size:20px;font-weight:700;letter-spacing:-.3px}}
  .navbar-sub{{font-size:12px;color:#a0aec0;margin-top:3px}}
  .navbar-logo{{font-size:28px}}

  /* Layout */
  .container{{max-width:1280px;margin:28px auto;padding:0 24px}}

  /* KPI cards */
  .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:24px}}
  .kpi-card{{background:white;border-radius:12px;padding:16px 20px;box-shadow:0 1px 3px rgba(0,0,0,.08);display:flex;align-items:center;gap:14px}}
  .kpi-icon{{font-size:26px}}
  .kpi-label{{font-size:12px;color:#718096;font-weight:500}}
  .kpi-value{{font-size:28px;font-weight:800;color:#2d3748;line-height:1.1}}

  /* Panel */
  .panel{{background:white;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:24px;overflow:hidden}}
  .panel-header{{padding:16px 22px;border-bottom:1px solid #edf2f7;display:flex;align-items:center;justify-content:space-between}}
  .panel-title{{font-size:15px;font-weight:700;color:#2d3748}}
  .panel-body{{padding:20px 22px}}

  /* Tabs */
  .tabs{{display:flex;gap:3px;padding:14px 16px 0;background:#f7fafc;border-bottom:1px solid #e2e8f0;flex-wrap:wrap}}
  .tab{{padding:9px 20px;border-radius:8px 8px 0 0;cursor:pointer;font-size:13px;font-weight:600;border:none;background:transparent;color:#718096;transition:.15s;border-bottom:2px solid transparent}}
  .tab:hover{{color:#2d3748;background:#edf2f7}}
  .tab.active{{color:#4299e1;border-bottom:2px solid #4299e1;background:white}}
  .tab-content{{display:none;padding:22px}}.tab-content.active{{display:block}}

  /* Tables */
  table{{width:100%;border-collapse:collapse}}
  th{{text-align:left;padding:10px 14px;font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:#718096;background:#f7fafc;border-bottom:2px solid #e2e8f0;white-space:nowrap}}
  .td{{padding:10px 14px;border-bottom:1px solid #f0f4f8;vertical-align:top}}
  tr:last-child .td{{border-bottom:none}}
  tr:hover .td{{background:#fafbfc}}
  .empty{{padding:24px;color:#a0aec0;text-align:center;font-style:italic}}

  /* Config cards */
  .cfg-card{{border:1px solid #e2e8f0;border-radius:10px;margin-bottom:14px;overflow:hidden;transition:.2s;box-shadow:0 1px 2px rgba(0,0,0,.04)}}
  .cfg-card:hover{{box-shadow:0 2px 8px rgba(0,0,0,.1)}}
  .cfg-header{{padding:14px 18px;background:#f7fafc;cursor:pointer;display:flex;justify-content:space-between;align-items:center;user-select:none}}
  .cfg-header:hover{{background:#edf2f7}}
  .section-label{{font-size:11px;font-weight:700;color:#718096;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}}

  /* Charts */
  .chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px}}
  .chart-box{{background:#f7fafc;border-radius:10px;padding:18px;min-height:260px;display:flex;flex-direction:column}}
  .chart-box h3{{font-size:13px;font-weight:700;color:#4a5568;margin-bottom:14px}}
  .chart-wrap{{flex:1;position:relative}}

  /* Raw JSON */
  pre{{background:#1a202c;color:#e2e8f0;padding:20px;border-radius:10px;overflow:auto;font-size:11px;line-height:1.7;max-height:550px;tab-size:2}}

  /* Search */
  .search-box{{padding:8px 14px;border:1px solid #e2e8f0;border-radius:8px;font-size:13px;width:260px;outline:none}}
  .search-box:focus{{border-color:#4299e1;box-shadow:0 0 0 3px rgba(66,153,225,.15)}}

  /* Badge count */
  .bcnt{{display:inline-flex;align-items:center;justify-content:center;background:#edf2f7;color:#4a5568;border-radius:999px;font-size:11px;font-weight:700;min-width:20px;height:20px;padding:0 6px;margin-left:5px}}
  .tab.active .bcnt{{background:#bee3f8;color:#2a4365}}

  /* Notifications */
  .notif{{padding:10px 14px;border-left:3px solid #4299e1;background:#ebf8ff;border-radius:0 6px 6px 0;margin-bottom:8px;font-size:13px}}

  @media(max-width:768px){{
    .container{{padding:0 12px}}
    .kpi-grid{{grid-template-columns:repeat(2,1fr)}}
    .chart-grid{{grid-template-columns:1fr}}
  }}
  @media print{{
    .tabs,.search-box{{display:none}}
    .tab-content{{display:block!important}}
    .cfg-card div[id^="cfg-"]{{display:block!important}}
  }}
</style>
</head>
<body>

<div class="navbar">
  <div>
    <div class="navbar-title">Bindplane Platform Assessment</div>
    <div class="navbar-sub">📡 {report['bindplane_url']} &nbsp;|&nbsp; 🕐 {ts} UTC</div>
  </div>
  <div class="navbar-logo">📊</div>
</div>

<div class="container">

  <!-- KPIs -->
  <div class="kpi-grid">{kpi_html}</div>

  <!-- Main panel con tabs -->
  <div class="panel">
    <div class="tabs">
      <button class="tab active" onclick="showTab('findings',this)">Findings <span class="bcnt">{len(findings)}</span></button>
      <button class="tab" onclick="showTab('agents',this)">Agentes <span class="bcnt">{len(agents_raw)}</span></button>
      <button class="tab" onclick="showTab('configs',this)">Configuraciones <span class="bcnt">{len(configs_d)}</span></button>
      <button class="tab" onclick="showTab('charts',this)">Gráficos</button>
      <button class="tab" onclick="showTab('logtypes',this)">📋 LogTypes <span class="bcnt">{len(lt_inv)}</span></button>
      <button class="tab" onclick="showTab('notifs',this)">Notificaciones <span class="bcnt">{len(report.get('notifications',[]))}</span></button>
      <button class="tab" onclick="showTab('raw',this)">Raw JSON</button>
    </div>

    <!-- FINDINGS -->
    <div id="tab-findings" class="tab-content active">
      <table>
        <thead>
          <tr>
            <th>Severidad</th><th>Categoría</th><th>Hallazgo</th><th>Detalle</th>
          </tr>
        </thead>
        <tbody>{finding_rows}</tbody>
      </table>
    </div>

    <!-- AGENTS -->
    <div id="tab-agents" class="tab-content">
      <div style="margin-bottom:14px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
        <input class="search-box" oninput="filterTable('agent-table',this.value)" placeholder="🔍 Buscar agente..."/>
        <div style="font-size:12px;color:#718096">
          Conectados: <strong style="color:#276749">{a['by_status'].get('connected',0)}</strong> &nbsp;
          Desconectados: <strong style="color:#9b2c2c">{len(a['disconnected'])}</strong> &nbsp;
          Falla: <strong style="color:#7b341e">{len(a['component_failed'])}</strong>
        </div>
      </div>
      <table id="agent-table">
        <thead>
          <tr>
            <th onclick="sortTable('agent-table',0)" style="cursor:pointer">Nombre ↕</th>
            <th>Estado</th>
            <th onclick="sortTable('agent-table',2)" style="cursor:pointer">Versión ↕</th>
            <th>Configuración</th>
            <th style="text-align:right">Bytes Enviados</th>
            <th style="text-align:right">Bytes Recibidos</th>
          </tr>
        </thead>
        <tbody>{agent_rows}</tbody>
      </table>
    </div>

    <!-- CONFIGURATIONS -->
    <div id="tab-configs" class="tab-content">
      <div style="margin-bottom:16px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
        <input class="search-box" oninput="filterCards(this.value)" placeholder="🔍 Buscar configuración..."/>
        <div style="display:flex;gap:8px">
          <button onclick="expandAll()" style="padding:6px 14px;border-radius:6px;border:1px solid #e2e8f0;background:white;cursor:pointer;font-size:12px">Expandir todo</button>
          <button onclick="collapseAll()" style="padding:6px 14px;border-radius:6px;border:1px solid #e2e8f0;background:white;cursor:pointer;font-size:12px">Colapsar todo</button>
        </div>
      </div>
      <div id="cfg-container">{config_cards}</div>
    </div>

    <!-- CHARTS -->
    <div id="tab-charts" class="tab-content">
      <div class="chart-grid">

        <div class="chart-box">
          <h3>Agentes por Estado</h3>
          <div class="chart-wrap"><canvas id="chartStatus"></canvas></div>
        </div>

        <div class="chart-box">
          <h3>Findings por Severidad</h3>
          <div class="chart-wrap"><canvas id="chartSev"></canvas></div>
        </div>

        <div class="chart-box">
          <h3>Destinations por Tipo</h3>
          <div class="chart-wrap"><canvas id="chartDest"></canvas></div>
        </div>

        <div class="chart-box">
          <h3>Agentes por Versión</h3>
          <div class="chart-wrap"><canvas id="chartVer"></canvas></div>
        </div>

        {'<div class="chart-box"><h3>Throughput por Configuración (bytes enviados)</h3><div class="chart-wrap"><canvas id="chartThroughput"></canvas></div></div>' if ct else ''}

      </div>
    </div>

    <!-- LOGTYPES INVENTORY -->
    <div id="tab-logtypes" class="tab-content">
      <div style="margin-bottom:14px;display:flex;align-items:center;justify-content:space-between">
        <div style="font-size:13px;color:#718096">
          Todos los LogTypes configurados en la plataforma, por config y destino.
        </div>
        <input class="search-box" oninput="filterTable('lt-table',this.value)" placeholder="🔍 Buscar logType..."/>
      </div>
      <table id="lt-table">
        <thead>
          <tr>
            <th onclick="sortTable('lt-table',0)" style="cursor:pointer">LogType ↕</th>
            <th onclick="sortTable('lt-table',1)" style="cursor:pointer">Configuración ↕</th>
            <th>Source Type</th>
            <th>Destino</th>
          </tr>
        </thead>
        <tbody>{lt_rows if lt_rows else '<tr><td colspan="4" class="empty">No se encontraron LogTypes</td></tr>'}</tbody>
      </table>
    </div>

    <!-- NOTIFICATIONS -->
    <div id="tab-notifs" class="tab-content">
      {''.join(f'<div class="notif"><strong>{n.get("title","")}</strong> — {n.get("message", n.get("body",""))}<div style="font-size:11px;color:#718096;margin-top:3px">{n.get("createdAt","")}</div></div>' for n in report.get("notifications",[])) or '<p class="empty">Sin notificaciones</p>'}
    </div>

    <!-- RAW JSON -->
    <div id="tab-raw" class="tab-content">
      <div style="display:flex;justify-content:flex-end;gap:8px;margin-bottom:12px">
        <button onclick="copyRaw()" style="padding:7px 16px;border-radius:6px;border:1px solid #e2e8f0;background:white;cursor:pointer;font-size:13px">📋 Copiar JSON</button>
        <button onclick="downloadRaw()" style="padding:7px 16px;border-radius:6px;border:1px solid #e2e8f0;background:white;cursor:pointer;font-size:13px">⬇ Descargar</button>
      </div>
      <pre id="raw-json">{raw_json_str}</pre>
    </div>

  </div><!-- /panel -->

  <div style="text-align:center;font-size:12px;color:#a0aec0;padding:16px 0 32px">
    Bindplane Assessment · Generado {ts} UTC
  </div>
</div>

<script>
// ── Tabs ──────────────────────────────────────────────────────────────────
function showTab(name, btn) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (btn) btn.classList.add('active');
  if (name === 'charts') initCharts();
}}

// ── Config toggle ─────────────────────────────────────────────────────────
function toggleCfg(id) {{
  const el  = document.getElementById('cfg-' + id);
  const arr = document.getElementById('arr-' + id);
  const open = el.style.display !== 'none';
  el.style.display  = open ? 'none' : 'block';
  arr.textContent   = open ? '▼' : '▲';
}}
function expandAll()  {{ document.querySelectorAll('[id^="cfg-"]').forEach(el => {{ if(el.id!=='cfg-container') el.style.display='block'; }}); document.querySelectorAll('[id^="arr-"]').forEach(el => el.textContent='▲'); }}
function collapseAll(){{ document.querySelectorAll('[id^="cfg-"]').forEach(el => {{ if(el.id!=='cfg-container') el.style.display='none';  }}); document.querySelectorAll('[id^="arr-"]').forEach(el => el.textContent='▼'); }}

// ── Search / filter ───────────────────────────────────────────────────────
function filterTable(tableId, q) {{
  q = q.toLowerCase();
  document.querySelectorAll('#' + tableId + ' tbody tr').forEach(row => {{
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}
function filterCards(q) {{
  q = q.toLowerCase();
  document.querySelectorAll('#cfg-container .cfg-card').forEach(card => {{
    card.style.display = card.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}

// ── Sort table ────────────────────────────────────────────────────────────
const _sortState = {{}};
function sortTable(tableId, col) {{
  const tbody = document.querySelector('#' + tableId + ' tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const asc   = !_sortState[tableId + col];
  _sortState[tableId + col] = asc;
  rows.sort((a, b) => {{
    const va = a.cells[col]?.textContent.trim() || '';
    const vb = b.cells[col]?.textContent.trim() || '';
    return asc ? va.localeCompare(vb, undefined, {{numeric:true}}) : vb.localeCompare(va, undefined, {{numeric:true}});
  }});
  rows.forEach(r => tbody.appendChild(r));
}}

// ── Raw JSON actions ──────────────────────────────────────────────────────
function copyRaw() {{
  navigator.clipboard.writeText(document.getElementById('raw-json').textContent);
  event.target.textContent = '✅ Copiado!';
  setTimeout(() => event.target.textContent = '📋 Copiar JSON', 2000);
}}
function downloadRaw() {{
  const blob = new Blob([document.getElementById('raw-json').textContent], {{type:'application/json'}});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = 'bindplane_raw.json'; a.click();
}}

// ── Charts (lazy init) ────────────────────────────────────────────────────
let chartsInit = false;
function initCharts() {{
  if (chartsInit) return;
  chartsInit = true;

  const doughnutOpts = (title) => ({{
    responsive:true, maintainAspectRatio:false,
    plugins:{{ legend:{{ position:'right', labels:{{ font:{{size:11}}, boxWidth:14 }} }},
               title:{{ display:false }} }}
  }});
  const barOpts = (title) => ({{
    responsive:true, maintainAspectRatio:false, indexAxis:'y',
    plugins:{{ legend:{{display:false}}, title:{{display:false}} }},
    scales:{{ x:{{ grid:{{color:'#f0f4f8'}} }}, y:{{ grid:{{display:false}} }} }}
  }});

  new Chart(document.getElementById('chartStatus'), {{
    type:'doughnut',
    data:{{ labels:{agent_status_labels}, datasets:[{{ data:{agent_status_data},
      backgroundColor:{agent_status_colors}, borderWidth:2, borderColor:'white' }}] }},
    options: doughnutOpts('Estado Agentes')
  }});

  new Chart(document.getElementById('chartSev'), {{
    type:'doughnut',
    data:{{ labels:{sev_labels}, datasets:[{{ data:{sev_data},
      backgroundColor:{sev_colors}, borderWidth:2, borderColor:'white' }}] }},
    options: doughnutOpts('Findings')
  }});

  new Chart(document.getElementById('chartDest'), {{
    type:'doughnut',
    data:{{ labels:{dest_labels}, datasets:[{{ data:{dest_data},
      backgroundColor:{dest_colors}, borderWidth:2, borderColor:'white' }}] }},
    options: doughnutOpts('Destinations')
  }});

  new Chart(document.getElementById('chartVer'), {{
    type:'bar',
    data:{{ labels:{ver_labels}, datasets:[{{ data:{ver_data},
      backgroundColor:{ver_colors}, borderRadius:4 }}] }},
    options: barOpts('Versiones')
  }});

  {'new Chart(document.getElementById("chartThroughput"), { type:"bar", data:{ labels:' + ct_labels + ', datasets:[{ data:' + ct_data + ', backgroundColor:' + ct_colors + ', borderRadius:4 }] }, options:{ responsive:true, maintainAspectRatio:false, indexAxis:"y", plugins:{ legend:{display:false} }, scales:{ x:{ grid:{color:"#f0f4f8"}, ticks:{ callback: v => v > 1073741824 ? (v/1073741824).toFixed(1)+"GB" : v > 1048576 ? (v/1048576).toFixed(1)+"MB" : (v/1024).toFixed(0)+"KB" } }, y:{ grid:{display:false} } } } });' if ct else ''}
}}
</script>
</body>
</html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  [+] HTML: {path}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    global API_PREFIX

    if not API_KEY:
        print("[ERROR] Seteá BINDPLANE_API_KEY en el .env")
        sys.exit(1)

    masked = API_KEY[:6] + "..." + API_KEY[-4:] if len(API_KEY) > 10 else f"({len(API_KEY)} chars)"
    print(f"\n{'='*60}")
    print(f"  Bindplane Assessment v2")
    print(f"  URL : {BASE_URL}")
    print(f"  Key : {masked}")
    print(f"{'='*60}")

    print("\nDetectando API prefix...", end=" ", flush=True)
    API_PREFIX = _detect_prefix()
    print(f"'{API_PREFIX or '/'}' ✓")

    test = SESSION.get(f"{BASE_URL}{API_PREFIX}/agents", timeout=10)
    print(f"Conectividad: HTTP {test.status_code}")
    if test.status_code == 401:
        print("[ERROR] API Key inválida.")
        sys.exit(1)
    if test.status_code == 403:
        print("[ERROR] Acceso denegado (plan Growth/Enterprise requerido).")
        sys.exit(1)

    print("\nRecolectando datos...")
    agents_summary   = collect_agents_summary()
    agents_full      = collect_agents_full(agents_summary)
    configurations   = collect_configurations()
    destinations     = collect_destinations()
    agent_versions   = collect_agent_versions()
    fleets           = collect_fleets()
    notifications    = collect_notifications()

    raw = {
        "agents_full":    agents_full,
        "configurations": configurations,
        "destinations":   destinations,
        "agent_versions": agent_versions,
        "fleets":         fleets,
        "notifications":  notifications,
    }

    print("\nAnalizando...")
    report = build_report(raw)

    print_report(report)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\nGuardando reportes...")
    save_json(report, f"bindplane_assessment_{ts}.json")
    save_raw_json(raw,    f"bindplane_raw_{ts}.json")
    save_csv(report,      f"bindplane_findings_{ts}.csv")
    save_html(report, raw, f"bindplane_assessment_{ts}.html")

    print(f"\n{'='*60}")
    print(f"  ✅ Assessment completo. Abrí el HTML en el navegador.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
