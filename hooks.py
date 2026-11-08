# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

_CONFIG_KEYS = frozenset(("dissolve_ms", "overlay_enabled", "overlay_url"))


def _mirror_dsk1(new_params, body):
    """Mélangeur 0.23.52 : l'habillage EST le DSK 1, dont l'état persisté vit dans le document des
    délais (`delays.dsks[0]` : url, active). Ce document fait foi au démarrage dès qu'il porte
    `dsks` ; overlay_url / overlay_enabled n'en sont alors que le reflet. On y recopie donc ce que
    l'on vient de persister, sinon un redéploiement reprendrait l'ancienne valeur du document."""
    doc = new_params.get("delays")
    if not isinstance(doc, dict) or not isinstance(doc.get("dsks"), list) or not doc["dsks"]:
        return
    d1 = doc["dsks"][0]
    if not isinstance(d1, dict):
        return
    doc = dict(doc)
    d1 = dict(d1)
    if "overlay_url" in body:
        d1["url"] = new_params["overlay_url"] or ""
    if "overlay_enabled" in body:
        d1["active"] = bool(new_params["overlay_enabled"])
    doc["dsks"] = [d1] + list(doc["dsks"][1:])
    new_params["delays"] = doc


def control_action(action, body, params, ctx):
    """POST /api/containers/<vmid>/control/config.
    Persiste dissolve_ms/overlay_enabled/overlay_url (scope user, jusqu'ici perdus au
    redeploy) et les pousse à chaud. dissolve_ms → :8082/config (le nouvel endpoint gère
    aussi l'overlay) ; overlay seul → :8082/overlay (endpoint EXISTANT, déjà présent sur
    tous les mixers déployés → pas de repli redeploy sur un mixer à l'antenne).
    0.23.52 : l'overlay est le DSK 1 — sa copie dans le document des délais suit (_mirror_dsk1).

    0.23.53 : action « outputs » (POST /api/containers/<vmid>/control/outputs) — la liste des
    sorties programme, cf. outputs_apply. Forme changée (sortie ajoutée, retirée ou renommée, PVW
    ajouté ou retiré) → redéploiement (hot_endpoint None), et seulement avec `confirm: true` : des
    abonnés peuvent être débranchés et l'antenne est coupée quelques secondes — la page les liste
    d'abord (GET /api/containers/<vmid>/flow_consumers). Forme inchangée → à chaud (/outputs).
    0.23.55 : le LIBELLÉ seul d'une sortie (même flux, ex. « CLEAN » → « Clean ») passe à chaud : nom
    poussé au moteur, `refresh_wiring` pour que l'orchestrateur republie le sender NMOS."""
    if action == "outputs":
        new, changed = outputs_apply(body or {}, dict(params or {}), (ctx or {}).get("hostname") or "")
        renamed = _renamed_outputs(params or {}, new, (ctx or {}).get("hostname") or "")
        if renamed and not hot_rename_supported(params or {}):
            changed = True          # moteur d'avant 0.23.55 : il refuserait le nom (409) → redéploiement
        if changed:
            if body.get("confirm") is not True:
                raise ValueError("confirm_required")
            return {"params": new, "hot_endpoint": None, "hot_body": None, "redeploy": True}
        hot = {"outputs": [dict(id=o["id"], tally=o["tally"], layers=o["layers"]) for o in new["outputs"]],
               "pvw_base": new["pvw_base"]}
        res = {"params": new, "hot_endpoint": "/outputs", "hot_body": hot}
        if renamed:
            # 0.23.55 : LIBELLÉ seul (même flux) → à chaud : le moteur prend le nom, et l'orchestrateur
            # republie le libellé du sender NMOS (`refresh_wiring`) — aucune coupure.
            for d in hot["outputs"]:
                if d["id"] in renamed:
                    d["name"] = renamed[d["id"]]
            res["refresh_wiring"] = True
        return res
    if action == "setup":
        return setup_apply(body or {}, dict(params or {}), ctx or {})
    if action != "config":
        return None
    new_params = dict(params)
    touched = [k for k in _CONFIG_KEYS if k in body]
    if not touched:
        return {"params": new_params, "hot_endpoint": None, "hot_body": None}

    if "dissolve_ms" in body:
        try:
            new_params["dissolve_ms"] = max(0, min(10000, int(body["dissolve_ms"])))
        except Exception:
            pass
    if "overlay_enabled" in body:
        new_params["overlay_enabled"] = bool(body["overlay_enabled"])
    if "overlay_url" in body:
        u = body["overlay_url"]
        new_params["overlay_url"] = (str(u).strip() or None) if u else None
    if "overlay_enabled" in body or "overlay_url" in body:
        _mirror_dsk1(new_params, body)

    if "dissolve_ms" in body:
        hot_body = {k: new_params[k] for k in touched if k in new_params}
        return {"params": new_params, "hot_endpoint": "/config", "hot_body": hot_body}

    # Overlay seul : mapping vers les clés attendues par l'endpoint /overlay existant.
    hot_body = {}
    if "overlay_enabled" in body:
        hot_body["enabled"] = new_params["overlay_enabled"]
    if "overlay_url" in body:
        hot_body["url"] = new_params["overlay_url"]
    return {"params": new_params, "hot_endpoint": "/overlay", "hot_body": hot_body}


# ─── SORTIES PROGRAMME (0.23.53) ─────────────────────────────────────────────────────────────
# Forme des sorties (nombre, noms → flux, PVW oui/non) : décidée ICI, au déploiement — le câblage
# (`wiring_produces`) et le moteur (script.py, `_outputs_init`) la dérivent de la même façon des
# mêmes params. Couches, tally et base du PVW : à chaud (POST /outputs du moteur, persisté dans
# `delays.outputs` par control.persist) ; ce document l'emporte sur `params.outputs` pour un même id.
# Les deux copies de la règle (ici et dans script.py) sont tenues ensemble par
# tests/verif_mixer_sorties.py : même slug, mêmes sorties effectives.
MAX_OUTPUTS = 4
OUTPUT_ID_PGM, OUTPUT_ID_CLEAN, OUTPUT_ID_PVW = 0, 1, 2
OUTPUT_NAME_MAX = 24
_MAX_KEYERS, _MAX_DSK = 4, 2
_NONE = (None, "", -1, "none")


def output_slug(name, oid):
    """Suffixe du flux d'une sortie (`<hostname>_<slug>`) : minuscules ASCII, [a-z0-9_], 24 au plus ;
    vide → out<id>. « PGM » → pgm, « CLEAN » → clean : les noms de flux d'avant les sorties."""
    import unicodedata
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii").lower()
    s = "".join(c if (c.isalnum() or c == "_") else "_" for c in s)
    while "__" in s:
        s = s.replace("__", "_")
    s = s.strip("_")[:OUTPUT_NAME_MAX].strip("_")
    return s or ("out%d" % oid)


def _int(v, dflt):
    try:
        return int(v)
    except (TypeError, ValueError):
        return dflt


def layer_keys(params):
    """Clés des couches déployées, ordre de la pile : k1…k<n_keyers>, puis d1…d<n_dsk>."""
    nk = max(1, min(_MAX_KEYERS, _int(params.get("n_keyers"), 1) if params.get("n_keyers") not in (None, "") else 1))
    raw = params.get("n_dsk")
    nd = 1 if raw in (None, "") else max(0, min(_MAX_DSK, _int(raw, 1)))
    return ["k%d" % (i + 1) for i in range(nk)] + ["d%d" % (j + 1) for j in range(nd)]


def effective_outputs(hostname, params):
    """(sorties, base du PVW) telles que le moteur les aura au démarrage : `params.outputs` ou
    l'ancien schéma (PGM toutes couches + CLEAN keyers « clean »), puis l'état à chaud persisté
    (`delays.outputs`), puis toute couche orpheline sur la sortie de base du PVW. Chaque sortie :
    {id, name, shm, tally, layers: [clés, ordre de la pile]}."""
    params = params or {}
    keys = layer_keys(params)
    dl = params.get("delays") if isinstance(params.get("delays"), dict) else {}
    raw = params.get("outputs")
    if not isinstance(raw, list) or not raw:
        kdoc = dl.get("keyers") if isinstance(dl.get("keyers"), list) else []
        clean = set("k%d" % (i + 1) for i, k in enumerate(kdoc) if isinstance(k, dict) and k.get("clean") in (True, 1, "1", "true"))
        outs = [dict(id=OUTPUT_ID_PGM, name="PGM", shm=params.get("shm_out_pgm") or (hostname + "_pgm"),
                     tally=True, layers=set(keys)),
                dict(id=OUTPUT_ID_CLEAN, name="CLEAN", shm=params.get("shm_out_clean") or (hostname + "_clean"),
                     tally=False, layers=set(k for k in keys if k in clean))]
        pvw = OUTPUT_ID_PGM
    else:
        outs, seen = [], set()
        for d in raw:
            if not isinstance(d, dict):
                continue
            oid = _int(d.get("id"), None)
            if oid is None or oid < 0 or oid == OUTPUT_ID_PVW or oid in seen or len(outs) >= MAX_OUTPUTS:
                continue
            seen.add(oid)
            name = str(d.get("name") or "").strip()[:OUTPUT_NAME_MAX] or ("OUT %d" % oid)
            shm = str(d.get("shm") or "").strip() or (hostname + "_" + output_slug(name, oid))
            t = d.get("tally", oid == OUTPUT_ID_PGM)
            outs.append(dict(id=oid, name=name, shm=shm, tally=(t.strip().lower() in ("1", "true", "yes", "on")
                                                              if isinstance(t, str) else bool(t)),
                             layers=set(k for k in (d.get("layers") or ()) if k in keys)))
        if not any(o["id"] == OUTPUT_ID_PGM for o in outs):
            outs.insert(0, dict(id=OUTPUT_ID_PGM, name="PGM", shm=params.get("shm_out_pgm") or (hostname + "_pgm"),
                                tally=True, layers=set(keys)))
            outs = outs[:MAX_OUTPUTS]
        outs.sort(key=lambda o: o["id"])
        pvw = params.get("pvw_base", OUTPUT_ID_PGM) if "pvw_base" in params else OUTPUT_ID_PGM
        if pvw in _NONE:
            pvw = None
        else:
            pvw = _int(pvw, -1)
            if not any(o["id"] == pvw for o in outs):
                pvw = OUTPUT_ID_PGM
    byid = dict((o["id"], o) for o in outs)
    doc = dl.get("outputs")
    if isinstance(doc, dict):
        for d in doc.get("list") or ():
            o = byid.get(d.get("id")) if isinstance(d, dict) else None
            if o is None:
                continue
            if "tally" in d:
                o["tally"] = bool(d["tally"])
            if isinstance(d.get("layers"), list):
                o["layers"] = set(k for k in d["layers"] if k in keys)
        if pvw is not None and doc.get("pvw_base") in byid:
            pvw = doc["pvw_base"]
    base = byid.get(OUTPUT_ID_PGM if pvw is None else pvw) or outs[0]
    for k in keys:
        if not any(k in o["layers"] for o in outs):
            base["layers"].add(k)
    for o in outs:
        o["layers"] = [k for k in keys if k in o["layers"]]
    return outs, pvw


# Libellé seul à chaud (0.23.55) : le moteur accepte un nouveau NOM sur POST /outputs tant que le flux
# ne change pas. Un moteur plus ancien répondrait 409 → le hook le traite alors comme un changement de
# forme (redéploiement confirmé) plutôt que de laisser l'orchestrateur redéployer sans confirmation.
HOT_RENAME_SINCE = (0, 23, 55)


def _version_tuple(v):
    try:
        return tuple(int(x) for x in str(v or "").strip().split(".")[:3])
    except ValueError:
        return ()


def hot_rename_supported(params):
    """Le moteur DÉPLOYÉ (`params.plugin_version`, estampillé au déploiement) prend-il un nom à chaud ?
    Version absente → non (cas sûr : redéploiement confirmé)."""
    return _version_tuple((params or {}).get("plugin_version")) >= HOT_RENAME_SINCE


def _renamed_outputs(old_params, new_params, hostname):
    """{id: nouveau nom} des sorties dont seul le LIBELLÉ change (même id, même flux)."""
    cur, _p = effective_outputs(hostname, old_params)
    byid = dict((o["id"], o) for o in cur)
    out = {}
    for o in new_params.get("outputs") or ():
        c = byid.get(o["id"])
        if c is not None and c["name"] != o["name"] and c["shm"] == o["shm"]:
            out[o["id"]] = o["name"]
    return out


def wiring_produces(hostname, params, produces):
    """Ports PRODUITS du mélangeur à sorties nommées (hook de app/plugins.py:derive_wiring). Ancien
    schéma (sans `outputs`) : None — les trois ports statiques du manifeste, inchangés. Sinon une
    sortie = un port : slot = id (UUID NMOS stable : 0 PGM, 1 CLEAN, ≥ 3), libellé = son nom,
    `tally_bus` « out:<id> » (sa liste dans /state.tally.outputs), `tally_counts` = sa case tally ;
    puis le PVW (slot 2) s'il existe."""
    if not isinstance((params or {}).get("outputs"), list) or not params["outputs"]:
        return None
    outs, pvw = effective_outputs(hostname, params)
    ports = [dict(essence="video", shm=o["shm"], label=o["name"], slot=o["id"],
                  tally_bus="out:%d" % o["id"], tally_counts=bool(o["tally"])) for o in outs]
    if pvw is not None:
        shm = params.get("shm_out_pvw") or (hostname + "_pvw")
        st = next((p for p in produces or () if p.get("shm") == shm), None) or dict(essence="video", shm=shm, label="PVW")
        ports.append(dict(st, slot=OUTPUT_ID_PVW))
    return ports


def outputs_apply(body, params, hostname):
    """Nouvelle liste de sorties demandée (`body.outputs`, `body.pvw_base`) → (params, forme changée).
    Lève ValueError (message = code) sur une demande refusée ; RIEN n'est appliqué alors."""
    want = body.get("outputs")
    if not isinstance(want, list) or not 1 <= len(want) <= MAX_OUTPUTS:
        raise ValueError("outputs: 1..%d required" % MAX_OUTPUTS)
    keys = layer_keys(params)
    cur, cur_pvw = effective_outputs(hostname, params)
    ids = set()
    norm = []
    for d in want:
        if not isinstance(d, dict):
            raise ValueError("outputs: object expected")
        oid = d.get("id")
        if oid is not None:
            oid = _int(oid, None)
            if oid is None or oid < 0 or oid == OUTPUT_ID_PVW or oid in ids:
                raise ValueError("outputs: bad id %r" % (d.get("id"),))
            ids.add(oid)
        name = str(d.get("name") or "").strip()[:OUTPUT_NAME_MAX]
        if not name:
            raise ValueError("outputs: name required")
        lay = d.get("layers")
        if lay is not None and (not isinstance(lay, list) or any(k not in keys for k in lay)):
            raise ValueError("outputs: unknown layer in %r" % (lay,))
        norm.append(dict(id=oid, name=name, tally=bool(d.get("tally", oid == OUTPUT_ID_PGM)),
                         layers=(list(lay) if lay is not None else None)))
    if OUTPUT_ID_PGM not in ids:
        raise ValueError("outputs: PGM (id 0) is always present")
    nxt = 3
    for d in norm:
        if d["id"] is None:
            while nxt in ids:
                nxt += 1
            d["id"] = nxt
            ids.add(nxt)
    norm.sort(key=lambda d: d["id"])
    byid = dict((o["id"], o) for o in cur)
    slugs = set()
    for d in norm:
        o = byid.get(d["id"])
        # Nom inchangé, ou libellé seul (même slug : « CLEAN » → « Clean ») : le flux aussi — un ancien
        # flux hors règle (shm_out_pgm) n'est pas renommé non plus (0.23.55).
        same = o and (o["name"] == d["name"] or output_slug(o["name"], d["id"]) == output_slug(d["name"], d["id"]))
        d["shm"] = o["shm"] if same else (hostname + "_" + output_slug(d["name"], d["id"]))
        sl = d["shm"][len(hostname) + 1:] if d["shm"].startswith(hostname + "_") else d["shm"]
        if sl in slugs or sl == "pvw":
            raise ValueError("outputs: duplicate stream name %r" % d["shm"])
        slugs.add(sl)
        if d["layers"] is None:
            d["layers"] = list(o["layers"]) if o else []
        d["layers"] = [k for k in keys if k in d["layers"]]
    pvw = body.get("pvw_base", cur_pvw) if "pvw_base" in body else cur_pvw
    if pvw in _NONE:
        pvw = None
    else:
        pvw = _int(pvw, None)
        if pvw not in ids:
            raise ValueError("pvw_base: unknown output %r" % (body.get("pvw_base"),))
    new = dict(params)
    new["outputs"] = [dict(id=d["id"], name=d["name"], shm=d["shm"], tally=d["tally"], layers=d["layers"])
                      for d in norm]
    new["pvw_base"] = pvw
    doc = dict(params.get("delays") or {}) if isinstance(params.get("delays"), dict) else {}
    doc["outputs"] = dict(list=[dict(id=d["id"], tally=d["tally"], layers=d["layers"]) for d in norm],
                          pvw_base=pvw)
    # Keyers sur la sortie 1 : leur booléen `clean` (cf. script.py) — le document des keyers suit.
    if isinstance(doc.get("keyers"), list):
        o1 = next((d for d in norm if d["id"] == OUTPUT_ID_CLEAN), None)
        doc["keyers"] = [dict(k, clean=("k%d" % (i + 1)) in (o1["layers"] if o1 else ())) if isinstance(k, dict) else k
                         for i, k in enumerate(doc["keyers"])]
    new["delays"] = doc
    # Forme : id et flux de chaque sortie, PVW oui/non. Le NOM seul n'en fait plus partie (0.23.55) :
    # même flux → libellé appliqué à chaud (moteur, NMOS, page Câbles), cf. control_action.
    shape = lambda outs, p: ([(o["id"], o["shm"]) for o in outs], p is not None)   # noqa: E731
    changed = shape(cur, cur_pvw) != shape(new["outputs"], pvw)
    return new, changed


# ─── ASSISTANT DE CONFIGURATION (0.23.56) ────────────────────────────────────────────────────
# POST /api/containers/<vmid>/control/setup — ce que l'assistant applique en UNE fois quand il faut
# redémarrer le script : structure (entrées, format, nombre de keyers et de DSK, genlock, attente
# d'alignement), couches (type, sources, URL — écrites dans le document persisté que le moteur relit
# au démarrage), forme des sorties (outputs_apply, mêmes règles que l'action « outputs »). Rien de
# NEUF n'est réglable ici : chaque clé existe déjà au déploiement ou par un endpoint du moteur ; le
# hook ne fait que les poser ENSEMBLE, parce que les couches d'une sortie dépendent du nombre de
# keyers (outputs_apply les valide contre la structure fusionnée).
# Sans redémarrage nécessaire : à chaud (POST /outputs), et la page pousse le reste par les endpoints
# habituels (/keyer/type, /dsk/N/web…). Le marqueur `setup_pending` (posé à la création, cf.
# manifeste `setup_wizard`) est retiré : le mélangeur est CONFIGURÉ.
SETUP_PENDING_KEY = "setup_pending"
LATENCY_MODES = ("fixed_1", "fastest")
HOT_SETUP_SINCE = (0, 23, 53)
_KEYER_TYPES = ("dve", "linear", "luma", "chroma", "pattern", "web")
_LAYER_URL_MAX = 2048
_MAX_INPUTS = 20
# Valeur effective d'une clé de structure absente des params (défauts du manifeste / du moteur).
_STRUCT_DEFAULTS = {"n_inputs": 2, "n_keyers": 1, "n_dsk": 1, "width": 1280, "height": 720, "fps": 25,
                    "scan": "p", "genlock": True, "align_budget": 1}
_STRUCT_OPTIONAL = ("chroma", "bit_depth", "colorimetry")


def _as_bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)


def setup_pending(params):
    """Le mélangeur attend-il sa première configuration ? Seul le marqueur de création le dit : un
    mélangeur sans ce champ (déployé avant l'assistant) est CONFIGURÉ."""
    return _as_bool((params or {}).get(SETUP_PENDING_KEY))


def _struct_value(key, v):
    """Valeur de structure validée (ValueError nommant la clé sinon)."""
    def num(lo, hi, cast=int):
        try:
            x = cast(v)
        except (TypeError, ValueError):
            raise ValueError("structure.%s" % key)
        if isinstance(v, bool) or not lo <= x <= hi:
            raise ValueError("structure.%s" % key)
        return x
    if key == "n_inputs":
        return num(1, _MAX_INPUTS)
    if key == "n_keyers":
        return num(1, _MAX_KEYERS)
    if key == "n_dsk":
        return num(0, _MAX_DSK)
    if key == "width":
        x = num(64, 7680)
        if x % 2:
            raise ValueError("structure.width")
        return x
    if key == "height":
        x = num(64, 4320)
        if x % 2:
            raise ValueError("structure.height")
        return x
    if key == "fps":
        x = num(1, 120, float)
        return int(x) if x == int(x) else x
    if key == "scan":
        if v not in ("p", "i"):
            raise ValueError("structure.scan")
        return v
    if key == "genlock":
        if not isinstance(v, bool):
            raise ValueError("structure.genlock")
        return v
    if key == "align_budget":
        return num(0, 4)
    if key == "chroma":
        if str(v) not in ("420", "422", "444"):
            raise ValueError("structure.chroma")
        return str(v)
    if key == "bit_depth":
        return num(8, 12)
    if key == "colorimetry":
        return str(v)[:16]
    raise ValueError("structure.%s: unknown key" % key)


def _struct_effective(params, key):
    v = (params or {}).get(key)
    if v in (None, ""):
        v = _STRUCT_DEFAULTS.get(key)
    if key == "genlock":
        return _as_bool(v)
    if key == "scan":
        return "i" if str(v).strip().lower() == "i" else "p"
    if key in ("fps",):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return v


def _layer_docs(raw, prev, n, n_inputs, fam):
    """Documents persistés des couches d'une famille (keyers ou dsks), dans l'ordre FINAL de
    l'assistant. Chaque entrée {from, type, input, key_input, url} : `from` = rang d'avant de cette
    couche (ses autres réglages — position, chroma… — la suivent), absent pour une couche nouvelle."""
    if raw is None:
        return None
    if not isinstance(raw, list) or len(raw) != n:
        raise ValueError("layers.%s: %d expected" % (fam, n))
    out = []
    for d in raw:
        if not isinstance(d, dict):
            raise ValueError("layers.%s: object expected" % fam)
        src = d.get("from")
        base = {}
        if src is not None:
            src = _int(src, -1)
            if not 0 <= src < len(prev) or not isinstance(prev[src], dict):
                src = None
            else:
                base = dict(prev[src])
        if "type" in d:
            t = str(d.get("type") or "").strip().lower()
            if t not in _KEYER_TYPES:
                raise ValueError("layers.%s.type" % fam)
            base["type"] = t
        for k in ("input", "key_input"):
            if k in d:
                i = _int(d.get(k), -1)
                if not 0 <= i < n_inputs:
                    raise ValueError("layers.%s.%s" % (fam, k))
                base[k] = i
        if "url" in d:
            u = str(d.get("url") if d.get("url") is not None else "").strip()
            if len(u) > _LAYER_URL_MAX:
                raise ValueError("layers.%s.url" % fam)
            base["url"] = u
        out.append(base)
    return out


def setup_apply(body, params, ctx):
    """Configuration de l'assistant → résultat du hook (cf. en-tête). Lève PermissionError sans le
    droit de déployer, ValueError (message = code) sur une demande refusée — rien n'est appliqué."""
    if not ctx.get("can_deploy"):
        raise PermissionError("containers.deploy")
    hostname = ctx.get("hostname") or ""
    first = setup_pending(params)
    merged = dict(params)
    # La TRANSITION n'est plus un réglage de création (retour de Cyril, 30/10 : « c'est de
    # l'exploitation de dire si on veut un mix ou un wipe ») : elle se règle dans la rubrique
    # Transitions. Refus NOMMÉ plutôt qu'un champ ignoré en silence.
    if "preset_mode" in body:
        raise ValueError("preset_mode: set in operation (/preset)")
    # Délai par défaut (0.23.57, « sources synchrones ») : réglage d'exploitation À CHAUD (/latency),
    # persisté ici ; la page le pousse ensuite au moteur. Jamais un redémarrage.
    if "latency_mode" in body:
        lm = str(body.get("latency_mode") or "").strip().lower()
        if lm not in LATENCY_MODES:
            raise ValueError("latency_mode")
        merged["latency_mode"] = lm
    st = body.get("structure") or {}
    if not isinstance(st, dict):
        raise ValueError("structure: object expected")
    for k, v in st.items():
        merged[k] = _struct_value(k, v)
    if merged.get("scan") == "i" and int(merged.get("height") or 0) != 1080:
        raise ValueError("structure.scan: interlaced is 1080 only")
    struct_changed = any(_struct_effective(params, k) != _struct_effective(merged, k) for k in _STRUCT_DEFAULTS) \
        or any(k in st and params.get(k) != merged.get(k) for k in _STRUCT_OPTIONAL)
    keys = layer_keys(merged)
    nk = sum(1 for k in keys if k[0] == "k")
    nd = len(keys) - nk
    n_in = _struct_effective(merged, "n_inputs")
    doc = dict(params.get("delays") or {}) if isinstance(params.get("delays"), dict) else {}
    lay = body.get("layers") or {}
    if not isinstance(lay, dict):
        raise ValueError("layers: object expected")
    kdocs = _layer_docs(lay.get("keyers"), list(doc.get("keyers") or []), nk, n_in, "keyers")
    ddocs = _layer_docs(lay.get("dsks"), list(doc.get("dsks") or []), nd, n_in, "dsks")
    web_max = _int(merged.get("web_layers_max"), 2) if merged.get("web_layers_max") not in (None, "") else 2
    if kdocs is not None or ddocs is not None:
        pages = sum(1 for d in (kdocs or []) + (ddocs or []) if d.get("type") == "web" and d.get("url"))
        if pages > web_max:
            raise ValueError("layers: %d web pages, %d allowed" % (pages, web_max))
    merged.pop(SETUP_PENDING_KEY, None)
    want = dict(body)
    if "outputs" not in want:
        cur, cur_pvw = effective_outputs(hostname, merged)
        want["outputs"] = [dict(id=o["id"], name=o["name"], tally=o["tally"], layers=o["layers"]) for o in cur]
        want.setdefault("pvw_base", cur_pvw)
    new, out_changed = outputs_apply(want, merged, hostname)
    renamed = _renamed_outputs(params, new, hostname)
    if renamed and not hot_rename_supported(params):
        out_changed = True
    # Moteur sans POST /outputs (avant 0.23.53) : un « à chaud » échouerait et l'orchestrateur
    # redéploierait SANS confirmation — on le traite donc comme un redémarrage, confirmé.
    old_engine = _version_tuple(params.get("plugin_version")) < HOT_SETUP_SINCE
    restart = bool(struct_changed or out_changed or old_engine)
    if restart:
        # Le moteur relit les couches au démarrage : on les écrit dans le document persisté.
        doc = dict(new.get("delays") or {})
        if kdocs is not None:
            doc["keyers"] = kdocs
        if ddocs is not None:
            doc["dsks"] = ddocs
        new["delays"] = doc
        # Première configuration : rien n'est encore à l'antenne (non câblé), on ne fait pas confirmer.
        if not first and body.get("confirm") is not True:
            raise ValueError("confirm_required")
        return {"params": new, "hot_endpoint": None, "hot_body": None, "redeploy": True, "restart": True}
    hot = {"outputs": [dict(id=o["id"], tally=o["tally"], layers=o["layers"]) for o in new["outputs"]],
           "pvw_base": new["pvw_base"]}
    for d in hot["outputs"]:
        if d["id"] in renamed:
            d["name"] = renamed[d["id"]]
    res = {"params": new, "hot_endpoint": "/outputs", "hot_body": hot, "restart": False}
    if renamed:
        res["refresh_wiring"] = True
    return res
