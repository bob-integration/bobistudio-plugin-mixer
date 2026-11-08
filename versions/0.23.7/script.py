# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

import mmap, struct, time, threading, json, os, signal, io
from collections import deque
import numpy as np
import bobimxl   # migration MXL Phase 1 : N entrées Reader + 3 sorties Writer (PGM/CLEAN/PVW)

class _StageDelayNulle:
    """Repli si le bobimxl de l'IMAGE est plus ancien que le script poussé.

    `deploy.py` pousse `bobimxl.py` à côté du script (il SHADOWE celui de l'image), mais le fait
    en BEST-EFFORT : un échec de ce push est journalisé et le déploiement continue avec le binding
    de l'image. Sans ce repli, l'absence de `StageDelay` lèverait AttributeError à l'import et
    tuerait le conteneur — pour une métrique manquante. Une mesure absente doit coûter la mesure,
    pas le module. Cf. docs/reference/LATENCE_CHAINE.md.
    """
    def observe(self, *a, **k):
        return False

    def publish(self):
        return None


def _sd_new():
    return bobimxl.StageDelay() if hasattr(bobimxl, "StageDelay") else _StageDelayNulle()



def _mxl_lib_state():
    """Variante libmxl réellement chargée (baseline / x86-64-v3) — diagnostic seul, ne doit
    JAMAIS faire échouer /state."""
    try:
        return bobimxl.lib_info()
    except Exception:
        return None
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Latence : Δ ts_out - ts_in par input câblé (rolling avg) ──
class RollingMs:
    def __init__(self, n=30):
        self.d = deque(maxlen=n); self.last_ns = 0
    def push(self, ms_value):
        self.d.append(ms_value); self.last_ns = time.time_ns()
    def avg(self):
        if not self.d: return None
        if time.time_ns() - self.last_ns > 2_000_000_000: return None
        return round(sum(self.d) / len(self.d), 1)

lat_in  = {{}}  # {{shm_name: RollingMs}} — TRANSIT par entrée (ts_read − ts_in producteur) = arrivée
own_lat = RollingMs()  # traitement PROPRE du nœud (ts_out − ts_cycle_start) → own_latency_ms
# DÉLAI D'ÉTAGE en TRAMES — grandeur DISTINCTE de own_lat, qui est un temps de CALCUL.
# Le mixer impose la coordonnée de ses sorties (open_grain(index=idx_out), grille TAI) : c'est
# donc un étage qui RE-CADENCE, pas un propagateur — l'écart mesuré est un vrai délai.
# Multi-entrées : on passe TOUTES les entrées liées, StageDelay en retient les deux bornes
# (la plus récente = plancher, la plus vieille = délai réellement subi par la trame produite).
# Cf. docs/reference/LATENCE_CHAINE.md.
delai_etage = _sd_new()

# ─── Config injectée (contrat plugin) ───────────────────────
CONFIG         = {config}
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

# ─── Niveau de log ─────────────────────────────────────────────────────────
# `log_level` (config_schema du plugin, défaut « info ») filtre les impressions du script.
# Le critère n'est PAS « verbeux vs silencieux » mais ÉVÉNEMENT vs MÉTRIQUE :
#   debug   — le lance-flammes : par trame, par bande, décisions internes
#   info    — ÉVÉNEMENTS rares et signifiants  ← DÉFAUT (toujours visible) : démarrage/
#             arrêt, session ouverte/fermée, changement de format, reconnexion, repli sur
#             un chemin dégradé, entrée qui apparaît/disparaît, rebascule.
#   warning — anomalies et replis subis
#   error   — échecs
# RÈGLE 1 : après une panne, le journal PAR DÉFAUT doit permettre de RECONSTITUER
#   l'histoire. Élever le niveau après coup ne récupère RIEN : ce qui n'a pas été écrit
#   est perdu. On ne coupe donc pas l'information, on coupe la redondance.
# RÈGLE 2 : une MÉTRIQUE PÉRIODIQUE (fps, compteurs) ne se journalise PAS — elle est déjà
#   publiée sur :8080 et échantillonnée par l'orchestrateur. La journaliser duplique la
#   mesure ET consomme la fenêtre de rétention (journal Docker non roté : le bruit purge
#   les lignes utiles anciennes). Au mieux `debug`.
# RÈGLE 3 : un événement qui peut partir EN RAFALE s'AGRÈGE sur une fenêtre et sort en UNE
#   ligne périodique (« N frames lentes sur la dernière minute, pire … ») — le signal
#   reste, le spam disparaît.
# Réglable à chaud, sans redéployer, quand le plugin expose l'endpoint de contrôle :
# POST :8082/log_level {{"level": "debug"}} (exposé aux macros via param_tree/actions).
_LOG_ORDER = {{"debug": 10, "info": 20, "warning": 30, "error": 40}}
LOG_LEVEL = str(CONFIG.get("log_level") or "info").strip().lower()
if LOG_LEVEL not in _LOG_ORDER:
    LOG_LEVEL = "info"
_LOG_MIN = _LOG_ORDER[LOG_LEVEL]


def log(msg, niveau="info"):
    """Impression gatée par le niveau de log courant (défaut du message : « info »)."""
    if _LOG_ORDER.get(niveau, 20) >= _LOG_MIN:
        print(msg, flush=True)


def set_log_level(niveau):
    """Change le niveau à chaud. Renvoie True si le niveau est reconnu."""
    global LOG_LEVEL, _LOG_MIN
    lv = str(niveau or "").strip().lower()
    if lv not in _LOG_ORDER:
        return False
    LOG_LEVEL, _LOG_MIN = lv, _LOG_ORDER[lv]
    return True



# ─── Kernel compose fusionné C (libbobi_mvk, chantier fusion numpy→C 2026-07) ─────────────
# Transitions on-air (dissolve/additif/wipe/keyer) câblées en UNE passe mémoire chacune via
# bobimxl.mvk_mixf_into / mvk_mixmap_into / mvk_place_into (image bobi-compute ≥ 0.13, ABI 3
# — bit-exact au numpy, cf. docstrings bobimxl.py). Lib absente (image < 0.13) OU wrappers non
# applicables (ABI trop vieille, formes/strides non conformes) → False → repli numpy intégral :
# le repli EST l'ancien code, chaque call-site le garde intact. Pas de chemin GPU dans le mixer
# (contrairement au multiview) : gate simple sur la disponibilité de la lib.
_MVK = bool(getattr(bobimxl, "mvk_available", lambda: False)())

# Nombre d'entrées : 1..MAX_INPUTS. Les slots ne sont PAS ouverts en permanence — `ensure_input`
# n'ouvre que le PGM, le PVW, le keyer et la destination d'une transition (4 lectures max par
# trame, quel que soit N) → un slot inutilisé ne coûte ni lecture, ni bande passante, ni CPU.
# Le plafond est donc une limite d'UI/câblage, pas une limite moteur.
# ÉCRÊTAGE JAMAIS SILENCIEUX : une valeur hors bornes est refusée en amont par l'orchestrateur
# (plugins.validate_config → 400) ; si une valeur héritée arrive quand même ici, on la borne
# ET on la SIGNALE (stderr + /state.config_warning + :8080.config_warning) — pas de rabotage muet.
MAX_INPUTS    = 20
_N_RAW        = CONFIG.get("n_inputs")
try:
    _N_ASKED  = int(_N_RAW) if _N_RAW not in (None, "") else 2
except (TypeError, ValueError):
    _N_ASKED  = 2
N_INPUTS      = max(1, min(MAX_INPUTS, _N_ASKED))   # 1..MAX_INPUTS
CONFIG_WARNING = None
if _N_ASKED != N_INPUTS:
    CONFIG_WARNING = ("n_inputs=%s hors bornes (1..%d) — %d entrées appliquées"
                      % (_N_RAW, MAX_INPUTS, N_INPUTS))
    log("⚠ CONFIG : " + CONFIG_WARNING, "warning")
inst = bobimxl.Instance()   # domaine MXL ($MXL_DOMAIN ou /dev/shm/mxl)
SHM_OUT_PGM   = CONFIG.get("shm_out_pgm")   or (HOSTNAME + "_pgm")    # PGM : vidéo + overlay
SHM_OUT_CLEAN = CONFIG.get("shm_out_clean") or (HOSTNAME + "_clean")  # PGM clean (sans overlay)
SHM_OUT_PVW   = CONFIG.get("shm_out_pvw")   or (HOSTNAME + "_pvw")    # PVW (preview)
WIDTH       = int(CONFIG.get("width") or 1280);  WIDTH  -= WIDTH % 2
HEIGHT      = int(CONFIG.get("height") or 720);  HEIGHT -= HEIGHT % 2
FPS         = int(round(float(CONFIG.get("fps") or 25))) or 25
# Genlock broadcast : cadence VERROUILLÉE sur l'entrée de référence (le programme = source bgd
# dominante) → émission 1:1 par trame de réf (pas de requantification, latence = traitement +
# alignement). Les autres entrées sont alignées par media-timestamp (write_ts) dans une fenêtre
# d'ALIGN_BUDGET trames (échéance T + budget). Hors budget → entrée présentée en retard + flag.
# Skew par entrée exposé en métriques. off → cadence libre horloge mur héritée. Cf. plan synchro.
_gl = CONFIG.get("genlock", True)
GENLOCK = _gl if isinstance(_gl, bool) else str(_gl).strip().lower() in ("1", "true", "yes", "on")
ALIGN_BUDGET = max(0, int(CONFIG.get("align_budget", 1)))   # trames d'attente max pour aligner
# Nombre de trames sans progression de la référence avant de LEVER L'ALARME `sync_ref_lost`.
# Ce seuil ne retarde AUCUNE sortie — la trame part toujours à l'échéance, cf. la boucle de
# genlock. Il évite seulement de faire clignoter l'alarme sur le jitter d'arrivée normal de la
# référence, qui dérive toujours un peu de la grille nominale (source à 49,8 fps pour un
# mélangeur réglé à 50).
REF_STALL_FRAMES = 2
# Tolérance de retard de la référence, en trames, avant d'émettre sans elle. 1,25 trame (25 ms
# en 50p) passe largement au-dessus du jitter d'arrivée normal tout en garantissant qu'une trame
# part TOUJOURS : une source morte coûte un retard d'un quart de trame, jamais un gel.
REF_LATE_TOL = 1.25
INITIAL_DISSOLVE_MS = int(CONFIG.get("dissolve_ms") or 500)
INITIAL_OVERLAY_URL     = CONFIG.get("overlay_url") or None
def _as_bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)
INITIAL_OVERLAY_ENABLED = _as_bool(CONFIG.get("overlay_enabled"))

# ─── Format yuv420p/422p/444p (selon CONFIG[chroma], défaut 4:2:2) ──
CHROMA = str(CONFIG.get("chroma") or "422")
# Profondeur shm 8/10/12 bits. Blends = poids (invariants à l'échelle, seul le dtype change) ;
# conversions BT.601 paramétrées par _SCALE ; en 8 bits dtype uint8 → traitement byte-identique.
BIT_DEPTH = int(CONFIG.get("bit_depth") or 8)
_DEEP  = BIT_DEPTH >= 10
_BPS   = 2 if _DEEP else 1
_NP_DT = np.uint16 if _DEEP else np.uint8
_SCALE = 1 << (BIT_DEPTH - 8)
_MAXV  = (1 << BIT_DEPTH) - 1
_NEUTf = float(1 << (BIT_DEPTH - 1))            # 128 / 512 / 2048
_WHITEf = float(235 << (BIT_DEPTH - 8))         # 235 / 940 / 3760 (blanc studio)
_BLACK_B = (16 << (BIT_DEPTH - 8)).to_bytes(_BPS, "little")
_NEUT_B  = (1 << (BIT_DEPTH - 1)).to_bytes(_BPS, "little")
_CW = {{"420": 2, "422": 2, "444": 1}}.get(CHROMA, 2)   # diviseur largeur chroma
_CH = {{"420": 2, "422": 1, "444": 1}}.get(CHROMA, 1)   # diviseur hauteur chroma
PIX_FMT = ({{"420": "yuv420p", "422": "yuv422p", "444": "yuv444p"}}.get(CHROMA, "yuv422p")
           + (("12le" if BIT_DEPTH >= 12 else "10le") if _DEEP else ""))
UV_W = WIDTH // _CW
UV_H = HEIGHT // _CH
Y_SIZE        = WIDTH * HEIGHT * _BPS
UV_SIZE       = UV_W * UV_H * _BPS
V_FRAME_SIZE  = Y_SIZE + 2 * UV_SIZE
# (ex-shm_video_ring/V_RING_SIZE/V_HEADER_SIZE/V_TOTAL_SIZE : dimensionnaient un ancien shm
# maison ; la sortie vidéo passe par bobimxl (MXL) — profondeur gérée par domaine, cf.
# réglage de nœud mxl_history_ms. Retiré 2026-08-09, constante morte.)

# ── MODE TRANCHE (chantier latence sous-trame, cf. patch mxl-planar-slices) ─────────────────
# slice_mode=true → en GENLOCK, la trame est composée et publiée BANDE PAR BANDE (SLICE_LINES
# lignes/bande) : VERROU-RÉFÉRENCE INDEXÉ — l'index cible de la trame = l'index du grain de
# l'ENTRÉE DE RÉFÉRENCE (sa tête, suivie par tranches) ; TOUTES les autres entrées actives
# (PGM/PVW pendant transition, keys) sont suivies AU MÊME INDEX via get_slice (réveil au commit
# partiel du producteur) — alignement DÉTERMINISTE trame-à-trame (remplace « latest + skew »).
# Les 3 sorties sont publiées progressivement (open_grain 1×, commit validSlices=1..N) à CE
# MÊME INDEX → propagation, l'aval du tissu s'aligne. Convention (identique moteur/multiview/
# color_corrector) : k tranches ⇔ lignes image [0, k·slice_height) valides sur les 3 plans.
# slice_mode absent/False, genlock off, entrelacé, hauteur non divisible ou lib sans le patch
# → chemin historique STRICTEMENT inchangé (flowDefs de sortie sans slice_height), repli loggé.
_slm = CONFIG.get("slice_mode", False)
SLICE_MODE  = _slm if isinstance(_slm, bool) else str(_slm).strip().lower() in ("1", "true", "yes", "on")
SLICE_LINES = max(1, int(CONFIG.get("slice_lines") or 36))
# GARDE-FOU n°2 : seuil (%) du taux de replis (fenêtre glissante ~10 s) au-delà duquel une
# entrée est SIGNALÉE (slice_late_inputs + advisory :8080) ; réarmé sous 2 % (hystérésis).
# On ne bloque NI ne dégrade rien automatiquement — on signale, l'exploitant décide.
SLICE_LATE_PCT = max(0.5, float(CONFIG.get("slice_late_pct") or 5.0))
_SL_CLEAR_PCT  = 2.0
_scan_i = str(CONFIG.get("scan") or "p").strip().lower().startswith("i")
SLICE_ON = (SLICE_MODE and GENLOCK and not _scan_i
            and HEIGHT % SLICE_LINES == 0 and SLICE_LINES % _CH == 0
            and getattr(bobimxl, "HAS_SLICES", False))
if SLICE_MODE and not SLICE_ON:
    log(f"mixer: slice_mode demandé mais inéligible (genlock={{GENLOCK}} entrelacé={{_scan_i}} "
        f"h={{HEIGHT}}%{{SLICE_LINES}} ch={{_CH}} lib_slices={{getattr(bobimxl, 'HAS_SLICES', False)}})"
        " — repli whole-frame", "warning")
_SL_NB = (HEIGHT // SLICE_LINES) if SLICE_ON else 0   # nb de bandes par trame

# ─── État runtime ───────────────────────────────────────────
state_lock = threading.Lock()
state = {{
    "pgm": 0,
    "pvw": 1 if N_INPUTS > 1 else 0,
    # Genlock : entrée de RÉFÉRENCE de synchro (choix opérateur, changeable à chaud via /sync_ref).
    # FIXE = indépendante de la sélection PGM/PVW → ne change PAS à la commutation. La cadence de
    # sortie se verrouille en 1:1 sur cette entrée (latence mini). Désigner la source la plus stable.
    "sync_ref": max(0, min(N_INPUTS - 1, int(CONFIG.get("sync_ref", 0) or 0))),
    # Transition / mix portent désormais des sous-objets bgd ET keyer (ou None).
    # bgd : crossfade pgm↔pvw. keyer : fade in/out du keyer.
    "transition":      None,
    "manual_mix":      None,
    "preset": {{
        "mode": "dissolve",
        "wipe": {{
            "direction": "horizontal",
            "reverse":   False,
            "softness":  0.10,
        }},
    }},
    # Keyer : image overlay (issue d'un input) sur le bus PGM uniquement, pas dans le clean.
    "keyer": {{
        "input":         0,        # slot d'entrée alimentant le keyer
        "active":        False,    # affiché ou pas sur PGM
        "x":             0.5,      # centre, fraction de WIDTH
        "y":             0.5,      # centre, fraction de HEIGHT
        "scale":         0.30,     # taille en fraction (préserve l'aspect du source)
        "border_width":  0,        # px
    }},
    # Cibles du prochain take/mix : on peut transitionner BGD, KEYER, ou les deux ensemble.
    "next_targets": {{"bgd": True, "keyer": False}},
    "inputs":          [str(CONFIG.get("input_%d" % (i + 1)) or "") for i in range(N_INPUTS)],
    "overlay_url":     INITIAL_OVERLAY_URL,
    "overlay_enabled": bool(INITIAL_OVERLAY_ENABLED),
    # Dissolve par défaut (ms) — mutable à chaud via POST /config (persisté par le hook
    # control_action puis repoussé ici ; survit ainsi au redeploy).
    "default_dissolve_ms": INITIAL_DISSOLVE_MS,
}}

def _snapshot_preset():
    """Copie défensive du preset courant — capté au moment de créer un mix."""
    p = state["preset"]
    return {{"mode": p["mode"], "wipe": dict(p["wipe"])}}

# Overlay : (H, W, 4) uint8 RGBA (pour /preview.png) + sa version PRÉ-CONVERTIE en YUV
# (dict y/u/v/a, cf. prep_overlay) réellement composée sur PGM. Les deux sont mis à jour
# ENSEMBLE par overlay_loop, sous overlay_lock.
overlay_lock = threading.Lock()
overlay_rgba = None
overlay_prep = None

# Métriques
metrics_lock = threading.Lock()
metrics = {{"fps": 0.0, "frame_index": 0, "pgm": 0, "pvw": 1 if N_INPUTS > 1 else 0,
           "inputs_latency_ms": {{}}, "own_latency_ms": None, "inputs_skew_ms": {{}}, "inputs_late": [],
           "sync_ref": 0, "sync_ref_lost": False,
           # MODE TRANCHE : compteurs + garde-fou n°2 (signalement des sources inaptes)
           "slice_mode": SLICE_ON, "slice_late_inputs": {{}}, "advisory": None,
           # Kernel compose fusionné C (transitions on-air) — cf. gate _MVK plus haut.
           "mvk": _MVK, "mvk_threads": (getattr(bobimxl, "mvk_threads", lambda: 0)() if _MVK else 0),
           # Config écrêtée (jamais silencieuse) — None = config saine.
           "n_inputs": N_INPUTS, "config_warning": CONFIG_WARNING}}

# SIGBUS handling
bus_error = threading.Event()
def _handle_sigbus(signum, frame):
    log("SIGBUS reçu — réouverture des mmap", "warning")
    bus_error.set()
signal.signal(signal.SIGBUS, _handle_sigbus)


# ─── HTTP : /metrics (8080) et control (8082) ───────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with metrics_lock: payload = dict(metrics)
        # Bouches-trous amont ÉCARTÉS (cf. bobimxl.MXL_GRAIN_FLAG_INVALID) : seul témoin du
        # comblement d'un trou par le producteur — cadence, index et statut de lecture restent
        # tous parfaits pendant l'épisode. Calculé à la requête : aucun coût par trame.
        payload["invalid_grains"] = bobimxl.invalid_grains_report()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass


class ControlHandler(BaseHTTPRequestHandler):
    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n: return {{}}
        try: return json.loads(self.rfile.read(n).decode())
        except Exception: return {{}}

    def _reply(self, code, payload):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())

    def do_GET(self):
        if self.path == "/preview.png":
            # Renvoie la dernière capture Playwright en PNG. 204 si rien encore chargé.
            with overlay_lock:
                arr = overlay_rgba
            if arr is None:
                self.send_response(204); self.end_headers(); return
            try:
                from PIL import Image
                img = Image.fromarray(arr, mode="RGBA")
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                data = buf.getvalue()
            except Exception as e:
                self._reply(500, {{"error": str(e)}}); return
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path == "/state":
            with state_lock:
                mm = state["manual_mix"]
                tr = state["transition"]
                s = {{
                    "pgm": state["pgm"],
                    "pvw": state["pvw"],
                    # OBJET, pas un booléen : control.js lit `s.transition.start` et
                    # `.duration_ms` pour se resynchroniser sur une transition démarrée AILLEURS
                    # (pupitre ATEM, autre navigateur). Avec un booléen il calculait
                    # `undefined - elapsed` → NaN → la T-barre restait figée et `aria-valuenow`
                    # partait en « NaN ». Reste truthy, donc les tests `if(s.transition)`
                    # existants sont inchangés.
                    # `elapsed_ms` est calculé ICI, sur l'horloge du conteneur : un observateur
                    # (audio suit la vidéo, orchestrateur) n'a pas à comparer `start` à SA propre
                    # horloge, qui peut différer de plusieurs dizaines de ms. `from_idx`/`to_idx` :
                    # entrées de la couche BGD en transition (None si seul le keyer transitionne).
                    "transition":      (None if tr is None else {{
                        "start":       tr.get("start"),
                        "duration_ms": tr.get("duration_ms"),
                        "elapsed_ms":  int(max(0.0, (time.time() - (tr.get("start") or time.time())) * 1000.0)),
                        "from_idx":    (tr.get("bgd") or {{}}).get("from_idx"),
                        "to_idx":      (tr.get("bgd") or {{}}).get("to_idx"),
                    }}),
                    "transition_targets": (None if tr is None else {{
                        "bgd":   tr.get("bgd")   is not None,
                        "keyer": tr.get("keyer") is not None,
                    }}),
                    "manual_mix":      (None if mm is None else {{
                        "alpha": mm["alpha"],
                        "bgd":   (None if mm.get("bgd") is None else {{
                            "from_idx": mm["bgd"]["from_idx"],
                            "to_idx":   mm["bgd"]["to_idx"],
                        }}),
                        "keyer": (None if mm.get("keyer") is None else {{
                            "from_active": mm["keyer"]["from_active"],
                            "to_active":   mm["keyer"]["to_active"],
                        }}),
                    }}),
                    "preset": {{
                        "mode": state["preset"]["mode"],
                        "wipe": dict(state["preset"]["wipe"]),
                    }},
                    "keyer":         dict(state["keyer"]),
                    "next_targets":  dict(state["next_targets"]),
                    "inputs":          list(state["inputs"]),
                    "n_inputs":        N_INPUTS,
                    "max_inputs":      MAX_INPUTS,
                    "config_warning":  CONFIG_WARNING,   # écrêtage signalé, jamais muet
                    "overlay_url":     state["overlay_url"],
                    "overlay_enabled": state["overlay_enabled"],
                    "default_dissolve_ms": state["default_dissolve_ms"],
                    "plugin_version": PLUGIN_VERSION,
                    "log_level":      LOG_LEVEL,   # lisible en condition de macro
                    "mxl_lib":        _mxl_lib_state(),
                }}
                # state_field par slot (topologie / câblage générique)
                for _i in range(N_INPUTS):
                    s["input_%d" % (_i + 1)] = state["inputs"][_i]
            self._reply(200, s)
        else:
            self._reply(404, {{"error": "not found"}})

    def do_POST(self):
        body = self._read_json()
        if self.path == "/pgm":
            # Hard cut direct vers l'entrée demandée
            try: target = int(body.get("input"))
            except Exception:
                self._reply(400, {{"error": "input requis"}}); return
            if not (0 <= target < N_INPUTS):
                self._reply(400, {{"error": "input hors plage"}}); return
            with state_lock:
                state["transition"] = None
                state["manual_mix"] = None
                state["pgm"] = target
            self._reply(200, {{"ok": True}})

        elif self.path == "/sync_ref":
            # Genlock : choisir l'entrée de référence de synchro (FIXE, ≠ commutation PGM/PVW).
            try: target = int(body.get("input"))
            except Exception:
                self._reply(400, {{"error": "input requis"}}); return
            if not (0 <= target < N_INPUTS):
                self._reply(400, {{"error": "input hors plage"}}); return
            with state_lock:
                state["sync_ref"] = target
            self._reply(200, {{"ok": True, "sync_ref": target}})

        elif self.path == "/pvw":
            try: target = int(body.get("input"))
            except Exception:
                self._reply(400, {{"error": "input requis"}}); return
            if not (0 <= target < N_INPUTS):
                self._reply(400, {{"error": "input hors plage"}}); return
            with state_lock:
                state["pvw"] = target
            self._reply(200, {{"ok": True}})

        elif self.path == "/take":
            # Take selon next_targets : BGD, KEYER ou les deux. duration_ms=0 = cut sec.
            duration = body.get("duration_ms")
            if duration is None:
                with state_lock: duration = state["default_dissolve_ms"]
            try: duration = max(0, int(duration))
            except Exception:
                self._reply(400, {{"error": "duration_ms invalide"}}); return
            with state_lock:
                targets = state["next_targets"]
                bgd_part = None; keyer_part = None
                if targets["bgd"]:
                    fr = state["pgm"]; to = state["pvw"]
                    if fr != to:
                        bgd_part = {{"from_idx": fr, "to_idx": to, "flip_to_pvw": fr}}
                if targets["keyer"]:
                    keyer_part = {{
                        "from_active": state["keyer"]["active"],
                        "to_active":   not state["keyer"]["active"],
                    }}
                if bgd_part is None and keyer_part is None:
                    self._reply(200, {{"ok": True, "noop": True}}); return
                state["manual_mix"] = None
                if duration <= 0:
                    # Cut sec : applique immédiatement
                    if bgd_part:
                        state["pgm"] = bgd_part["to_idx"]
                        state["pvw"] = bgd_part["flip_to_pvw"]
                    if keyer_part:
                        state["keyer"]["active"] = keyer_part["to_active"]
                    state["transition"] = None
                else:
                    state["transition"] = {{
                        "start": time.time(),
                        "duration_ms": duration,
                        "bgd":      bgd_part,
                        "keyer":    keyer_part,
                        "preset":   _snapshot_preset(),
                    }}
            self._reply(200, {{"ok": True}})

        elif self.path == "/mix":
            # Crossfade T-bar. Agit selon next_targets : BGD et/ou KEYER.
            try: alpha = float(body.get("alpha"))
            except Exception:
                self._reply(400, {{"error": "alpha invalide"}}); return
            alpha = max(0.0, min(1.0, alpha))
            with state_lock:
                state["transition"] = None
                if alpha >= 0.999:
                    # Commit : applique les transitions des sous-objets actifs.
                    mm = state["manual_mix"]
                    if mm is not None:
                        if mm.get("bgd"):
                            state["pgm"] = mm["bgd"]["to_idx"]
                            state["pvw"] = mm["bgd"]["from_idx"]
                        if mm.get("keyer"):
                            state["keyer"]["active"] = mm["keyer"]["to_active"]
                    else:
                        # Pas de mix actif : commit selon next_targets directement.
                        t = state["next_targets"]
                        if t["bgd"] and state["pgm"] != state["pvw"]:
                            state["pgm"], state["pvw"] = state["pvw"], state["pgm"]
                        if t["keyer"]:
                            state["keyer"]["active"] = not state["keyer"]["active"]
                    state["manual_mix"] = None
                elif alpha <= 0.001:
                    state["manual_mix"] = None
                else:
                    mm = state["manual_mix"]
                    if mm is None:
                        # Démarre un nouveau mix manuel selon next_targets.
                        t = state["next_targets"]
                        bgd_part = None; keyer_part = None
                        if t["bgd"] and state["pgm"] != state["pvw"]:
                            bgd_part = {{"from_idx": state["pgm"], "to_idx": state["pvw"]}}
                        if t["keyer"]:
                            keyer_part = {{
                                "from_active": state["keyer"]["active"],
                                "to_active":   not state["keyer"]["active"],
                            }}
                        if bgd_part is None and keyer_part is None:
                            self._reply(200, {{"ok": True, "noop": True}}); return
                        state["manual_mix"] = {{
                            "alpha":  alpha,
                            "bgd":    bgd_part,
                            "keyer":  keyer_part,
                            "preset": _snapshot_preset(),
                        }}
                    else:
                        mm["alpha"] = alpha
            self._reply(200, {{"ok": True}})

        elif self.path == "/targets":
            # Cibles du prochain take/mix (PATCH-like). Au moins une doit rester active.
            with state_lock:
                t = state["next_targets"]
                if "bgd"   in body: t["bgd"]   = bool(body.get("bgd"))
                if "keyer" in body: t["keyer"] = bool(body.get("keyer"))
                # Si tout est éteint, on rallume BGD par défaut.
                if not t["bgd"] and not t["keyer"]: t["bgd"] = True
            self._reply(200, {{"ok": True}})

        elif self.path == "/keyer/input":
            try: idx = int(body.get("input"))
            except Exception:
                self._reply(400, {{"error": "input requis"}}); return
            if not (0 <= idx < N_INPUTS):
                self._reply(400, {{"error": "input hors plage"}}); return
            with state_lock:
                state["keyer"]["input"] = idx
            self._reply(200, {{"ok": True}})

        elif self.path == "/keyer/active":
            # Toggle direct (cut sans transition).
            with state_lock:
                if "active" in body:
                    state["keyer"]["active"] = bool(body.get("active"))
                else:
                    state["keyer"]["active"] = not state["keyer"]["active"]
                # Cut keyer annule toute transition keyer en cours.
                if state["manual_mix"] and state["manual_mix"].get("keyer"):
                    state["manual_mix"]["keyer"] = None
                    if not state["manual_mix"].get("bgd"):
                        state["manual_mix"] = None
                if state["transition"] and state["transition"].get("keyer"):
                    state["transition"]["keyer"] = None
                    if not state["transition"].get("bgd"):
                        state["transition"] = None
            self._reply(200, {{"ok": True}})

        elif self.path == "/keyer/transform":
            with state_lock:
                k = state["keyer"]
                if "x" in body:
                    try: k["x"] = max(0.0, min(1.0, float(body["x"])))
                    except Exception: pass
                if "y" in body:
                    try: k["y"] = max(0.0, min(1.0, float(body["y"])))
                    except Exception: pass
                if "scale" in body:
                    try: k["scale"] = max(0.05, min(1.0, float(body["scale"])))
                    except Exception: pass
                if "border_width" in body:
                    try: k["border_width"] = max(0, min(50, int(body["border_width"])))
                    except Exception: pass
            self._reply(200, {{"ok": True}})

        elif self.path == "/preset":
            # Met à jour le preset de transition (PATCH-like). S'applique au PROCHAIN
            # /take ou /mix ; les transitions en cours ne sont pas affectées.
            with state_lock:
                p = state["preset"]
                if "mode" in body:
                    m = str(body.get("mode") or "").lower()
                    if m in ("dissolve", "additif", "wipe"):
                        p["mode"] = m
                if "wipe" in body and isinstance(body.get("wipe"), dict):
                    w = body["wipe"]; cur = p["wipe"]
                    if "direction" in w:
                        d = str(w.get("direction") or "").lower()
                        if d in ("horizontal", "vertical", "diagonal", "iris", "heart"):
                            cur["direction"] = d
                            if d == "heart":
                                _kick_heart_precompute()   # init en tâche de fond
                    if "reverse" in w:
                        cur["reverse"] = bool(w.get("reverse"))
                    if "softness" in w:
                        try:    cur["softness"] = max(0.0, min(1.0, float(w.get("softness"))))
                        except Exception: pass
            self._reply(200, {{"ok": True}})

        elif self.path == "/input":
            # Câble un slot d'entrée à un shm donné (ou détache si shm vide/null).
            # Schéma générique plugin {{slot, shm}} ; alias `idx` toléré.
            _raw = body.get("slot")
            if _raw is None: _raw = body.get("idx")
            try: idx = int(_raw)
            except Exception:
                self._reply(400, {{"error": "slot requis"}}); return
            if not (0 <= idx < N_INPUTS):
                self._reply(400, {{"error": "idx hors plage"}}); return
            shm_name = body.get("shm")
            shm_name = (shm_name or "").strip()
            with state_lock:
                state["inputs"][idx] = shm_name
            self._reply(200, {{"ok": True}})

        elif self.path == "/overlay":
            # Change URL et/ou flag enabled. Les deux sont optionnels (PATCH-like).
            with state_lock:
                if "url" in body:
                    u = body.get("url")
                    state["overlay_url"] = (u or "").strip() or None
                if "enabled" in body:
                    state["overlay_enabled"] = bool(body.get("enabled"))
            self._reply(200, {{"ok": True}})

        elif self.path == "/log_level":
            # Verbosité À CHAUD (pas de redéploiement) : instruction d'incident. Le niveau
            # PERSISTANT reste le champ `log_level` du config_schema ; celui-ci est volatil.
            ok = set_log_level(body.get("level") or body.get("log_level"))
            self._reply(200 if ok else 400, {{"ok": ok, "log_level": LOG_LEVEL}})

        elif self.path == "/config":
            # Réglages d'exploitation persistés côté orchestrateur (dissolve_ms/overlay_*),
            # poussés ici à chaud par le hook control_action. Toutes les clés sont optionnelles
            # (PATCH-like) — overlay_* rejoue exactement la logique de /overlay.
            with state_lock:
                if "dissolve_ms" in body:
                    try:
                        state["default_dissolve_ms"] = max(0, min(10000, int(body.get("dissolve_ms"))))
                    except Exception:
                        pass
                if "overlay_url" in body:
                    u = body.get("overlay_url")
                    state["overlay_url"] = (u or "").strip() or None
                if "overlay_enabled" in body:
                    state["overlay_enabled"] = bool(body.get("overlay_enabled"))
            self._reply(200, {{"ok": True}})

        else:
            self._reply(404, {{"error": "not found"}})

    def log_message(self, *a): pass


threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
    daemon=True).start()
threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
    daemon=True).start()


# ─── Thread Playwright : capture la page HTML en RGBA ───────
def overlay_loop():
    global overlay_rgba, overlay_prep
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/playwright-browsers")
    try:
        from playwright.sync_api import sync_playwright
        from PIL import Image
    except ImportError as e:
        log(f"playwright/PIL indisponible — overlay HTML désactivé ({{e}})", "warning")
        return
    pw = sync_playwright().start()
    try:
        # Chromium système (apt) plutôt que celui de Playwright dont le CDN coupe.
        chromium_exec = None
        for _p in ("/usr/bin/chromium", "/usr/bin/chromium-browser"):
            if os.path.exists(_p):
                chromium_exec = _p; break
        launch_kwargs = {{"args": ["--no-sandbox", "--disable-dev-shm-usage"]}}
        if chromium_exec:
            launch_kwargs["executable_path"] = chromium_exec
        browser = pw.chromium.launch(**launch_kwargs)
        ctx = browser.new_context(viewport={{"width": WIDTH, "height": HEIGHT}})
        page = None
        current_url = None
        _prev_rgba = None     # dernière capture CONVERTIE (détection d'habillage statique)
        while True:
            with state_lock:
                # On charge la page dès qu'une URL est définie. Le drapeau enabled
                # contrôle uniquement la composition sur PGM, pas le rendu Playwright,
                # pour que l'aperçu DSK reste vivant même quand le DSK est hors-antenne.
                want_url = state["overlay_url"]
            if want_url != current_url:
                if page is not None:
                    try: page.close()
                    except Exception: pass
                    page = None
                with overlay_lock:
                    overlay_rgba = None
                    overlay_prep = None
                _prev_rgba = None
                if want_url:
                    try:
                        page = ctx.new_page()
                        page.goto(want_url, timeout=10000, wait_until="domcontentloaded")
                        page.evaluate(
                            "document.documentElement.style.background='transparent';"
                            "document.body && (document.body.style.background='transparent');"
                        )
                    except Exception as e:
                        log(f"playwright goto erreur ({{want_url}}): {{e}}", "warning")
                        try:
                            if page: page.close()
                        except Exception: pass
                        page = None
                current_url = want_url
            if page is not None:
                try:
                    png = page.screenshot(omit_background=True, type="png")
                    img = Image.open(io.BytesIO(png)).convert("RGBA")
                    if img.size != (WIDTH, HEIGHT):
                        img = img.resize((WIDTH, HEIGHT))
                    _rgba = np.asarray(img, dtype=np.uint8)
                    # Conversion RGB→YUV de l'habillage ICI (hors du chemin critique de la
                    # compose) : ce thread décode déjà un PNG 1080p à chaque tick, la prep pèse
                    # moins. La boucle de compose ne fait plus qu'un blend YUV.
                    # Habillage STATIQUE (le cas courant : un synthé qui ne bouge pas) → la
                    # capture est identique à la précédente : on ne reconvertit rien (le memcmp
                    # coûte ~1 ms contre ~35 ms de conversion).
                    if overlay_prep is not None and _prev_rgba is not None \
                            and np.array_equal(_rgba, _prev_rgba):
                        pass
                    else:
                        _prep = prep_overlay(_rgba)
                        _prev_rgba = _rgba
                        with overlay_lock:
                            overlay_rgba = _rgba
                            overlay_prep = _prep
                except Exception as e:
                    log(f"playwright screenshot erreur: {{e}}", "warning")
                    time.sleep(0.5)
            time.sleep(0.040)
    finally:
        pw.stop()

threading.Thread(target=overlay_loop, daemon=True).start()


# ─── Wiring dynamique des inputs ────────────────────────────
# Chaque slot peut être (re)câblé à chaud via POST /input.
# input_handles[idx] = (file, mmap, name_actuellement_ouvert)
input_handles = [None] * N_INPUTS

# ─── LECTEUR ORPHELIN : producteur RECRÉÉ SOUS LE MÊME NOM ───────────────────────────────────
# ★ LE PIÈGE, ET POURQUOI RIEN NE LE DÉCLENCHE (2026-10-23). Un producteur redéployé recrée son
# flux sous le MÊME nom, mais c'est un NOUVEL anneau. Notre handle reste sur l'ANCIEN : ses grains
# restent LISIBLES, donc `get_latest()` ne rend pas None et aucune exception ne survient. Et comme
# le nom voulu est inchangé, `ensure_input` rendait le lecteur en cache indéfiniment. Vu de
# l'opérateur : l'entrée reste FIGÉE sur la dernière image de l'ancien flux, ou passe au noir si
# l'anneau mort ne rend plus rien — alors que le multiview, lui, affiche bien la source.
# Constaté en recette : stills redéployé à 14h45, entrée 6 du mélangeur morte, `invalid_grains`
# FIGÉ (rien de nouveau n'est rejeté : on sert du périmé) et `inputs_latency_ms` à null.
# Le multiview a appris cette leçon le 2026-07-28 (cf. multiview `_drop_input`, `_vh_sample`,
# `_ANC_STALE_S`) ; le mélangeur ne l'avait jamais apprise, alors que le commentaire de la
# réouverture ci-dessous citait déjà cette parade comme « générique ». Une parade qui existe sur
# un chemin et manque à son jumeau ne protège personne.
#
# DÉTECTION : l'horodatage de dernière écriture du PRODUCTEUR (`last_write_time`) ne peut pas
# avancer sans écrivain. Un flux vivant l'avance 50 fois par seconde ; figé 2 s, c'est 100 trames
# sans écriture — sans ambiguïté. On ne se sert PAS de l'index de grain seul : les index MXL sont
# dérivés du temps (cf. mémoire `cadence-index-mxl-derive-du-temps`), piège voisin.
# Seuil aligné sur `_ANC_STALE_S` du multiview : même produit, même question.
_in_ecriture_vue = {{}}        # idx -> (dernier last_write_time vu, monotone de ce constat)
REOPEN_ORPHELIN_S = 2.0
_in_echec = {{}}               # idx -> monotone du dernier ÉCHEC d'ouverture (GC + log bornés)
RETENTE_GC_S = 2.0            # au plus un garbage_collect + un log par entrée et par 2 s


def _input_decroche(idx, r):
    """Vrai si le producteur de cette entrée n'écrit plus — anneau mort, handle à jeter.

    Rend False dès qu'on ne peut PAS juger (pas d'horodatage, lecture en échec) : un capteur qui
    se taît vaut mieux qu'une réouverture déclenchée sur une incertitude, qui ferait clignoter une
    entrée saine. Les retentes sont BORNÉES à une par `REOPEN_ORPHELIN_S` (le repère est oublié
    après détection, donc on ne boucle pas sur un producteur réellement absent)."""
    try:
        wt = r.last_write_time()
    except Exception:
        return False
    if not wt:
        return False
    now = time.monotonic()
    vu = _in_ecriture_vue.get(idx)
    if vu is None or vu[0] != wt:
        _in_ecriture_vue[idx] = (wt, now)      # ça avance : tout va bien
        return False
    if now - vu[1] <= REOPEN_ORPHELIN_S:
        return False
    _in_ecriture_vue.pop(idx, None)
    return True


def ensure_input(idx):
    """Renvoie le bobimxl.Reader pour l'input idx, en (ré-)ouvrant si le nom de flux a changé
    OU si son producteur a été recréé sous le même nom (cf. _input_decroche).
    Renvoie None si le slot n'est pas câblé ou le flux est indisponible."""
    with state_lock:
        wanted = state["inputs"][idx]
    cur = input_handles[idx]   # (reader, name) ou None
    _libere_notre_handle = False   # vrai si l'on ferme NOTRE lecteur juste avant de réouvrir
    if not wanted:
        if cur is not None:
            try: cur[0].close()
            except Exception: pass
            input_handles[idx] = None
        return None
    if cur is not None and cur[1] == wanted:
        if not _input_decroche(idx, cur[0]):
            return cur[0]
        # Même nom, mais plus personne n'écrit : l'anneau est mort. On le DIT (une fois par
        # détection, pas par trame) et on repart par le chemin de réouverture ci-dessous.
        # Journal en ANGLAIS : convention du dépôt pour tout nouveau log (2026-10-15).
        log(f"input #{{idx}}: reader STALE on {{wanted}} — producer has not written for "
            f"{{REOPEN_ORPHELIN_S:.0f}} s (flow recreated under the same name?), reopening",
            "warning")
    if cur is not None:
        try: cur[0].close()
        except Exception: pass
        input_handles[idx] = None
        _libere_notre_handle = True
        # GC OBLIGATOIRE entre close et réouverture (parade générique du piège des générations,
        # cf. multiview._drop_input / color_corrector) : un flux périmé reste résolvable PAR NOM
        # tant qu'il n'est pas collecté — sans GC, la réouverture retombe sur l'orphelin.
        # (La collecte elle-même a lieu une SEULE fois, plus bas, sous `_libere_notre_handle`.)
    # ★★ LE GC NE DOIT PAS DÉPENDRE DE LA POSSESSION D'UN HANDLE (2026-10-23).
    # Au PREMIER échec d'ouverture, `input_handles[idx]` est déjà None. Aux tentatives suivantes
    # `cur` vaut None, donc la branche ci-dessus — la SEULE qui collecte — était sautée. Le flux
    # périmé restant résolvable par nom tant qu'il n'est pas collecté, chaque retente retombait
    # sur l'orphelin : le mélangeur ne pouvait JAMAIS récupérer une source redéployée, et
    # réessayait 50 fois par seconde en silence. Deux entrées de la recette du 2026-10-23
    # (decklink-io_3 après le redéploiement de 14h23, stills-Atelier après celui de 14h45) étaient
    # dans cet état : flux PRÉSENTS dans le domaine, et ZÉRO mappage dans le processus — vérifié
    # sur /proc/<pid>/maps. Le testeur voyait du noir « depuis toujours ».
    # Collecte BORNÉE à une par RETENTE_GC_S et par entrée : un flux réellement absent (slot câblé
    # d'avance, producteur pas encore déployé) ne doit pas déclencher un GC par trame.
    # ★★ NE COLLECTER QUE CE QUI EST À NOUS (corrigé le 2026-10-23, avant tout déploiement).
    # `garbage_collect()` est GLOBAL AU DOMAINE. Le producteur DeckLink s'en abstient
    # délibérément et dit pourquoi dans `decklink_mxl.cpp` : le 2026-08-19 ce ramasse-miettes a
    # détruit QUATRE FLUX VIVANTS appartenant à un autre processus, laissant tous leurs lecteurs
    # à 0 i/s sur une génération morte. « On préfère échouer en le DISANT plutôt que réparer
    # notre démarrage en cassant le voisin. »
    # Ma première version appelait donc la collecte à CHAQUE échec d'ouverture, toutes les 2 s et
    # par entrée : un slot câblé sur un producteur pas encore déployé aurait collecté le domaine
    # en boucle, indéfiniment, et pu détruire les flux vivants des voisins. Risque introduit par
    # moi, retiré avant tout déploiement.
    # RÈGLE : on ne collecte QUE si l'on vient de relâcher NOTRE PROPRE handle sur ce nom — là,
    # la génération morte est la nôtre et personne d'autre ne peut la libérer. Un échec sur un
    # flux qu'on n'a jamais ouvert appartient à quelqu'un d'autre : ce n'est pas à nous de le
    # ramasser, et le diagnostic du C++ dit déjà quoi faire (supprimer le flux périmé à la main).
    if _libere_notre_handle:
        try: inst.garbage_collect()
        except Exception: pass
    _ech = _in_echec.get(idx)      # sert au BORNAGE DU JOURNAL ci-dessous, plus à la collecte
    try:
        r = bobimxl.Reader(inst, wanted)   # lève si le flux n'existe pas encore
        input_handles[idx] = (r, wanted)
        _in_echec.pop(idx, None)
        log(f"input #{{idx}} câblé sur flux MXL {{wanted}}", "info")
        return r
    except Exception as e:
        # ★ NE PLUS ÉCHOUER EN SILENCE. Cet `except` rendait None sans un mot : une entrée noire
        # dont PERSONNE ne pouvait dire pourquoi, ni même qu'une ouverture échouait. On journalise
        # une fois par RETENTE_GC_S et par entrée (pas 50 fois par seconde), avec la raison.
        if _ech is None or (time.monotonic() - _ech) > RETENTE_GC_S:
            log(f"input #{{idx}}: cannot open MXL flow {{wanted}} ({{type(e).__name__}}: {{e}}) "
                f"— retrying, GC done", "warning")
        _in_echec[idx] = time.monotonic()
        return None


def _in_header(idx):
    """(frame_index, write_ts_ns) du producteur de l'entrée `idx`, ou (0, 0) si indispo.
    Sert au verrou de cadence (réf) et à l'alignement par media-timestamp (genlock).
    En MXL : index = headIndex courant, ts = dernière écriture (TAI)."""
    r = ensure_input(idx)
    if r is None: return 0, 0
    try:
        h = r.head_index()
        if h == bobimxl.MXL_UNDEFINED_INDEX: return 0, 0
        return h, r.last_write_time()
    except Exception: return 0, 0


# ─── Sources ENTRELACÉES NATIVES (modèle SDK MXL : 1 grain = 1 CHAMP, ½ hauteur) ───────────
# Le mixer compose sur un canvas de TRAME (WIDTH×HEIGHT progressif). Une source entrelacée
# livre des grains de V_FRAME_SIZE/2 octets : on TISSE les 2 grains-champs de la dernière trame
# complète (index PAIR = 1er champ ; tff → lignes paires, bff → impaires — convention unique du
# projet, cf. recorder/2110_io) pour retrouver une trame pleine. Sans ça, le grain-champ était
# silencieusement REJETÉ (taille ≠ canvas) → entrée noire.
_IL_SRC = {{}}   # nom de flux -> "tff"/"bff" (None = progressif)

def _il_order(r):
    """Ordre de champ de la source (None si progressive). Lu UNE fois dans le flow_def."""
    key = getattr(r, "name", None) or id(r)
    if key in _IL_SRC:
        return _IL_SRC[key]
    fo = None
    try:
        f = r.format()
        if f and f.get("interlaced"):
            fo = str(f.get("field_order") or "tff").lower()
    except Exception:
        f = None
    _IL_SRC[key] = fo
    return fo

def _weave_fields(first_b, second_b, bff):
    """Tisse 2 grains-champs en une trame planar pleine (Y puis U puis V)."""
    planes = [(Y_SIZE // 2, WIDTH * _BPS),
              (UV_SIZE // 2, UV_W * _BPS), (UV_SIZE // 2, UV_W * _BPS)]
    out = bytearray(); off = 0
    for (psz, rb) in planes:
        a = np.frombuffer(first_b,  dtype=np.uint8, count=psz, offset=off).reshape(-1, rb)
        b = np.frombuffer(second_b, dtype=np.uint8, count=psz, offset=off).reshape(-1, rb)
        fr = np.empty((a.shape[0] * 2, rb), dtype=np.uint8)
        if bff: fr[1::2] = a; fr[0::2] = b
        else:   fr[0::2] = a; fr[1::2] = b
        out += fr.tobytes(); off += psz
    return bytes(out)

def lire_frame_yuv(r):
    """Lit la frame la plus récente du Reader. Renvoie (bytes, ts_in_ns) ou (None, None).
    Source ENTRELACÉE (grain = champ) → tissage des 2 champs en une trame pleine."""
    if r is None: return None, None
    try:
        got = r.get_latest()
        if got is None: return None, None
        n = len(got[2])
        if n == V_FRAME_SIZE // 2:
            fo = _il_order(r)
            if fo is None: return None, None       # ½ canvas mais progressif → format ≠ → ignoré
            fi = (got[0] - 1) // 2                 # trame complète = champs 2·fi et 2·fi+1
            if fi < 0: return None, None
            g0 = r.get(2 * fi); g1 = r.get(2 * fi + 1)
            if (g0 is None or g1 is None
                    or len(g0[2]) != n or len(g1[2]) != n): return None, None
            return _weave_fields(bytes(g0[2]), bytes(g1[2]), fo == "bff"), r.last_write_time()
        if n != V_FRAME_SIZE: return None, None    # format ≠ programme → ignoré
        return bytes(got[2]), r.last_write_time()
    except Exception:
        return None, None


# ─── Compositing helpers ────────────────────────────────────
def blend_yuv(yuv_a, yuv_b, alpha):
    """Dissolve classique (planes Y/U/V en moyenne pondérée). Total des contributions = 1."""
    if _MVK:
        try:
            ca = 1.0 - alpha; cb = float(alpha)
            Ya = np.frombuffer(yuv_a, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Yb = np.frombuffer(yuv_b, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Ua = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Ub = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Va = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            Vb = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            out_y = np.empty((HEIGHT, WIDTH), dtype=_NP_DT)
            out_u = np.empty((UV_H, UV_W), dtype=_NP_DT)
            out_v = np.empty((UV_H, UV_W), dtype=_NP_DT)
            if (bobimxl.mvk_mixf_into(out_y, Ya, Yb, ca, cb) and
                    bobimxl.mvk_mixf_into(out_u, Ua, Ub, ca, cb) and
                    bobimxl.mvk_mixf_into(out_v, Va, Vb, ca, cb)):
                return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()
        except Exception:
            pass
    a = np.frombuffer(yuv_a, dtype=_NP_DT).astype(np.float32)
    b = np.frombuffer(yuv_b, dtype=_NP_DT).astype(np.float32)
    out = a * (1.0 - alpha) + b * alpha
    return out.astype(_NP_DT).tobytes()

def blend_yuv_additive(yuv_a, yuv_b, alpha):
    """Mix additif : à α=0.5 les deux luma s'ajoutent (clip 255). Chroma reste en blend
    pondéré pour ne pas saturer la couleur. Coef A descend de 1 à 0 entre α=0.5 et α=1 ;
    Coef B monte de 0 à 1 entre α=0 et α=0.5, puis reste à 1."""
    coef_a = min(1.0, 2.0 * (1.0 - alpha))
    coef_b = min(1.0, 2.0 * alpha)
    if _MVK:
        try:
            Ya2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Yb2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Ua2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Ub2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Va2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            Vb2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            out_y = np.empty((HEIGHT, WIDTH), dtype=_NP_DT)
            out_u = np.empty((UV_H, UV_W), dtype=_NP_DT)
            out_v = np.empty((UV_H, UV_W), dtype=_NP_DT)
            if (bobimxl.mvk_mixf_into(out_y, Ya2, Yb2, coef_a, coef_b, clip=True, maxv=_MAXV) and
                    bobimxl.mvk_mixf_into(out_u, Ua2, Ub2, 1.0 - alpha, alpha) and
                    bobimxl.mvk_mixf_into(out_v, Va2, Vb2, 1.0 - alpha, alpha)):
                return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()
        except Exception:
            pass
    Ya = np.frombuffer(yuv_a[:Y_SIZE], dtype=_NP_DT).astype(np.float32)
    Yb = np.frombuffer(yuv_b[:Y_SIZE], dtype=_NP_DT).astype(np.float32)
    out_y = np.clip(Ya * coef_a + Yb * coef_b, 0, _MAXV).astype(_NP_DT).tobytes()
    Ua = np.frombuffer(yuv_a[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).astype(np.float32)
    Ub = np.frombuffer(yuv_b[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).astype(np.float32)
    Va = np.frombuffer(yuv_a[Y_SIZE+UV_SIZE:], dtype=_NP_DT).astype(np.float32)
    Vb = np.frombuffer(yuv_b[Y_SIZE+UV_SIZE:], dtype=_NP_DT).astype(np.float32)
    out_u = (Ua * (1.0 - alpha) + Ub * alpha).astype(_NP_DT).tobytes()
    out_v = (Va * (1.0 - alpha) + Vb * alpha).astype(_NP_DT).tobytes()
    return out_y + out_u + out_v

# Caches paresseux des grilles de coordonnées (init au 1er wipe).
_wipe_xc = None; _wipe_yc = None; _wipe_dc = None; _wipe_ic = None

# ─── Heart wipe : profondeur précalculée en tâche de fond ──
# Pour chaque pixel, on cherche la plus petite échelle s telle que le coeur (équation
# (X² + Y² - 1)³ - X²·Y³ ≤ 0) contienne le pixel. Stocké comme depth ∈ [0..1] (1 = corner).
# Calcul fait async pour ne pas bloquer le rendu vidéo.
_heart_depth = None
_heart_ready = threading.Event()
_heart_compute_started = False
_heart_compute_lock = threading.Lock()

def _compute_heart_depth():
    global _heart_depth
    log("Précalcul du wipe Cœur...", "info")
    t0 = time.time()
    _ensure_wipe_coords()
    cx, cy = WIDTH / 2.0, HEIGHT / 2.0
    max_scale = 2.2 * float(np.sqrt((WIDTH / 2.0) ** 2 + (HEIGHT / 2.0) ** 2))
    K = 64
    Xc = (_wipe_xc - cx).astype(np.float32)
    Yc = (cy - _wipe_yc).astype(np.float32)
    depth = np.full((HEIGHT, WIDTH), 2.0, dtype=np.float32)
    for k in range(1, K + 1):
        s = max_scale * k / K
        X = Xc / s; Y = Yc / s
        v = X * X + Y * Y - 1.0
        inside = (v * v * v - X * X * Y * Y * Y) <= 0
        mask = inside & (depth > 1.0)
        depth = np.where(mask, np.float32(k / K), depth)
    _heart_depth = depth
    _heart_ready.set()
    log(f"Wipe Cœur prêt en {{time.time() - t0:.1f}}s", "info")

def _kick_heart_precompute():
    global _heart_compute_started
    with _heart_compute_lock:
        if _heart_compute_started: return
        _heart_compute_started = True
    threading.Thread(target=_compute_heart_depth, daemon=True).start()

def _ensure_wipe_coords():
    global _wipe_xc, _wipe_yc, _wipe_dc, _wipe_ic
    if _wipe_xc is not None: return
    _wipe_xc = np.tile(np.arange(WIDTH,  dtype=np.float32),         (HEIGHT, 1))
    _wipe_yc = np.tile(np.arange(HEIGHT, dtype=np.float32)[:, None], (1, WIDTH))
    _wipe_dc = _wipe_xc + _wipe_yc
    dx = _wipe_xc - WIDTH  / 2.0
    dy = _wipe_yc - HEIGHT / 2.0
    _wipe_ic = np.sqrt(dx * dx + dy * dy)

def _wipe_alpha_map(alpha, direction, reverse, softness_frac):
    """Renvoie un masque float32 (HEIGHT, WIDTH) : 0 = A visible, 1 = B visible.
    Le bord transitoire glisse de hors-cadre (α=0) à hors-cadre (α=1) — donc α=0
    et α=1 produisent un cadre uniforme A ou B."""
    _ensure_wipe_coords()
    if direction == "vertical":
        coord = _wipe_yc if not reverse else (HEIGHT - 1 - _wipe_yc)
        max_c = HEIGHT - 1
        soft  = max(1.0, softness_frac * HEIGHT)
    elif direction == "diagonal":
        max_c = WIDTH + HEIGHT - 2
        coord = _wipe_dc if not reverse else (max_c - _wipe_dc)
        soft  = max(1.0, softness_frac * max_c)
    elif direction == "iris":
        coord = _wipe_ic
        max_c = float(np.sqrt((WIDTH / 2.0) ** 2 + (HEIGHT / 2.0) ** 2))
        soft  = max(1.0, softness_frac * max_c)
        if reverse:
            # Iris fermant : le bord part de r_max et se contracte vers 0.
            p = max_c + soft / 2 - alpha * (max_c + soft)
            return np.clip(0.5 + (coord - p) / soft, 0.0, 1.0).astype(np.float32)
        # Iris ouvrant (par défaut) : le bord part de 0 et s'étend.
        p = -soft / 2 + alpha * (max_c + soft)
        return np.clip(0.5 + (p - coord) / soft, 0.0, 1.0).astype(np.float32)
    elif direction == "heart":
        _kick_heart_precompute()
        if not _heart_ready.is_set() or _heart_depth is None:
            # Heart pas encore prêt : on retombe sur un dissolve uniforme.
            return np.full((HEIGHT, WIDTH), float(alpha), dtype=np.float32)
        soft = max(0.001, softness_frac)
        if reverse:
            edge = (1.0 + soft) - alpha * (1.0 + 2 * soft)
            return np.clip(0.5 + (_heart_depth - edge) / soft, 0.0, 1.0).astype(np.float32)
        edge = -soft + alpha * (1.0 + 2 * soft)
        return np.clip(0.5 + (edge - _heart_depth) / soft, 0.0, 1.0).astype(np.float32)
    else:   # horizontal (défaut)
        coord = _wipe_xc if not reverse else (WIDTH - 1 - _wipe_xc)
        max_c = WIDTH - 1
        soft  = max(1.0, softness_frac * WIDTH)
    p = -soft / 2 + alpha * (max_c + soft)
    return np.clip(0.5 + (p - coord) / soft, 0.0, 1.0).astype(np.float32)

def blend_yuv_wipe(yuv_a, yuv_b, alpha, direction, reverse, softness):
    a_full = _wipe_alpha_map(alpha, direction, reverse, softness)
    if _MVK:
        try:
            Ya2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Yb2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
            Ua2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Ub2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W)
            Va2 = np.frombuffer(yuv_a, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            Vb2 = np.frombuffer(yuv_b, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
            out_y = np.empty((HEIGHT, WIDTH), dtype=_NP_DT)
            out_u = np.empty((UV_H, UV_W), dtype=_NP_DT)
            out_v = np.empty((UV_H, UV_W), dtype=_NP_DT)
            # Chroma : masque plein (pas de matérialisation du demi-masque), sous-échantillonné
            # en lignes (a_full[::_CH]) et en colonnes via m_colstep=_CW côté kernel C.
            mask_half = a_full[::_CH]
            if (bobimxl.mvk_mixmap_into(out_y, Ya2, Yb2, a_full) and
                    bobimxl.mvk_mixmap_into(out_u, Ua2, Ub2, mask_half, m_colstep=_CW) and
                    bobimxl.mvk_mixmap_into(out_v, Va2, Vb2, mask_half, m_colstep=_CW)):
                return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()
        except Exception:
            pass
    a_half = a_full[::_CH, ::_CW]
    Ya = np.frombuffer(yuv_a[:Y_SIZE], dtype=_NP_DT).reshape(HEIGHT, WIDTH).astype(np.float32)
    Yb = np.frombuffer(yuv_b[:Y_SIZE], dtype=_NP_DT).reshape(HEIGHT, WIDTH).astype(np.float32)
    out_y = (Ya * (1.0 - a_full) + Yb * a_full).astype(_NP_DT).tobytes()
    Ua = np.frombuffer(yuv_a[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).reshape(UV_H, UV_W).astype(np.float32)
    Ub = np.frombuffer(yuv_b[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).reshape(UV_H, UV_W).astype(np.float32)
    out_u = (Ua * (1.0 - a_half) + Ub * a_half).astype(_NP_DT).tobytes()
    Va = np.frombuffer(yuv_a[Y_SIZE+UV_SIZE:], dtype=_NP_DT).reshape(UV_H, UV_W).astype(np.float32)
    Vb = np.frombuffer(yuv_b[Y_SIZE+UV_SIZE:], dtype=_NP_DT).reshape(UV_H, UV_W).astype(np.float32)
    out_v = (Va * (1.0 - a_half) + Vb * a_half).astype(_NP_DT).tobytes()
    return out_y + out_u + out_v

def blend_yuv_preset(yuv_a, yuv_b, alpha, preset):
    """Dispatch sur le mode du preset snapshot."""
    if preset is None:
        return blend_yuv(yuv_a, yuv_b, alpha)
    mode = preset.get("mode", "dissolve")
    if mode == "additif":
        return blend_yuv_additive(yuv_a, yuv_b, alpha)
    if mode == "wipe":
        w = preset.get("wipe") or {{}}
        return blend_yuv_wipe(
            yuv_a, yuv_b, alpha,
            w.get("direction", "horizontal"),
            bool(w.get("reverse", False)),
            float(w.get("softness", 0.10)),
        )
    return blend_yuv(yuv_a, yuv_b, alpha)


def composite_keyer_yuv(bgd_yuv, keyer_yuv, keyer_alpha, kx, ky, kscale, border_w):
    """Compose le keyer (resized via nearest-neighbor) sur bgd_yuv en YUV420.
    Le keyer occupe un rectangle centré sur (kx*W, ky*H), de taille kscale*W × kscale*H
    (aspect du source préservé via min). Une bordure blanche optionnelle entoure le rect."""
    if keyer_alpha <= 0.001 or keyer_yuv is None:
        return bgd_yuv
    # Dimensions de l'image keyer dans l'output, conservant l'aspect 16:9 de la source.
    dst_w = max(2, int(WIDTH  * kscale))
    dst_h = max(2, int(HEIGHT * kscale))
    if dst_w % 2: dst_w -= 1
    if dst_h % 2: dst_h -= 1

    # Position (centre)
    cx = kx * WIDTH; cy = ky * HEIGHT
    x0 = int(cx - dst_w / 2); y0 = int(cy - dst_h / 2)
    if x0 % 2: x0 -= 1
    if y0 % 2: y0 -= 1

    # Clipping aux bornes de l'output
    out_x0 = max(0, x0); out_y0 = max(0, y0)
    out_x1 = min(WIDTH,  x0 + dst_w); out_y1 = min(HEIGHT, y0 + dst_h)
    if out_x1 <= out_x0 or out_y1 <= out_y0:
        return bgd_yuv
    src_x_off = out_x0 - x0   # décalage de coupe à gauche (positif si rect dépasse à gauche)
    src_y_off = out_y0 - y0
    vis_w = out_x1 - out_x0
    vis_h = out_y1 - out_y0

    # Resize nearest-neighbor du source vers (dst_w, dst_h).
    src_y = np.frombuffer(keyer_yuv[:Y_SIZE], dtype=_NP_DT).reshape(HEIGHT, WIDTH)
    src_u = np.frombuffer(keyer_yuv[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).reshape(UV_H, UV_W)
    src_v = np.frombuffer(keyer_yuv[Y_SIZE+UV_SIZE:], dtype=_NP_DT).reshape(UV_H, UV_W)
    ix  = (np.arange(dst_w) * WIDTH  // dst_w)
    iy  = (np.arange(dst_h) * HEIGHT // dst_h)
    ix2 = (np.arange(dst_w // _CW) * UV_W // (dst_w // _CW))
    iy2 = (np.arange(dst_h // _CH) * UV_H // (dst_h // _CH))

    # Composite dans bgd
    bgd_y = np.frombuffer(bgd_yuv[:Y_SIZE], dtype=_NP_DT).reshape(HEIGHT, WIDTH).copy()
    bgd_u = np.frombuffer(bgd_yuv[Y_SIZE:Y_SIZE+UV_SIZE], dtype=_NP_DT).reshape(UV_H, UV_W).copy()
    bgd_v = np.frombuffer(bgd_yuv[Y_SIZE+UV_SIZE:], dtype=_NP_DT).reshape(UV_H, UV_W).copy()

    _mvk_ok = False
    if _MVK:
        try:
            # Indices SOURCE absolus déjà restreints à la zone visible (mêmes valeurs que la
            # crop numpy ci-dessous : ix/iy dépendent de la position DANS dst_w/dst_h, pas de
            # la fenêtre visible). mvk_place_into fait gather + resize en une passe mémoire.
            row_y = iy[src_y_off:src_y_off+vis_h].astype(np.int32)
            col_y = ix[src_x_off:src_x_off+vis_w].astype(np.int32)
            row_c = iy2[src_y_off//_CH:(src_y_off+vis_h)//_CH].astype(np.int32)
            col_c = ix2[src_x_off//_CW:(src_x_off+vis_w)//_CW].astype(np.int32)
            k_y = np.empty((row_y.size, col_y.size), dtype=_NP_DT)
            k_u = np.empty((row_c.size, col_c.size), dtype=_NP_DT)
            k_v = np.empty((row_c.size, col_c.size), dtype=_NP_DT)
            dst_y = bgd_y[out_y0:out_y1, out_x0:out_x1]
            dst_u = bgd_u[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW]
            dst_v = bgd_v[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW]
            # Blend région : dst ← reg·(1−kα) + k·kα, dst ALIASE a (supporté par mvk_mixf_into).
            # INVARIANT repli : rien ne doit muter bgd_* tant qu'un échec est possible (sinon le
            # repli numpy re-blenderait par-dessus → double blend). Les place_into ne touchent
            # pas bgd ; on vérifie les formes AVANT les mixf (leurs seules causes d'échec ici),
            # puis les 3 mixf ne peuvent plus échouer ensemble ou séparément.
            _mvk_ok = (
                bobimxl.mvk_place_into(k_y, src_y, row_y, col_idx=col_y) and
                bobimxl.mvk_place_into(k_u, src_u, row_c, col_idx=col_c) and
                bobimxl.mvk_place_into(k_v, src_v, row_c, col_idx=col_c) and
                dst_y.shape == k_y.shape and dst_u.shape == k_u.shape
                and dst_v.shape == k_v.shape)
            if _mvk_ok:
                _mvk_ok = (
                    bobimxl.mvk_mixf_into(dst_y, dst_y, k_y, 1.0 - keyer_alpha, keyer_alpha) and
                    bobimxl.mvk_mixf_into(dst_u, dst_u, k_u, 1.0 - keyer_alpha, keyer_alpha) and
                    bobimxl.mvk_mixf_into(dst_v, dst_v, k_v, 1.0 - keyer_alpha, keyer_alpha))
        except Exception:
            _mvk_ok = False

    if not _mvk_ok:
        k_y_full = src_y[iy[:, None], ix[None, :]]
        k_u_full = src_u[iy2[:, None], ix2[None, :]]
        k_v_full = src_v[iy2[:, None], ix2[None, :]]

        # Crop à la zone visible
        k_y = k_y_full[src_y_off:src_y_off+vis_h, src_x_off:src_x_off+vis_w]
        k_u = k_u_full[src_y_off//_CH:(src_y_off+vis_h)//_CH, src_x_off//_CW:(src_x_off+vis_w)//_CW]
        k_v = k_v_full[src_y_off//_CH:(src_y_off+vis_h)//_CH, src_x_off//_CW:(src_x_off+vis_w)//_CW]

        reg_y = bgd_y[out_y0:out_y1, out_x0:out_x1].astype(np.float32)
        bgd_y[out_y0:out_y1, out_x0:out_x1] = (reg_y * (1.0 - keyer_alpha) +
                                                k_y.astype(np.float32) * keyer_alpha).astype(_NP_DT)
        reg_u = bgd_u[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW].astype(np.float32)
        bgd_u[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW] = (reg_u * (1.0 - keyer_alpha) +
                                                           k_u.astype(np.float32) * keyer_alpha).astype(_NP_DT)
        reg_v = bgd_v[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW].astype(np.float32)
        bgd_v[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW] = (reg_v * (1.0 - keyer_alpha) +
                                                           k_v.astype(np.float32) * keyer_alpha).astype(_NP_DT)

    # Bordure blanche (BT.601 : Y=235, U=V=128). Tracée avec le même alpha.
    if border_w > 0:
        bw = int(border_w)
        def _stripe(y0_, y1_, x0_, x1_):
            if y1_ <= y0_ or x1_ <= x0_: return
            reg = bgd_y[y0_:y1_, x0_:x1_].astype(np.float32)
            bgd_y[y0_:y1_, x0_:x1_] = (reg * (1.0 - keyer_alpha) + _WHITEf * keyer_alpha).astype(_NP_DT)
            reg = bgd_u[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW].astype(np.float32)
            bgd_u[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW] = (reg * (1.0 - keyer_alpha) + _NEUTf * keyer_alpha).astype(_NP_DT)
            reg = bgd_v[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW].astype(np.float32)
            bgd_v[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW] = (reg * (1.0 - keyer_alpha) + _NEUTf * keyer_alpha).astype(_NP_DT)
        bx0 = max(0, x0); bx1 = min(WIDTH, x0 + dst_w)
        by0 = max(0, y0); by1 = min(HEIGHT, y0 + dst_h)
        _stripe(max(0, y0),                   min(HEIGHT, y0 + bw),        bx0, bx1)  # haut
        _stripe(max(0, y0 + dst_h - bw),      min(HEIGHT, y0 + dst_h),     bx0, bx1)  # bas
        _stripe(by0, by1, max(0, x0),                 min(WIDTH,  x0 + bw))             # gauche
        _stripe(by0, by1, max(0, x0 + dst_w - bw),    min(WIDTH,  x0 + dst_w))          # droite

    return bgd_y.tobytes() + bgd_u.tobytes() + bgd_v.tobytes()


def prep_overlay(ovl_rgba):
    """RGBA (H, W, 4) → overlay PRÉ-CONVERTI en YUV : (y, u, v, alpha) prêt à blender.

    Appelé UNE FOIS PAR CAPTURE (thread overlay_loop, cadence 40 ms), PAS par trame — la
    conversion RGB→YUV de l'habillage ne dépend que de l'habillage. Le blend alpha est AFFINE,
    donc il COMMUTE avec la conversion RGB↔YUV : blender en RGB puis convertir (ce que faisait
    composite_overlay) et convertir l'overlay puis blender en YUV donnent le même résultat —
    sauf que la 1ʳᵉ forme imposait un roundtrip YUV→RGB→YUV PLEIN CADRE par trame (banc
    2026-07-14, 1080p : 263 ms/trame, soit 6,5× le budget 25p) et, pire, n'était pas
    l'IDENTITÉ : son clip + sa requantification RGB dégradaient TOUTE l'image, y compris les
    pixels que l'overlay ne touche pas (99 % des pixels Y modifiés, jusqu'à 6 LSB, alpha=0).
    Ici la source n'est plus jamais reconvertie : hors overlay, les octets sortent intacts.

    Coefficients BT.601 STUDIO-range (0.257/0.504/0.098 + 16 ; ⚠ mvk_rgba2yuv est en
    FULL-range 0.299/0.587/0.114 → inutilisable ici sans décaler les niveaux de l'habillage).
    Chroma sous-échantillonnée par POINT (comme composite_overlay historique), plans matérialisés
    contigus (les kernels mvk exigent le dernier axe contigu — cf. l'alpha stridé du multiview)."""
    r = ovl_rgba[:, :, 0].astype(np.float32)
    g = ovl_rgba[:, :, 1].astype(np.float32)
    b = ovl_rgba[:, :, 2].astype(np.float32)
    y = np.clip(( 0.257*r + 0.504*g + 0.098*b + 16.0)  * _SCALE, 0, _MAXV).astype(_NP_DT)
    # U/V calculées SUR LE RGB DÉJÀ SOUS-ÉCHANTILLONNÉ (÷4 d'ops) : le chroma est point-sampled
    # (composite_overlay prenait u_p[::_CH, ::_CW]), et point-sampler une expression per-pixel
    # revient à l'évaluer sur les pixels point-samplés → strictement les mêmes octets.
    rs = np.ascontiguousarray(r[::_CH, ::_CW])
    gs = np.ascontiguousarray(g[::_CH, ::_CW])
    bs = np.ascontiguousarray(b[::_CH, ::_CW])
    u = np.clip((-0.148*rs - 0.291*gs + 0.439*bs + 128.0) * _SCALE, 0, _MAXV).astype(_NP_DT)
    v = np.clip(( 0.439*rs - 0.368*gs - 0.071*bs + 128.0) * _SCALE, 0, _MAXV).astype(_NP_DT)
    return dict(
        y=np.ascontiguousarray(y),
        u=np.ascontiguousarray(u),
        v=np.ascontiguousarray(v),
        # masque alpha PLEIN (float32 contigu) : le chroma le ré-échantillonne en lignes
        # (a[::_CH]) et en colonnes via m_colstep=_CW côté kernel — contrat mvk_mixmap_into.
        a=np.ascontiguousarray(ovl_rgba[:, :, 3].astype(np.float32) / 255.0),
    )


def composite_overlay(yuv_bytes, ovl):
    """Compose l'overlay PRÉ-CONVERTI (cf. prep_overlay) sur une frame yuv, EN YUV."""
    src_y = np.frombuffer(yuv_bytes, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH)
    src_u = np.frombuffer(yuv_bytes, dtype=_NP_DT, count=UV_SIZE // _BPS,
                          offset=Y_SIZE).reshape(UV_H, UV_W)
    src_v = np.frombuffer(yuv_bytes, dtype=_NP_DT, count=UV_SIZE // _BPS,
                          offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W)
    a_full = ovl["a"]
    if _MVK:
        try:
            out_y = np.empty((HEIGHT, WIDTH), dtype=_NP_DT)
            out_u = np.empty((UV_H, UV_W), dtype=_NP_DT)
            out_v = np.empty((UV_H, UV_W), dtype=_NP_DT)
            a_half = a_full[::_CH]     # cf. blend_yuv_wipe : masque plein, lignes décimées
            if (bobimxl.mvk_mixmap_into(out_y, src_y, ovl["y"], a_full) and
                    bobimxl.mvk_mixmap_into(out_u, src_u, ovl["u"], a_half, m_colstep=_CW) and
                    bobimxl.mvk_mixmap_into(out_v, src_v, ovl["v"], a_half, m_colstep=_CW)):
                return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()
        except Exception:
            pass
    ah = a_full[::_CH, ::_CW]
    out_y = (src_y.astype(np.float32) * (1.0 - a_full) +
             ovl["y"].astype(np.float32) * a_full).astype(_NP_DT)
    out_u = (src_u.astype(np.float32) * (1.0 - ah) +
             ovl["u"].astype(np.float32) * ah).astype(_NP_DT)
    out_v = (src_v.astype(np.float32) * (1.0 - ah) +
             ovl["v"].astype(np.float32) * ah).astype(_NP_DT)
    return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()


def _calc_keyer_alpha(keyer_state, manual, trans):
    """Alpha du keyer pour LA trame (statique ou en transition). Évalué UNE fois par trame —
    LATCH d'état par grain : jamais de changement à mi-trame (partagé historique/tranche)."""
    keyer_alpha_now = 1.0 if keyer_state["active"] else 0.0
    if manual is not None and manual.get("keyer"):
        p = max(0.0, min(1.0, manual["alpha"]))
        fr = 1.0 if manual["keyer"]["from_active"] else 0.0
        to = 1.0 if manual["keyer"]["to_active"]   else 0.0
        keyer_alpha_now = fr + (to - fr) * p
    elif trans is not None and trans.get("keyer"):
        p = max(0.0, min(1.0,
            (time.time() - trans["start"]) * 1000.0 / trans["duration_ms"]))
        fr = 1.0 if trans["keyer"]["from_active"] else 0.0
        to = 1.0 if trans["keyer"]["to_active"]   else 0.0
        keyer_alpha_now = fr + (to - fr) * p
    return keyer_alpha_now


# ─── MODE TRANCHE : état runtime + compose bandé ────────────────────────────────────────────
# Actif seulement si SLICE_ON — le chemin historique whole-frame reste STRICTEMENT intact.
_sl_backoff  = {{}}   # slot → [fi bloqué, timeouts consécutifs] ; 2 timeouts même grain → dormante
_sl_late_win = {{}}   # shm → deque(0/1) fenêtre ~10 s du taux de replis (garde-fou n°2)
_sl_flagged  = {{}}   # shm → pct : entrées signalées (hystérésis SLICE_LATE_PCT / _SL_CLEAR_PCT)
_sl_dbg      = None   # [entrées, valid0, attentes, replis, dormantes] de la dernière trame

_BLACK_Yi = 16 << (BIT_DEPTH - 8)     # noir Y (scalaire dtype pipeline)
_NEUT_Ci  = 1 << (BIT_DEPTH - 1)      # chroma neutre


def _sl_bind(slot, idx_ref, ts_in_per_slot, dbg):
    """Ouvre l'entrée `slot` pour la trame d'index idx_ref (VERROU-RÉFÉRENCE INDEXÉ) :
    sur-grille (|tête − idx_ref| ≤ 2) → suivie À idx_ref (get_slice(idx_ref, need_k)) ;
    HORS-GRILLE → suivie par SA tête (dégradation propre, skew mesuré comme avant).
    Renvoie l'état par-entrée du compose bandé (vues zéro-copie sur le grain, tranches valides,
    budget d'attente) ou None si indisponible / format ≠ programme."""
    r = ensure_input(slot)
    if r is None:
        return None
    try:
        h = r.head_index()
        if h == bobimxl.MXL_UNDEFINED_INDEX:
            return None
        h = int(h)
        fi = idx_ref if abs(h - idx_ref) <= 2 else h
        got = None; static = False
        _bk = _sl_backoff.get(slot)
        if _bk is not None and fi > _bk[0]:
            _sl_backoff.pop(slot, None); _bk = None   # tête avancée → réarme la dormante
        if _bk is not None and _bk[1] >= 2:
            # DORMANTE (backoff, garde-fou n°1) : 2 timeouts consécutifs sur le MÊME grain
            # (producteur mort mi-grain) → dernier grain complet direct, ZÉRO sonde, tant que
            # la tête n'avance pas — elle ne consomme plus aucun budget.
            dbg[4] += 1
            got = r.get(fi - 1, timeout_ns=2_000_000) or r.get_latest()
            static = True
        else:
            # 1ʳᵉ tranche du grain ciblé (réveil au commit partiel) ; pas encore là / flux
            # whole-frame sans le patch slices → dernier grain complet (dégénéré, 0 attente).
            got = r.get_slice(fi, 1, timeout_ns=3_000_000)
            if got is None:
                got = r.get_latest(); static = True
        if got is not None and got[2].size == V_FRAME_SIZE // 2 and _il_order(r) is not None:
            # Source ENTRELACÉE (grain = champ) : PAS de mode tranche possible (le producteur
            # committe par CHAMPS) → repli whole-frame TISSÉ, comme lire_frame_yuv.
            fr, ts = lire_frame_yuv(r)
            if fr is None:
                return None
            pl = np.frombuffer(fr, dtype=np.uint8)
            ts_in_per_slot[slot] = ts
            return dict(slot=slot, r=r, fi=int(got[0]), total=1, valid=1,
                        islh=HEIGHT, budget=int(1e9 / FPS), late=False,
                        y=pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                        u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                        v=pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))
        if got is None or got[2].size != V_FRAME_SIZE:
            return None    # rien de lisible, ou format ≠ programme (même garde que lire_frame_yuv)
        gi = got[1]
        total = max(1, int(gi.totalSlices or 1))
        pl = got[2]
        st = dict(slot=slot, r=r, fi=int(got[0]), total=total,
                  valid=(total if static else max(1, int(gi.validSlices or 1))),
                  islh=max(1, HEIGHT // total),
                  budget=int(1e9 / FPS),   # budget d'attente PAR ENTRÉE (motif multiview)
                  late=False,
                  y=pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                  u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                  v=pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))
        if dbg[1] < 0:
            dbg[1] = st["valid"]
        ts_in_per_slot[slot] = r.last_write_time()
        return st
    except Exception:
        return None


def _sl_wait(st, need_row, deadl, dbg):
    """Attend (get_slice, réveil au commit partiel) que l'entrée couvre la ligne image
    `need_row` — budget PAR ENTRÉE, borné par le budget de trame DUR `deadl` (garde-fou n°1).
    Dépassement → REPLI IMMÉDIAT sur le dernier grain complet (tearing d'UNE image sur CETTE
    entrée seulement, les bandes déjà posées restent celles du grain fi) + comptage backoff
    (2 timeouts même grain → dormante). Renvoie les ns réellement attendues (exclues de own)."""
    need_k = min(st["total"], need_row // st["islh"] + 1)
    if need_k <= st["valid"]:
        return 0
    dbg[2] += 1
    _left = min(deadl - time.monotonic(), st["budget"] / 1e9)
    _w0 = time.monotonic()
    g = st["r"].get_slice(st["fi"], need_k, timeout_ns=max(1, int(_left * 1e9))) if _left > 0 else None
    dw = int((time.monotonic() - _w0) * 1e9)
    st["budget"] -= dw
    if g is not None:
        st["valid"] = max(need_k, int(g[1].validSlices or need_k))
        return dw
    dbg[3] += 1
    st["late"] = True
    _bk = _sl_backoff.get(st["slot"])
    _sl_backoff[st["slot"]] = [st["fi"], (_bk[1] + 1 if _bk and _bk[0] == st["fi"] else 1)]
    try:
        gp = st["r"].get(st["fi"] - 1, timeout_ns=2_000_000)
    except Exception:
        gp = None
    if gp is not None and gp[2].size == V_FRAME_SIZE:
        pl = gp[2]
        st["y"] = pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH)
        st["u"] = pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
        st["v"] = pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
    st["valid"] = st["total"]     # plus aucune attente pour cette entrée cette trame
    return dw


def _sl_blend_band(oy, ou, ov, A, B, alpha, mode, wm, a, b):
    """Blend BGD de la bande [a,b) — MÊMES maths float32 que blend_yuv / blend_yuv_additive /
    blend_yuv_wipe (opérations per-pixel, ligne-locales) appliquées au DELTA de lignes →
    équivalence octet-à-octet bande ≡ plein (array_equal, cf. tests offline). Le masque wipe
    `wm` = (plein, chroma) est PRÉCALCULÉ par (alpha, préréglage) puis TRANCHÉ par bande."""
    ca = a // _CH; cb = b // _CH
    # Kernel C fusionné sur la BANDE — les vues de bande sont ligne-contigües, donc directement
    # acceptées par les kernels (mêmes appels que le chemin plein cadre, bit-exacts : le blend
    # est per-pixel, une bande est un sous-ensemble de lignes). Sans ce câblage, le mode tranche
    # retombait intégralement en numpy (banc 2026-07-14 : 18,0 ms vs 5,25 ms par trame 1080p).
    if _MVK:
        try:
            Ay_ = A["y"][a:b]; By_ = B["y"][a:b]
            Au_ = A["u"][ca:cb]; Bu_ = B["u"][ca:cb]
            Av_ = A["v"][ca:cb]; Bv_ = B["v"][ca:cb]
            if mode == "wipe" and wm is not None:
                if (bobimxl.mvk_mixmap_into(oy[a:b], Ay_, By_, wm[0][a:b]) and
                        bobimxl.mvk_mixmap_into(ou[ca:cb], Au_, Bu_, wm[1][ca:cb]) and
                        bobimxl.mvk_mixmap_into(ov[ca:cb], Av_, Bv_, wm[1][ca:cb])):
                    return
            elif mode == "additif":
                _ka = min(1.0, 2.0 * (1.0 - alpha)); _kb = min(1.0, 2.0 * alpha)
                if (bobimxl.mvk_mixf_into(oy[a:b], Ay_, By_, _ka, _kb, clip=True, maxv=_MAXV) and
                        bobimxl.mvk_mixf_into(ou[ca:cb], Au_, Bu_, 1.0 - alpha, alpha) and
                        bobimxl.mvk_mixf_into(ov[ca:cb], Av_, Bv_, 1.0 - alpha, alpha)):
                    return
            else:   # dissolve
                if (bobimxl.mvk_mixf_into(oy[a:b], Ay_, By_, 1.0 - alpha, alpha) and
                        bobimxl.mvk_mixf_into(ou[ca:cb], Au_, Bu_, 1.0 - alpha, alpha) and
                        bobimxl.mvk_mixf_into(ov[ca:cb], Av_, Bv_, 1.0 - alpha, alpha)):
                    return
        except Exception:
            pass   # repli numpy intégral ci-dessous (rien n'a été écrit : les kernels
                   # échouent AVANT d'écrire, ils valident formes/strides en amont)
    Ya = A["y"][a:b].astype(np.float32);   Yb = B["y"][a:b].astype(np.float32)
    Ua = A["u"][ca:cb].astype(np.float32); Ub = B["u"][ca:cb].astype(np.float32)
    Va = A["v"][ca:cb].astype(np.float32); Vb = B["v"][ca:cb].astype(np.float32)
    if mode == "wipe" and wm is not None:
        af = wm[0][a:b]; ah = wm[1][ca:cb]
        oy[a:b]   = (Ya * (1.0 - af) + Yb * af).astype(_NP_DT)
        ou[ca:cb] = (Ua * (1.0 - ah) + Ub * ah).astype(_NP_DT)
        ov[ca:cb] = (Va * (1.0 - ah) + Vb * ah).astype(_NP_DT)
    elif mode == "additif":
        coef_a = min(1.0, 2.0 * (1.0 - alpha))
        coef_b = min(1.0, 2.0 * alpha)
        oy[a:b]   = np.clip(Ya * coef_a + Yb * coef_b, 0, _MAXV).astype(_NP_DT)
        ou[ca:cb] = (Ua * (1.0 - alpha) + Ub * alpha).astype(_NP_DT)
        ov[ca:cb] = (Va * (1.0 - alpha) + Vb * alpha).astype(_NP_DT)
    else:   # dissolve (défaut)
        oy[a:b]   = (Ya * (1.0 - alpha) + Yb * alpha).astype(_NP_DT)
        ou[ca:cb] = (Ua * (1.0 - alpha) + Ub * alpha).astype(_NP_DT)
        ov[ca:cb] = (Va * (1.0 - alpha) + Vb * alpha).astype(_NP_DT)


def _sl_keyer_geom(ks):
    """Géométrie du keyer précalculée UNE fois par trame (latch) — mêmes calculs que
    composite_keyer_yuv (rect, clipping, index nearest-neighbor). None si invisible."""
    dst_w = max(2, int(WIDTH * ks["scale"])); dst_h = max(2, int(HEIGHT * ks["scale"]))
    if dst_w % 2: dst_w -= 1
    if dst_h % 2: dst_h -= 1
    cx = ks["x"] * WIDTH; cy = ks["y"] * HEIGHT
    x0 = int(cx - dst_w / 2); y0 = int(cy - dst_h / 2)
    if x0 % 2: x0 -= 1
    if y0 % 2: y0 -= 1
    out_x0 = max(0, x0); out_y0 = max(0, y0)
    out_x1 = min(WIDTH, x0 + dst_w); out_y1 = min(HEIGHT, y0 + dst_h)
    if out_x1 <= out_x0 or out_y1 <= out_y0:
        return None
    src_x_off = out_x0 - x0
    vis_w = out_x1 - out_x0
    ix  = (np.arange(dst_w) * WIDTH  // dst_w)
    iy  = (np.arange(dst_h) * HEIGHT // dst_h)
    ix2 = (np.arange(dst_w // _CW) * UV_W // (dst_w // _CW))
    iy2 = (np.arange(dst_h // _CH) * UV_H // (dst_h // _CH))
    return dict(x0=x0, y0=y0, dst_w=dst_w, dst_h=dst_h,
                out_x0=out_x0, out_y0=out_y0, out_x1=out_x1, out_y1=out_y1,
                iy=iy, iy2=iy2,
                # colonnes VISIBLES précalculées (crop gauche/droite intégré à l'index)
                ixv=ix[src_x_off:src_x_off + vis_w],
                ix2v=ix2[src_x_off // _CW:(src_x_off + vis_w) // _CW],
                bw=int(ks["border_width"]))


def _sl_keyer_need(g, a, b):
    """Dernière ligne image SOURCE du keyer requise par la bande [a,b) (nearest, Y + chroma —
    convention : k tranches ⇔ lignes [0, k·islh) valides sur les 3 plans). -1 si la bande
    n'intersecte pas le rect (bordure seule : aucune ligne source requise)."""
    nr = -1
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    if a2 < b2:
        nr = int(g["iy"][b2 - 1 - g["y0"]])
    ca2 = max(a // _CH, g["out_y0"] // _CH); cb2 = min(b // _CH, g["out_y1"] // _CH)
    if ca2 < cb2:
        jc = int(g["iy2"][cb2 - 1 - g["y0"] // _CH])
        nr = max(nr, (jc + 1) * _CH - 1)
    return nr


def _sl_stripe_band(py, pu, pv, ka, y0_, y1_, x0_, x1_, a, b):
    """Bordure blanche du keyer, restreinte aux lignes de la bande [a,b) — mêmes ops que
    _stripe (composite_keyer_yuv), les bandes partitionnent exactement les rangées chroma."""
    if y1_ <= y0_ or x1_ <= x0_:
        return
    r0 = max(y0_, a); r1 = min(y1_, b)
    if r0 < r1:
        reg = py[r0:r1, x0_:x1_].astype(np.float32)
        py[r0:r1, x0_:x1_] = (reg * (1.0 - ka) + _WHITEf * ka).astype(_NP_DT)
    c0 = max(y0_ // _CH, a // _CH); c1 = min(y1_ // _CH, b // _CH)
    if c0 < c1:
        cx0 = x0_ // _CW; cx1 = x1_ // _CW
        reg = pu[c0:c1, cx0:cx1].astype(np.float32)
        pu[c0:c1, cx0:cx1] = (reg * (1.0 - ka) + _NEUTf * ka).astype(_NP_DT)
        reg = pv[c0:c1, cx0:cx1].astype(np.float32)
        pv[c0:c1, cx0:cx1] = (reg * (1.0 - ka) + _NEUTf * ka).astype(_NP_DT)


def _sl_keyer_band(py, pu, pv, K, g, ka, a, b):
    """Compose le keyer sur la bande [a,b) du PGM — mêmes maths que composite_keyer_yuv
    (nearest + blend float32), en ne lisant du SOURCE que les lignes requises par la bande
    (gather par lignes → équivalence octet avec le fancy-index plein trame tranché)."""
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    if a2 < b2:
        sr = g["iy"][a2 - g["y0"]:b2 - g["y0"]]
        dst = py[a2:b2, g["out_x0"]:g["out_x1"]]
        k_y = None
        if _MVK:
            try:
                # gather + resize nearest EN UNE PASSE (mvk_place_into), puis blend région
                # in-place (dst aliase a) — même paire d'appels que composite_keyer_yuv.
                _k = np.empty(dst.shape, dtype=_NP_DT)
                if (bobimxl.mvk_place_into(_k, K["y"], sr.astype(np.int32),
                                           col_idx=g["ixv"].astype(np.int32)) and
                        bobimxl.mvk_mixf_into(dst, dst, _k, 1.0 - ka, ka)):
                    k_y = True
            except Exception:
                k_y = None
        if k_y is None:
            k_y = K["y"][sr][:, g["ixv"]]
            reg = dst.astype(np.float32)
            py[a2:b2, g["out_x0"]:g["out_x1"]] = (reg * (1.0 - ka) +
                                                  k_y.astype(np.float32) * ka).astype(_NP_DT)
    ca2 = max(a // _CH, g["out_y0"] // _CH); cb2 = min(b // _CH, g["out_y1"] // _CH)
    if ca2 < cb2:
        cx0 = g["out_x0"] // _CW; cx1 = g["out_x1"] // _CW
        src_c = g["iy2"][ca2 - g["y0"] // _CH:cb2 - g["y0"] // _CH]
        du = pu[ca2:cb2, cx0:cx1]; dv = pv[ca2:cb2, cx0:cx1]
        # INVARIANT repli (cf. composite_keyer_yuv) : U et V ont la MÊME géométrie et le même
        # dtype → les deux mixf réussissent ou échouent ENSEMBLE ; un kernel qui refuse n'a
        # rien écrit (validation formes/strides en amont) → le repli numpy ne double-blende pas.
        done = False
        if _MVK:
            try:
                _ku = np.empty(du.shape, dtype=_NP_DT)
                _kv = np.empty(dv.shape, dtype=_NP_DT)
                _rc = src_c.astype(np.int32); _cc = g["ix2v"].astype(np.int32)
                done = (bobimxl.mvk_place_into(_ku, K["u"], _rc, col_idx=_cc) and
                        bobimxl.mvk_place_into(_kv, K["v"], _rc, col_idx=_cc) and
                        bobimxl.mvk_mixf_into(du, du, _ku, 1.0 - ka, ka) and
                        bobimxl.mvk_mixf_into(dv, dv, _kv, 1.0 - ka, ka))
            except Exception:
                done = False
        if not done:
            k_u = K["u"][src_c][:, g["ix2v"]]
            k_v = K["v"][src_c][:, g["ix2v"]]
            reg = du.astype(np.float32)
            pu[ca2:cb2, cx0:cx1] = (reg * (1.0 - ka) + k_u.astype(np.float32) * ka).astype(_NP_DT)
            reg = dv.astype(np.float32)
            pv[ca2:cb2, cx0:cx1] = (reg * (1.0 - ka) + k_v.astype(np.float32) * ka).astype(_NP_DT)
    if g["bw"] > 0:
        bw = g["bw"]
        x0 = g["x0"]; y0 = g["y0"]; dst_w = g["dst_w"]; dst_h = g["dst_h"]
        bx0 = max(0, x0); bx1 = min(WIDTH, x0 + dst_w)
        by0 = max(0, y0); by1 = min(HEIGHT, y0 + dst_h)
        _sl_stripe_band(py, pu, pv, ka, max(0, y0),                min(HEIGHT, y0 + bw),    bx0, bx1, a, b)  # haut
        _sl_stripe_band(py, pu, pv, ka, max(0, y0 + dst_h - bw),   min(HEIGHT, y0 + dst_h), bx0, bx1, a, b)  # bas
        _sl_stripe_band(py, pu, pv, ka, by0, by1, max(0, x0),              min(WIDTH, x0 + bw),    a, b)     # gauche
        _sl_stripe_band(py, pu, pv, ka, by0, by1, max(0, x0 + dst_w - bw), min(WIDTH, x0 + dst_w), a, b)     # droite


def _sl_overlay_band(py, pu, pv, ovl, a, b):
    """Overlay PRÉ-CONVERTI (cf. prep_overlay) sur la bande [a,b) — MÊMES maths que
    composite_overlay (blend alpha en YUV, per-pixel donc ligne-local) appliquées au delta de
    lignes → équivalence octet bande ≡ plein (SLICE_LINES % _CH == 0). IN-PLACE : dst aliase
    la source a du kernel (supporté par mvk_mixmap_into)."""
    ca = a // _CH; cb = b // _CH
    a_full = ovl["a"][a:b]              # lignes contigües (dernier axe contigu → mvk OK)
    oy = ovl["y"][a:b]; ou = ovl["u"][ca:cb]; ov = ovl["v"][ca:cb]
    if _MVK:
        try:
            a_half = a_full[::_CH]      # masque plein, lignes décimées + m_colstep=_CW
            if (bobimxl.mvk_mixmap_into(py[a:b], py[a:b], oy, a_full) and
                    bobimxl.mvk_mixmap_into(pu[ca:cb], pu[ca:cb], ou, a_half, m_colstep=_CW) and
                    bobimxl.mvk_mixmap_into(pv[ca:cb], pv[ca:cb], ov, a_half, m_colstep=_CW)):
                return
        except Exception:
            pass
    ah = a_full[::_CH, ::_CW]
    py[a:b]   = (py[a:b].astype(np.float32) * (1.0 - a_full) +
                 oy.astype(np.float32) * a_full).astype(_NP_DT)
    pu[ca:cb] = (pu[ca:cb].astype(np.float32) * (1.0 - ah) +
                 ou.astype(np.float32) * ah).astype(_NP_DT)
    pv[ca:cb] = (pv[ca:cb].astype(np.float32) * (1.0 - ah) +
                 ov.astype(np.float32) * ah).astype(_NP_DT)


def _slice_frame(idx_ref, idx_out, bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, bgd_preset,
                 keyer_alpha, keyer_state, ovl, pgm, pvw, ts_in_per_slot):
    """MODE TRANCHE — compose et PUBLIE la trame BANDE PAR BANDE aux 3 sorties PGM/CLEAN/PVW.

    DEUX index, volontairement distincts : `idx_ref` = index du grain de l'entrée de RÉFÉRENCE,
    auquel on LIT toutes les entrées (verrou-référence indexé) ; `idx_out` = index auquel on
    ÉCRIT nos grains (grille TAI, cf. _ouvrir_sortie). Les confondre ferait dépendre la
    coordonnée de notre sortie d'une entrée — et reculer l'index si on alterne avec le chemin
    trame entière, dont l'index de grille est en avance d'un grain sur la tête de la référence.
    Entrées sur la grille (cas 2110 normal) : les deux coïncident au grain près.
    LATCH d'état par grain : TOUT l'état reçu en arguments (alpha T-bar, sélections PGM/PVW,
    préréglage wipe, keyer, overlay) a été FIGÉ par l'appelant à l'ouverture de la trame et
    sert pour TOUTES les bandes — jamais de changement d'état à mi-trame.
    GARDE-FOU n°1 : budget de trame DUR = 1,0×période depuis l'ouverture des grains de sortie
    (marge comprise) ; budget PAR ENTRÉE à l'intérieur ; entrée qui dépasse → REPLI IMMÉDIAT
    sur son dernier grain complet (tearing d'UNE image sur cette entrée seulement) ; backoff
    des mortes ; le commit final tombe TOUJOURS dans la période (try/finally + complément
    noir si exception). Renvoie (wait_ns cumulées — EXCLUES de own_latency —, late par slot)."""
    global _sl_dbg
    dbg = [0, -1, 0, 0, 0]   # [entrées, valid0, attentes, replis, dormantes]
    # ─ LATCH IMMÉDIAT (avant toute attente/bind) : mode, masque wipe (précalculé UNE fois par
    #   (alpha, préréglage) puis TRANCHÉ par bande) et géométrie keyer sont figés ICI.
    k_in = int(keyer_state["input"])
    keyer_on = keyer_alpha > 0.001
    kg = _sl_keyer_geom(keyer_state) if keyer_on else None
    bgd_mode = (bgd_preset.get("mode", "dissolve") if bgd_preset else "dissolve")
    wm = None
    if bgd_active and bgd_mode == "wipe":
        w = (bgd_preset.get("wipe") or dict())
        af = _wipe_alpha_map(bgd_alpha, w.get("direction", "horizontal"),
                             bool(w.get("reverse", False)), float(w.get("softness", 0.10)))
        wm = (af, af[::_CH, ::_CW])
    if ovl is not None and ovl["y"].shape != (HEIGHT, WIDTH):
        ovl = None   # même garde de forme que le chemin historique (overlay PRÉ-CONVERTI)
    # ─ Ouverture des entrées actives de la trame, TOUTES au même index (ou leur tête si
    #   hors-grille). Un slot partagé par plusieurs couches (ex. keyer = PGM) n'est lu qu'une fois.
    slots = ([bgd_from_idx, bgd_to_idx] if bgd_active else [pgm])
    if keyer_on:
        slots.append(k_in)
    slots.append(pvw)
    srcs = dict()
    for s in slots:
        if s not in srcs:
            srcs[s] = _sl_bind(s, idx_ref, ts_in_per_slot, dbg)
    dbg[0] = sum(1 for x in srcs.values() if x is not None)
    # ─ Couche BGD latchée : blend si les DEUX sources présentes, sinon copie (to ou from),
    #   sinon noir — même repli que l'historique (f_to or f_from or empty), décidé UNE fois.
    if bgd_active:
        A = srcs.get(bgd_from_idx); B = srcs.get(bgd_to_idx)
        blend_on = A is not None and B is not None
        S = B if B is not None else A
    else:
        A = B = None; blend_on = False
        S = srcs.get(pgm)
    K = srcs.get(k_in) if keyer_on else None
    V = srcs.get(pvw)
    # ─ 3 grains de sortie à NOTRE index (idx_out, grille TAI) + vues par plan. Distinct de
    #   idx_ref, qui n'a servi qu'à LIRE les entrées à leur coordonnée : on ne veut pas que la
    #   coordonnée de nos grains dépende d'une entrée (cf. _ouvrir_sortie). Entrées 2110, donc
    #   déjà sur la grille : les deux valeurs coïncident au grain près.
    _i, gi_p, vw_p = out_pgm.open_grain(index=idx_out)
    _i, gi_c, vw_c = out_clean.open_grain(index=idx_out)
    _i, gi_v, vw_v = out_pvw.open_grain(index=idx_out)
    # Les trois sorties partagent idx_out : une seule observation suffit, sur le PROGRAMME.
    # `_sl_bind` renvoie r (reader) et fi (index réellement lu) par entrée — y compris sur son
    # chemin dégradé hors-grille, où fi est la tête de l'entrée et non idx_ref.
    delai_etage.observe(out_pgm, idx_out,
                        [(q["r"], q["fi"]) for q in srcs.values()
                         if q and q.get("r") is not None and q.get("fi") is not None])
    def _planes(vw):
        return (vw[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                vw[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                vw[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))
    pg_y, pg_u, pg_v = _planes(vw_p)
    cl_y, cl_u, cl_v = _planes(vw_c)
    pv_y, pv_u, pv_v = _planes(vw_v)
    # GARDE-FOU n°1 : la sortie ne peut JAMAIS être en retard — budget de trame DUR.
    deadl = time.monotonic() + 1.0 / FPS
    wait_ns = 0
    done = 0
    try:
        for k in range(_SL_NB):
            a = k * SLICE_LINES; b = a + SLICE_LINES
            ca = a // _CH; cb = b // _CH
            # ─ CLEAN = couche BGD (le clean n'a NI keyer NI overlay, par définition) ─
            if blend_on:
                wait_ns += _sl_wait(A, b - 1, deadl, dbg)
                wait_ns += _sl_wait(B, b - 1, deadl, dbg)
                _sl_blend_band(cl_y, cl_u, cl_v, A, B, bgd_alpha, bgd_mode, wm, a, b)
            elif S is not None:
                wait_ns += _sl_wait(S, b - 1, deadl, dbg)
                cl_y[a:b] = S["y"][a:b]; cl_u[ca:cb] = S["u"][ca:cb]; cl_v[ca:cb] = S["v"][ca:cb]
            else:
                cl_y[a:b] = _BLACK_Yi; cl_u[ca:cb] = _NEUT_Ci; cl_v[ca:cb] = _NEUT_Ci
            # ─ PGM = CLEAN + keyer + overlay HTML ─
            pg_y[a:b] = cl_y[a:b]; pg_u[ca:cb] = cl_u[ca:cb]; pg_v[ca:cb] = cl_v[ca:cb]
            if K is not None and kg is not None:
                nr = _sl_keyer_need(kg, a, b)
                if nr >= 0:
                    wait_ns += _sl_wait(K, nr, deadl, dbg)
                _sl_keyer_band(pg_y, pg_u, pg_v, K, kg, keyer_alpha, a, b)
            if ovl is not None:
                _sl_overlay_band(pg_y, pg_u, pg_v, ovl, a, b)
            # ─ PVW ─
            if V is not None:
                wait_ns += _sl_wait(V, b - 1, deadl, dbg)
                pv_y[a:b] = V["y"][a:b]; pv_u[ca:cb] = V["u"][ca:cb]; pv_v[ca:cb] = V["v"][ca:cb]
            else:
                pv_y[a:b] = _BLACK_Yi; pv_u[ca:cb] = _NEUT_Ci; pv_v[ca:cb] = _NEUT_Ci
            done = b
            if k < _SL_NB - 1:   # commit PROGRESSIF (réveille l'aval) ; bande finale → finally
                out_pgm.commit(gi_p,   valid_slices=k + 1)
                out_clean.commit(gi_c, valid_slices=k + 1)
                out_pvw.commit(gi_v,   valid_slices=k + 1)
    finally:
        # Complément : bandes jamais posées (exception à mi-trame) → noir, pour ne JAMAIS
        # publier de lignes indéterminées ; puis commit FINAL TOUJOURS (un grain laissé
        # partiel ne serait jamais lisible par un consommateur whole-frame).
        if done < HEIGHT:
            cd = done // _CH
            for _py, _pu2, _pv2 in ((pg_y, pg_u, pg_v), (cl_y, cl_u, cl_v), (pv_y, pv_u, pv_v)):
                _py[done:] = _BLACK_Yi; _pu2[cd:] = _NEUT_Ci; _pv2[cd:] = _NEUT_Ci
        for _w, _gi in ((out_pgm, gi_p), (out_clean, gi_c), (out_pvw, gi_v)):
            try: _w.commit(_gi, valid_slices=None)
            except Exception: pass
    late = dict()
    for s, st in srcs.items():
        if st is not None:
            late[s] = bool(st["late"])
    _sl_dbg = dbg
    return wait_ns, late


# ─── Boucle principale ──────────────────────────────────────
def _ouvrir_sortie(name):
    """Writer MXL de sortie (planar). Index TOUJOURS explicite à l'écriture (out_index, commun aux
    3 sorties → PGM/CLEAN/PVW synchronisés), calculé par bobimxl.Writer.next_index — la grille TAI,
    qui ne dépend d'aucune entrée. MODE TRANCHE : le flowDef
    porte slice_height → libmxl publie le grain en N tranches égales (commit progressif) ; sans
    slice : inchangé."""
    return bobimxl.Writer(inst, name, WIDTH, HEIGHT, CHROMA, BIT_DEPTH, FPS, 1,
                          **(dict(slice_height=SLICE_LINES) if SLICE_ON else dict()))

out_pgm   = _ouvrir_sortie(SHM_OUT_PGM)
out_clean = _ouvrir_sortie(SHM_OUT_CLEAN)
out_pvw   = _ouvrir_sortie(SHM_OUT_PVW)
if SLICE_ON:
    log(f"mixer: MODE TRANCHE actif — {{SLICE_LINES}} lignes/bande ({{_SL_NB}} tranches/trame), "
        "verrou-référence INDEXÉ sur l'entrée sync_ref", "info")

def _ecrire_sortie(w, yuv, frame_index):
    try:
        _i, gi, vw = w.open_grain(index=frame_index)
        vw[:len(yuv)] = np.frombuffer(yuv, dtype=np.uint8)
        w.commit(gi)
    except Exception:
        pass

frame_index = 0
start = time.time()
_fps_last_idx = 0          # fps en fenêtre glissante (delta depuis le dernier report)
_fps_last_t   = start
next_t = start
interval = 1.0 / FPS
half_ns = (interval / 2.0) * 1e9          # demi-période en ns (tolérance d'alignement)
last_ref_idx = 0                          # genlock : dernier index de l'entrée de référence émis
ref_stall_t = 0.0                         # instant où la réf a cessé d'avancer (0 = elle avance)
last_cycle_t = start                      # début de la trame précédente (base de l'échéance réf)
last_black = start                        # genlock : cadence du noir quand pas de réf
empty_frame = _BLACK_B * (WIDTH * HEIGHT) + _NEUT_B * (2 * UV_W * UV_H)   # noir yuv

while True:
    if bus_error.is_set():
        bus_error.clear()
        for i in range(N_INPUTS):
            h = input_handles[i]
            if h is not None:
                try: h[0].close()
                except Exception: pass
                input_handles[i] = None
        for w in (out_pgm, out_clean, out_pvw):
            try: w.close()
            except Exception: pass
        # GC entre close et reopen (même parade que ensure_input : sans GC, la réouverture
        # peut retomber sur un flux orphelin résolvable par nom).
        try: inst.garbage_collect()
        except Exception: pass
        time.sleep(2)
        out_pgm   = _ouvrir_sortie(SHM_OUT_PGM)
        out_clean = _ouvrir_sortie(SHM_OUT_CLEAN)
        out_pvw   = _ouvrir_sortie(SHM_OUT_PVW)
        continue

    # Snapshot état + finalisation de transition si terminée
    with state_lock:
        pgm = state["pgm"]; pvw = state["pvw"]
        trans = state["transition"]
        manual = state["manual_mix"]
        if trans is not None:
            elapsed_ms = (time.time() - trans["start"]) * 1000.0
            if elapsed_ms >= trans["duration_ms"]:
                if trans.get("bgd"):
                    state["pgm"] = trans["bgd"]["to_idx"]
                    state["pvw"] = trans["bgd"]["flip_to_pvw"]
                if trans.get("keyer"):
                    state["keyer"]["active"] = trans["keyer"]["to_active"]
                state["transition"] = None
                pgm = state["pgm"]; pvw = state["pvw"]
                trans = None
        ovl_active   = state["overlay_enabled"] and bool(state["overlay_url"])
        keyer_state  = dict(state["keyer"])

    # Capture ts_in par slot lu ce cycle (pour latence par câble)
    ts_in_per_slot = {{}}

    # ─ Couche BGD (transition pgm↔pvw si applicable) ─────────
    # (déterminée ci-dessous ; bgd_to_idx/pgm servent à choisir l'entrée de RÉFÉRENCE du genlock)
    bgd_alpha = 0.0; bgd_active = False
    bgd_from_idx = pgm; bgd_to_idx = pgm
    bgd_preset = None
    if manual is not None and manual.get("bgd"):
        bgd_active = True
        bgd_alpha  = max(0.0, min(1.0, manual["alpha"]))
        bgd_from_idx = manual["bgd"]["from_idx"]
        bgd_to_idx   = manual["bgd"]["to_idx"]
        bgd_preset   = manual.get("preset")
    elif trans is not None and trans.get("bgd"):
        bgd_active = True
        bgd_alpha  = max(0.0, min(1.0,
            (time.time() - trans["start"]) * 1000.0 / trans["duration_ms"]))
        bgd_from_idx = trans["bgd"]["from_idx"]
        bgd_to_idx   = trans["bgd"]["to_idx"]
        bgd_preset   = trans.get("preset")

    # ─ Cadence : genlock verrou sur l'entrée de RÉFÉRENCE (choix opérateur, FIXE), ou free-run ─
    # On émet 1 sortie par nouvelle trame de la réf (verrou 1:1, latence mini, pas de requantif). La
    # réf est `state["sync_ref"]` — INDÉPENDANTE de PGM/PVW → ne change pas à la commutation. Les
    # autres entrées sont alignées sur T = write_ts de la réf, dans une fenêtre d'ALIGN_BUDGET trames ;
    # au-delà → présentées en retard + flag. Réf absente/morte → repli free-run (programme vivant)+flag.
    T = None; ref_lost = False
    with state_lock:
        ref_idx = max(0, min(N_INPUTS - 1, int(state["sync_ref"])))
    if GENLOCK:
        ri, rw = _in_header(ref_idx)
        if ri == 0:
            ref_lost = True                    # réf perdue → repli free-run (ne fige pas le programme)
            now = time.time()
            wait = next_t - now
            if wait > 0: time.sleep(wait)
        elif ri == last_ref_idx:
            # Réf PRÉSENTE mais pas encore avancée.
            #
            # ★ RÈGLE ABSOLUE : LE MÉLANGEUR N'ATTEND JAMAIS AU-DELÀ DE L'ÉCHÉANCE DE LA TRAME.
            # On patiente la réf tant qu'il reste du temps avant `next_t`, et pas une milliseconde
            # de plus : à l'échéance on compose quand même, avec le contenu disponible. La sortie
            # garde donc TOUJOURS sa cadence — au pire elle répète une trame, ce qui est le
            # comportement attendu d'un mélangeur dont une source décroche. Elle ne gèle pas.
            #
            # Le défaut d'origine était de faire `continue` sans borne : une source qui existe et
            # n'avance plus arrêtait le programme pour de bon (vu en production : référence issue
            # d'un flux répliqué par RDMA dont l'initiateur tournait à vide — flux présent, jamais
            # incrémenté ; trois heures à 0 fps, sans une alerte).
            # L'échéance est RELATIVE à la trame précédente, jamais la grille absolue `next_t` :
            # celle-ci est calée sur la cadence NOMINALE, or une source à 49,8 fps pour un
            # mélangeur réglé à 50 la laisse en permanence dans le passé. Borner par elle
            # reviendrait à ne plus jamais attendre la référence — genlock désactivé en silence.
            now = time.time()
            echeance = last_cycle_t + interval * REF_LATE_TOL
            if now < echeance:
                time.sleep(min(0.002, echeance - now)); continue
            # Échéance atteinte : on émet. Une réf simplement EN RETARD n'est pas une réf morte —
            # on ne lève `sync_ref_lost` qu'après REF_STALL_FRAMES trames sans progression, pour
            # ne pas faire clignoter l'alarme sur le jitter d'arrivée normal.
            if ref_stall_t == 0.0:
                ref_stall_t = now
            if (now - ref_stall_t) >= REF_STALL_FRAMES * interval:
                ref_lost = True
        else:
            last_ref_idx = ri
            ref_stall_t = 0.0                  # la réf a bougé : le délai de garde repart de zéro
            T = rw
            # MODE TRANCHE : l'alignement par media-timestamp est REMPLACÉ par le verrou-
            # référence INDEXÉ (toutes les entrées suivies au même index, par bandes) → pas
            # d'attente d'alignement ici. Whole-frame : fenêtre d'ALIGN_BUDGET inchangée.
            if ALIGN_BUDGET > 0 and not SLICE_ON:
                cand = set([bgd_from_idx, bgd_to_idx, pgm, pvw, keyer_state["input"]])
                cand.discard(ref_idx)
                budget_deadline = time.time() + ALIGN_BUDGET * interval
                while time.time() < budget_deadline:
                    behind = False
                    for c in cand:
                        _, cw = _in_header(c)
                        if cw and cw < T - half_ns:   # cette entrée n'a pas atteint l'instant T
                            behind = True; break
                    if not behind: break
                    time.sleep(0.002)
    else:
        now = time.time()
        wait = next_t - now
        if wait > 0: time.sleep(wait)

    last_cycle_t = time.time()        # base de l'échéance d'attente de la réf, trame suivante
    ts_cycle_start = time.time_ns()   # début du compositing (réf détectée + alignée) → base OWN
    # ★ BASE TAI POUR LE TRANSIT, DISTINCTE DE CELLE DU TRAVAIL PROPRE (2026-10-23).
    # `Reader.last_write_time()` rend un instant **TAI** (epoch ST 2059) ; sa docstring donne même
    # la formule juste : `(now_tai() - last_write_time())/1e6`. Le transit la calculait pourtant
    # depuis `time.time_ns()`, qui est de l'epoch **UNIX** : on soustrayait deux horloges
    # d'époques différentes, donc l'écart TAI−UTC — exactement **−37 000 ms** relevés en recette
    # sur le player PUIS sur les entrées DeckLink dès qu'elles ont été lues. Une latence négative
    # de 37 secondes était publiée telle quelle.
    # `own` reste sur l'horloge Unix : ses deux bornes (`ts_cycle_start`, `ts_out`) en viennent,
    # la soustraction y est homogène. Ne pas fusionner les deux bases.
    ts_cycle_tai = bobimxl.now_tai()   # base du TRANSIT (même époque que last_write_time)

    # ─ INDEX DE SORTIE ───────────────────────────────────────────────────────────────────────
    # Le calcul vit UNE SEULE FOIS pour toute la flotte, dans bobimxl.Writer.next_index (grille
    # TAI, strictement croissante, holdover) — ne pas le refaire ici. Ce qui se décide ICI, en
    # revanche, c'est que la coordonnée du mélangeur ne dépend d'AUCUNE entrée : on commence une
    # émission sur la caméra 1, elle part faire autre chose, on continue sans elle et l'index ne
    # s'en aperçoit pas. `sync_ref` ne sert qu'à la CADENCE et à l'ALIGNEMENT des entrées.
    out_index = out_pgm.next_index()

    _sl_waited = 0   # mode tranche : cumul des attentes get_slice de la trame (EXCLUES de own_latency)
    if SLICE_ON and T is not None:
        # ─ MODE TRANCHE : trame bandée à l'INDEX de la référence (verrou-référence INDEXÉ).
        # LATCH d'état par grain : bgd_alpha/bgd_preset/sélections (calculés ci-dessus, avant la
        # cadence), keyer et overlay sont FIGÉS ICI et servent pour TOUTES les bandes de la trame
        # — jamais un changement d'état à mi-trame (une trame moitié-cut serait fausse).
        keyer_alpha_now = _calc_keyer_alpha(keyer_state, manual, trans)
        ovl = None
        if ovl_active:
            with overlay_lock:
                ovl = overlay_prep
        _sl_late = dict()
        try:
            _sl_waited, _sl_late = _slice_frame(int(last_ref_idx), int(out_index),
                                                bgd_active, bgd_alpha,
                                                bgd_from_idx, bgd_to_idx, bgd_preset,
                                                keyer_alpha_now, keyer_state, ovl,
                                                pgm, pvw, ts_in_per_slot)
        except Exception as _e:
            # Garde-fou : une trame tranche qui plante ne tue PAS la boucle (la sortie garde
            # sa dernière image via le ring, cadence inchangée).
            log(f"mixer: erreur trame tranche ignorée : {{_e}}", "warning")
        # GARDE-FOU n°2 : fenêtre glissante (~10 s) du taux de replis PAR ENTRÉE câblée ;
        # au-delà de SLICE_LATE_PCT → signalée (métriques) ; réarmée sous _SL_CLEAR_PCT.
        with state_lock:
            _stn = list(state["inputs"])
        for _slot, _l in _sl_late.items():
            _nm = _stn[_slot] if _slot < len(_stn) else ""
            if not _nm:
                continue
            _wnd = _sl_late_win.get(_nm)
            if _wnd is None:
                _wnd = _sl_late_win[_nm] = deque(maxlen=max(1, 10 * FPS))
            _wnd.append(1 if _l else 0)
            _pct = 100.0 * sum(_wnd) / len(_wnd)
            if _nm in _sl_flagged:
                if _pct < _SL_CLEAR_PCT:
                    _sl_flagged.pop(_nm, None)          # réarmé (hystérésis)
                else:
                    _sl_flagged[_nm] = round(_pct, 1)
            elif _pct >= SLICE_LATE_PCT and len(_wnd) >= FPS:
                _sl_flagged[_nm] = round(_pct, 1)
    else:
        # ─ Chemin HISTORIQUE whole-frame (slice_mode off, inéligible, ou réf perdue) —
        # STRICTEMENT inchangé.
        if bgd_active:
            f_from, ts_from = lire_frame_yuv(ensure_input(bgd_from_idx))
            f_to,   ts_to   = lire_frame_yuv(ensure_input(bgd_to_idx))
            if ts_from: ts_in_per_slot[bgd_from_idx] = ts_from
            if ts_to:   ts_in_per_slot[bgd_to_idx]   = ts_to
            if f_from and f_to:
                yuv_bgd = blend_yuv_preset(f_from, f_to, bgd_alpha, bgd_preset)
            else:
                yuv_bgd = f_to or f_from or empty_frame
        else:
            yuv_pgm_src, ts_pgm = lire_frame_yuv(ensure_input(pgm))
            if ts_pgm: ts_in_per_slot[pgm] = ts_pgm
            yuv_bgd = yuv_pgm_src or empty_frame

        # ─ Couche Keyer (alpha actuel : statique ou en transition) ─
        keyer_alpha_now = _calc_keyer_alpha(keyer_state, manual, trans)

        # Le CLEAN ne contient PAS le keyer (par définition).
        yuv_clean = yuv_bgd

        # Le PGM contient BGD + keyer + overlay HTML.
        yuv_pgm = yuv_bgd
        if keyer_alpha_now > 0.001:
            k_in = keyer_state["input"]
            k_frame, ts_k = lire_frame_yuv(ensure_input(k_in))
            if ts_k: ts_in_per_slot[k_in] = ts_k
            if k_frame:
                yuv_pgm = composite_keyer_yuv(
                    yuv_pgm, k_frame, keyer_alpha_now,
                    keyer_state["x"], keyer_state["y"], keyer_state["scale"],
                    keyer_state["border_width"],
                )
        if ovl_active:
            with overlay_lock:
                ovl = overlay_prep
            if ovl is not None:
                yuv_pgm = composite_overlay(yuv_pgm, ovl)

        # ─ Bus PVW (entrée en preview, sans overlay) ─
        yuv_pvw_src, ts_pvw = lire_frame_yuv(ensure_input(pvw))
        if ts_pvw: ts_in_per_slot[pvw] = ts_pvw
        yuv_pvw = yuv_pvw_src or empty_frame

        # ─ Écriture des 3 shm ─
        # ⚠ CHEMIN WHOLE-FRAME (slice off, inéligible, ou RÉFÉRENCE PERDUE) : pas de délai
        # d'étage mesurable — `lire_frame_yuv` ne rend que (octets, ts), sans index de grain.
        # On observe quand même AVEC UNE LISTE VIDE : ça n'invente aucune mesure, mais ça COMPTE
        # le refus, donc `:8080` publie `non_mesurable` au lieu de rester muet. Sans ça, un mixer
        # qui produit sans mesurer était indiscernable d'un mixer à l'arrêt — constaté sur
        # `mixer-test` le 2026-08-19, qui tournait sur CE chemin (sync_ref_lost) pendant que je
        # croyais avoir instrumenté la seule sortie du plugin.
        delai_etage.observe(out_pgm, out_index, [])
        _ecrire_sortie(out_pgm,   yuv_pgm,   out_index)
        _ecrire_sortie(out_clean, yuv_clean, out_index)
        _ecrire_sortie(out_pvw,   yuv_pvw,   out_index)
    ts_out = time.time_ns()

    # Latence par slot consommé : Δ ts_out - ts_in, indexé par shm_name câblé
    # + skew d'alignement (ts_in - T) et flag « en retard » (genlock : hors fenêtre d'alignement).
    with state_lock:
        slot_to_shm = list(state["inputs"])
    skew_in = {{}}; late_in = []
    for slot_idx, ts_in in ts_in_per_slot.items():
        shm_name = slot_to_shm[slot_idx] if slot_idx < len(slot_to_shm) else ""
        if not shm_name:
            continue
        if shm_name not in lat_in:
            lat_in[shm_name] = RollingMs()
        # TRANSIT (arrivée) = âge du grain à la lecture. Base TAI obligatoire : `ts_in` vient de
        # `last_write_time()`, qui est en TAI. Avec `ts_cycle_start` (Unix) on publiait −37 s.
        lat_in[shm_name].push((ts_cycle_tai - ts_in) / 1e6)
        if T is not None:
            skew_in[shm_name] = round((ts_in - T) / 1e6, 2)
            if ts_in < T - half_ns:
                late_in.append(shm_name)
    # own = TRAVAIL de compositing : en mode tranche, les attentes get_slice (suivi du fil ≈
    # période de trame) sont EXCLUES — le tissu/monitoring y lit la SATURATION du nœud, pas le
    # suivi (même contrat que multiview/color_corrector). Whole-frame : _sl_waited = 0.
    own_lat.push((ts_out - ts_cycle_start - _sl_waited) / 1e6)  # traitement PROPRE (compositing/mix)
    if T is not None:
        with metrics_lock:
            metrics["inputs_skew_ms"] = skew_in
            metrics["inputs_late"] = late_in

    frame_index += 1
    next_t = start + frame_index * interval

    if frame_index % FPS == 0:
        _now = time.time(); _dt = _now - _fps_last_t
        with metrics_lock:
            if _dt > 0 and frame_index >= _fps_last_idx:
                metrics["fps"] = round((frame_index - _fps_last_idx) / _dt, 1)
            _fps_last_idx = frame_index; _fps_last_t = _now
            metrics["frame_index"] = frame_index
            metrics["pgm"] = pgm
            metrics["pvw"] = pvw
            metrics["sync_ref"] = ref_idx
            metrics["sync_ref_lost"] = ref_lost
            metrics["inputs_latency_ms"] = {{k: v.avg() for k, v in lat_in.items()}}
            metrics["own_latency_ms"] = own_lat.avg()
            metrics["delai_etage_trames"] = delai_etage.publish()
            if SLICE_ON:
                # Observabilité mode tranche : compteurs de la dernière trame + garde-fou n°2.
                _sd = _sl_dbg
                if _sd:
                    metrics["slice"] = {{"inputs": _sd[0], "valid0": _sd[1], "waits": _sd[2],
                                        "fallbacks": _sd[3], "dormant": _sd[4],
                                        "backoff": len(_sl_backoff)}}
                metrics["slice_late_inputs"] = dict(_sl_flagged)
                # AVIS STRUCTURÉ (clé + paramètres) plutôt qu'une phrase française : c'est le
                # SEUL chemin par lequel du texte écrit ici atteint le journal d'alertes de
                # l'orchestrateur, donc le seul que le keyage ne peut pas rattraper de l'autre
                # côté. La liste des entrées est une DONNÉE, pas de la prose — c'est ce qui rend
                # l'avis traduisible, là où l'ancien `" ; ".join` d'une phrase par entrée ne
                # l'était pas. L'orchestrateur accepte encore la chaîne nue (repli pour un
                # script pas encore redéployé), cf. app/metrics.py.
                metrics["advisory"] = ({{"key": "plugin.mixer.advisory.slice_late",
                                        "params": {{"entrees": ", ".join(sorted(_sl_flagged))}}}}
                                       if _sl_flagged else None)
