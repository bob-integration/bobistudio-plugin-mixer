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
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler

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

# Colorimétrie de la COULEUR DE CLÉ d'un keyer chroma (0.23.22) : la clé #RRGGBB est convertie avec la
# matrice déclarée (`colorimetry` : 601 / 709 / 2020), comme le split. Absente ou inconnue → 709, la
# colorimétrie HD — et c'est DIT : `caps.key_colorimetry_assumed` dans /state, journal au premier
# keyer chroma. (Les autres conversions RVB du mélangeur — habillage — restent en BT.601 : dette connue.)
_KEY_KRKB = {{"601": (0.299, 0.114), "709": (0.2126, 0.0722), "2020": (0.2627, 0.0593)}}
_KEY_COL_RAW = str(CONFIG.get("colorimetry") or "").strip().lower().replace("bt", "").replace(".", "")
KEY_COLORIMETRY_ASSUMED = _KEY_COL_RAW not in _KEY_KRKB
KEY_COLORIMETRY = "709" if KEY_COLORIMETRY_ASSUMED else _KEY_COL_RAW
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

# ─── DÉLAIS (0.23.9) ────────────────────────────────────────
# MODE TRANCHE : chaque entrée est lue à l'image « créneau courant − délai », le créneau venant de
# l'HORLOGE (grille TAI), jamais d'une entrée. Délai par défaut du mélangeur : "fixed_1" = 1 image
# (défaut, toutes les sources arrivent à temps), "fastest" = 0 (créneau courant, attente bornée de
# la 1ʳᵉ tranche). GROUPES DE DÉLAI : des entrées calées ENSEMBLE à un délai propre (0 à 6 images) ;
# une entrée sans groupe suit le délai par défaut. Le PGM/CLEAN/PVW porte le numéro du GROUPE DE
# RÉFÉRENCE (délai par défaut s'il n'y en a pas) : il ne change PAS à la commutation, et une source
# d'un autre groupe y figure avec sa latence propre.
LATENCY_MODES = ("fixed_1", "fastest")
# 6 = 5 images de DÉCALAGE AMONT + 1 image du mode fixe. L'historique MXL (200 ms = 10 grains à
# 50 fps) le couvre.
GROUP_DELAY_MAX = 6
GROUP_NAME_MAX = 32
# DÉCALAGE AMONT (0.23.22, décision 2026-10-25) : une source dont le contenu arrive déjà vieux de u
# images (HF, liaison codée, traitement extérieur) reste ESTAMPILLÉE À L'ARRIVÉE — image et son, sinon
# un enregistrement ISO de la source se décalerait — et c'est ici qu'on la recale, visiblement : elle
# est lue u images plus RÉCENTE que son groupe, ce qui aligne son contenu sur celui des autres. Son
# groupe doit donc offrir au moins « délai nécessaire mesuré + u » ; un groupe trop court ne peut pas
# compenser (on ne lit pas dans le futur) : l'entrée est lue au plus récent possible et signalée.
UPSTREAM_MAX = 5


def _latency_mode(v):
    v = str(v or "").strip().lower()
    return v if v in LATENCY_MODES else "fixed_1"


def _norm_delays(d):
    """Document des groupes, borné : {{"groups": {{nom: 0..6}}, "inputs": {{"1": nom, …}}, "ref_group": nom|None,
    "upstream": {{"1": 1..5, …}}}}. Entrées numérotées à partir de 1 (comme partout dans l'interface) ;
    une entrée pointant vers un groupe inconnu redevient sans groupe, plutôt que de rester rattachée à
    un délai fantôme. Un décalage amont nul n'est pas stocké ; hors bornes, il est ignoré."""
    d = d if isinstance(d, dict) else dict()
    groups = dict()
    for name, frames in (d.get("groups") or dict()).items():
        name = str(name or "").strip()[:GROUP_NAME_MAX]
        try:
            frames = int(frames)
        except Exception:
            continue
        if name and 0 <= frames <= GROUP_DELAY_MAX:
            groups[name] = frames
    inputs = dict()
    for k, g in (d.get("inputs") or dict()).items():
        try:
            n = int(k)
        except Exception:
            continue
        if 1 <= n <= N_INPUTS and g in groups:
            inputs[str(n)] = g
    ref = d.get("ref_group")
    upstream = dict()
    for k, u in (d.get("upstream") or dict()).items():
        try:
            n, u = int(k), int(u)
        except Exception:
            continue
        if 1 <= n <= N_INPUTS and 1 <= u <= UPSTREAM_MAX:
            upstream[str(n)] = u
    return {{"groups": groups, "inputs": inputs, "ref_group": ref if ref in groups else None,
            "upstream": upstream}}


def _default_delay(mode):
    return 0 if mode == "fastest" else 1


def _input_delay(slot, mode, delays):
    """Délai (images) de l'entrée `slot` (0-based) : celui de son groupe, sinon le défaut."""
    g = delays["inputs"].get(str(slot + 1))
    return delays["groups"][g] if g else _default_delay(mode)


def _upstream_of(slot, delays):
    """Décalage amont (images) de l'entrée `slot` (0-based)."""
    return int((delays.get("upstream") or dict()).get(str(slot + 1), 0))


def _read_delay(slot, mode, delays):
    """Délai auquel l'entrée est LUE : celui de son groupe moins son décalage amont, jamais négatif."""
    return max(0, _input_delay(slot, mode, delays) - _upstream_of(slot, delays))


def _ref_delay(mode, delays):
    g = delays.get("ref_group")
    return delays["groups"][g] if g else _default_delay(mode)


# ─── KEYERS AMONT (0.23.14) ─────────────────────────────────
# Keyers DANS le M/E, comme les USK d'un ATEM : composés sur le fond, au-dessus de la transition
# BGD (ils font partie de ce qui transitionne), sous l'habillage HTML (DSK). Le keyer 1 est en
# dessous, le N au-dessus. Nombre fixé au déploiement (`n_keyers`, 1 à 4, défaut 1).
# Le keyer 1 EST l'ancien « keyer » : /keyer/* sans numéro le vise, /state.keyer le publie, la
# cible de transition « keyer » le désigne — rien ne change pour un appel d'avant 0.23.14.
# Types :
#   dve    — l'incrustation historique : image réduite/placée + bordure, alpha uniforme, SANS découpe ;
#   linear — deux entrées, fill + key : la luminance du key donne l'alpha, PLEINE ÉCHELLE
#            (0 = transparent, max = opaque — contrat du flux `_key` du plugin stills) ;
#   luma   — l'alpha est tiré de la luminance du fill, échelle STUDIO (16 → 0, 235 → 1 en 8 bits).
#   chroma — (0.23.22) l'alpha est tiré de la CHROMA du fill : distance, dans le plan (U, V), à la
#            couleur de clé (`chroma_color`, défaut vert #00b140 comme la couche ART du split) ;
#            transparent sous la tolérance (`chroma_tol`), rampe sur `chroma_soft`, opaque au-delà.
#            Suppression de débordement (`chroma_spill`) : la composante du fill DANS LA DIRECTION de
#            la clé est retirée (la frange verte d'une chevelure redevient neutre).
#   pattern — (0.23.22) motif : l'alpha est une FORME générée (barres horizontale/verticale/diagonale,
#            iris, cœur — les motifs du volet — et boîte), centrée en (`pattern_x`, `pattern_y`) de
#            l'écran, de taille `pattern_size`, adoucie de `pattern_soft` ; fill = une entrée.
# Découpe (linear, luma) : inversion, seuil (clip) puis pente (gain), normalisés :
#   alpha = clamp((k − clip) / (1 − clip) × gain, 0, 1).
# `invert` vaut pour les quatre types à découpe (chroma : garder la couleur de clé ; motif : le dehors).
# Fly key pour TOUS les types : masque (fractions retirées sur chaque bord) + taille + X + Y.
# Les transitions, l'ON AIR, l'AUTO et le CLEAN sont communs à tous les types.
KEYER_TYPES = ("dve", "linear", "luma", "chroma", "pattern")
MAX_KEYERS = 4
_NK_RAW = CONFIG.get("n_keyers")
try:
    _NK_ASKED = int(_NK_RAW) if _NK_RAW not in (None, "") else 1
except (TypeError, ValueError):
    _NK_ASKED = 1
N_KEYERS = max(1, min(MAX_KEYERS, _NK_ASKED))
if _NK_ASKED != N_KEYERS:
    _nk_warn = ("n_keyers=%s hors bornes (1..%d) — %d keyer(s) appliqué(s)"
                % (_NK_RAW, MAX_KEYERS, N_KEYERS))
    CONFIG_WARNING = (CONFIG_WARNING + " ; " + _nk_warn) if CONFIG_WARNING else _nk_warn
    log("CONFIG: n_keyers=%s out of range (1..%d), %d keyer(s) applied"
        % (_NK_RAW, MAX_KEYERS, N_KEYERS), "warning")
KEY_GAIN_MIN = 0.1
KEY_GAIN_MAX = 20.0
KEYER_AUTO_MS_MAX = 10000
_MASK_EDGES = ("left", "right", "top", "bottom")


def _to_int(v):
    return int(round(float(v)))


# Champs numériques réglables d'un keyer : (min, max, conversion). Une valeur hors bornes est
# ramenée dans les bornes (comme /keyer/transform l'a toujours fait) ; une valeur ILLISIBLE est
# refusée et nommée dans la réponse.
_KEYER_NUM = dict(x=(0.0, 1.0, float), y=(0.0, 1.0, float), scale=(0.05, 1.0, float),
                  border_width=(0, 50, _to_int), clip=(0.0, 1.0, float),
                  gain=(KEY_GAIN_MIN, KEY_GAIN_MAX, float), auto_ms=(0, KEYER_AUTO_MS_MAX, _to_int),
                  auto_soft=(0.0, 1.0, float),
                  chroma_tol=(0.0, 1.0, float), chroma_soft=(0.0, 1.0, float), chroma_spill=(0.0, 1.0, float),
                  pattern_size=(0.0, 1.0, float), pattern_x=(0.0, 1.0, float), pattern_y=(0.0, 1.0, float),
                  pattern_soft=(0.0, 1.0, float))
# Type de l'AUTO PROPRE d'un keyer (0.23.19) : fondu (mix) ou volet (wipe, mêmes motifs que le fond).
# Ne vaut que pour l'AUTO du keyer : emporté par la transition M/E, le keyer suit celle-ci.
KEYER_AUTO_MODES = ("mix", "wipe")
WIPE_DIRECTIONS = ("horizontal", "vertical", "diagonal", "iris", "heart")
# Formes du keyer MOTIF (0.23.22) : les motifs du volet, plus la boîte (rectangle au format de l'image).
PATTERN_SHAPES = WIPE_DIRECTIONS + ("box",)
CHROMA_KEY_DEFAULT = "#00b140"
# Ce qui survit à un redéploiement (document persisté par l'orchestrateur, cf. GET /delays).
# L'état d'antenne (active) n'en fait PAS partie : un redéploiement ne remet rien à l'antenne.
_KEYER_PERSIST = ("type", "input", "key_input", "x", "y", "scale", "border_width",
                  "clip", "gain", "invert", "auto_ms", "clean",
                  "auto_mode", "auto_dir", "auto_rev", "auto_soft", "auto_alt",
                  "chroma_color", "chroma_tol", "chroma_soft", "chroma_spill",
                  "pattern", "pattern_size", "pattern_x", "pattern_y", "pattern_soft")


def _keyer_default():
    return dict(type="dve", input=0, key_input=(1 if N_INPUTS > 1 else 0), active=False,
                x=0.5, y=0.5, scale=1.0, border_width=0,
                mask=dict(left=0.0, right=0.0, top=0.0, bottom=0.0),
                clip=0.0, gain=1.0, invert=False, auto_ms=INITIAL_DISSOLVE_MS,
                auto_mode="mix", auto_dir="horizontal", auto_rev=False, auto_soft=0.10, auto_alt=False,
                # NIVEAU DE CLEAN (0.23.15) : ce keyer figure-t-il sur la sortie CLEAN ? Réglé PAR
                # KEYER, à la demande de l'utilisateur (« choisir qui on veut ou pas dessus »).
                # Défaut : non — la sortie CLEAN des mélangeurs existants reste le fond seul.
                clean=False,
                # chroma (0.23.22) : vert de plateau, tolérance et douceur moyennes, débordement à moitié
                chroma_color=CHROMA_KEY_DEFAULT, chroma_tol=0.20, chroma_soft=0.15, chroma_spill=0.50,
                # motif (0.23.22) : iris au centre, à mi-taille, légèrement adouci
                pattern="iris", pattern_size=0.50, pattern_x=0.5, pattern_y=0.5, pattern_soft=0.10)


def _keyer_patch(k, body):
    """Applique à `k` les champs réglables présents dans `body`, bornés. Rend la liste des champs
    REFUSÉS (illisibles, hors vocabulaire, entrée inexistante) ; rien n'est appliqué s'il y en a
    un — l'appelant valide d'abord sur une copie (cf. _keyer_try)."""
    bad = []
    for key, (lo, hi, cast) in _KEYER_NUM.items():
        if key in body:
            try:
                k[key] = max(lo, min(hi, cast(body[key])))
            except Exception:
                bad.append(key)
    if "type" in body:
        t = str(body.get("type") or "").strip().lower()
        if t in KEYER_TYPES:
            k["type"] = t
        else:
            bad.append("type")
    for key in ("input", "key_input"):
        if key in body:
            try:
                v = int(body[key])
            except Exception:
                v = -1
            if 0 <= v < N_INPUTS:
                k[key] = v
            else:
                bad.append(key)
    if "invert" in body:
        k["invert"] = _as_bool(body["invert"])
    for key, vocab in (("auto_mode", KEYER_AUTO_MODES), ("auto_dir", WIPE_DIRECTIONS),
                       ("pattern", PATTERN_SHAPES)):
        if key in body:
            v = str(body.get(key) or "").strip().lower()
            if v in vocab:
                k[key] = v
            else:
                bad.append(key)
    if "chroma_color" in body:
        c = _norm_hex(body.get("chroma_color"))
        if c:
            k["chroma_color"] = c
        else:
            bad.append("chroma_color")
    if "auto_rev" in body:
        k["auto_rev"] = _as_bool(body["auto_rev"])
    if "auto_alt" in body:
        k["auto_alt"] = _as_bool(body["auto_alt"])
    if "clean" in body:
        k["clean"] = _as_bool(body["clean"])
    m = body.get("mask")
    if isinstance(m, dict):
        for e in _MASK_EDGES:
            if e in m:
                try:
                    k["mask"][e] = max(0.0, min(1.0, float(m[e])))
                except Exception:
                    bad.append("mask." + e)
    return bad


def _norm_hex(v):
    """« #RRGGBB » (ou « #RGB », dièse facultatif) → « #rrggbb » ; None si illisible."""
    h = str(v if v is not None else "").strip().lstrip("#").lower()
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    if len(h) != 6 or any(c not in "0123456789abcdef" for c in h):
        return None
    return "#" + h


def _keyers_init():
    """Keyers au démarrage : défauts, puis le document PERSISTÉ (clé `keyers` du document des
    délais, cf. GET /delays — un seul document persisté par plugin). Un champ illisible garde
    son défaut ; un keyer en trop dans le document (n_keyers réduit) est ignoré."""
    ks = [_keyer_default() for _ in range(N_KEYERS)]
    d = CONFIG.get("delays")
    saved = d.get("keyers") if isinstance(d, dict) else None
    if isinstance(saved, list):
        for k, doc in zip(ks, saved):
            if isinstance(doc, dict):
                for key in list(doc):
                    _keyer_patch(k, {{key: doc[key]}})
    return ks


_KEYERS_INIT = _keyers_init()

# Endpoints de RÉGLAGE d'un keyer → champs acceptés (tous persistés : control.persist.settings).
_KEYER_SETTINGS = dict((
    ("/keyer/input", ("input", "key_input")),
    ("/keyer/transform", ("x", "y", "scale", "border_width")),
    ("/keyer/type", ("type",)),
    ("/keyer/mask", _MASK_EDGES),
    ("/keyer/key", ("clip", "gain", "invert")),
    ("/keyer/rate", ("auto_ms", "auto_mode", "auto_dir", "auto_rev", "auto_soft", "auto_alt")),
    ("/keyer/clean", ("clean",)),
    ("/keyer/chroma", ("chroma_color", "chroma_tol", "chroma_soft", "chroma_spill")),
    ("/keyer/pattern", ("pattern", "pattern_size", "pattern_x", "pattern_y", "pattern_soft")),
))

# ─── Sauvegardes de keyer PAR MÉLANGEUR (0.23.16) ───────────────────────────────────────────
# Réglage complet d'un keyer (type, entrées, fly, masque, découpe, durée de l'AUTO, clean —
# jamais l'état d'antenne) rangé sous un nom, propre à CE conteneur. Persisté dans le même
# document que les délais et les keyers (GET /delays, clé `keyer_presets`) : l'orchestrateur
# n'en retient qu'un par plugin. BORNÉ en nombre et en longueur de nom — le document est relu à
# chaque lecture de la config du conteneur, il ne doit pas grossir sans limite.
KEYER_PRESETS_MAX = 32
KEYER_PRESET_NAME_MAX = 32


def _keyer_settings(k):
    """Réglages persistables d'un keyer (copie, sans l'état d'antenne)."""
    d = dict((f, k[f]) for f in _KEYER_PERSIST)
    d["mask"] = dict(k["mask"])
    return d


def _preset_name(v):
    """Nom de sauvegarde normalisé (espaces retirés, tronqué), ou None s'il est vide."""
    s = str(v if v is not None else "").strip()[:KEYER_PRESET_NAME_MAX]
    return s or None


def _keyer_patch_lenient(k, doc):
    """Applique `doc` champ par champ : un champ refusé (entrée qui n'existe plus sur ce
    mélangeur, type inconnu) garde la valeur courante au lieu de tout faire échouer. Rend la
    liste des champs écartés — l'appelant la publie, jamais en silence."""
    skipped = []
    for key in list(doc):
        if key not in _KEYER_PERSIST and key != "mask":
            continue
        skipped += _keyer_patch(k, {{key: doc[key]}})
    return skipped


def _norm_keyer_presets(raw):
    """Sauvegardes bornées : {{nom: réglages}}. Nom vide ou en trop → ignoré ; réglages relus
    à travers les bornes du moteur (une valeur illisible garde le défaut)."""
    out = dict()
    if not isinstance(raw, dict):
        return out
    for name, doc in raw.items():
        name = _preset_name(name)
        if not name or not isinstance(doc, dict) or name in out:
            continue
        if len(out) >= KEYER_PRESETS_MAX:
            break
        k = _keyer_default()
        _keyer_patch_lenient(k, doc)
        out[name] = _keyer_settings(k)
    return out


def _keyer_presets_init():
    d = CONFIG.get("delays")
    return _norm_keyer_presets(d.get("keyer_presets") if isinstance(d, dict) else None)


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
            # N/R (0.23.20) : le sens s'inverse de lui-même à la fin de chaque volet exécuté.
            "alternate": False,
        }},
    }},
    # KEYERS AMONT (0.23.14) : N keyers sur le PGM (pas dans le clean). `keyer` = le keyer 1,
    # MÊME objet que keyers[0] (compatibilité des endpoints et de /state.keyer).
    "keyers":         _KEYERS_INIT,
    "keyer":          _KEYERS_INIT[0],
    # AUTO propre à chaque keyer : {{idx: {{start, duration_ms, from_active, to_active}}}}.
    "key_autos":      {{}},
    # Cibles du prochain take/mix : le fond (BGD) et/ou chaque keyer, ensemble.
    "next_targets": {{"bgd": True, "keyers": [False] * N_KEYERS}},
    "inputs":          [str(CONFIG.get("input_%d" % (i + 1)) or "") for i in range(N_INPUTS)],
    "overlay_url":     INITIAL_OVERLAY_URL,
    "overlay_enabled": bool(INITIAL_OVERLAY_ENABLED),
    # Dissolve par défaut (ms) — mutable à chaud via POST /config (persisté par le hook
    # control_action puis repoussé ici ; survit ainsi au redeploy).
    "default_dissolve_ms": INITIAL_DISSOLVE_MS,
    # Délai par défaut (⚙, à chaud via /latency) et groupes de délai (/delays, persistés par
    # l'orchestrateur — control.persist).
    "latency_mode": _latency_mode(CONFIG.get("latency_mode")),
    "delays": _norm_delays(CONFIG.get("delays")),
    # Sauvegardes de keyer nommées, propres à ce mélangeur (persistées avec les délais).
    "keyer_presets": _keyer_presets_init(),
}}

def _nr_after(used):
    """Fin d'une transition du fond (appelant sous state_lock) : en N/R, le sens du PROCHAIN volet
    s'inverse — seulement si ce qui vient d'être joué ÉTAIT un volet (`used` = preset capté au
    départ ; un fondu ne consomme pas de sens)."""
    w = state["preset"]["wipe"]
    if w.get("alternate") and used and used.get("mode") == "wipe":
        w["reverse"] = not w.get("reverse")


def _snapshot_preset():
    """Copie défensive du preset courant — capté au moment de créer un mix. Elle fige ce qui est
    GÉOMÉTRIQUE (mode, direction, sens) : changé en cours de volet, le bord sauterait. L'adoucissement,
    lui, n'est PAS lu ici mais à chaque trame (_live_wipe_preset)."""
    p = state["preset"]
    return {{"mode": p["mode"], "wipe": dict(p["wipe"])}}


def _live_wipe_preset(frozen):
    """Preset d'une transition du fond pour LA trame (appelant sous state_lock) : la géométrie
    figée au départ (`frozen`, cf. _snapshot_preset) + l'adoucissement VIVANT du preset courant —
    un paramètre continu qu'on tourne en regardant le résultat (pupitre, macro) doit agir sur le
    volet EN COURS, pas au suivant (relevé 2026-09-01, corrigé 2026-10-26). Lu une fois par trame :
    toutes les bandes d'une trame en mode tranche voient la même valeur, donc aucune couture.
    `frozen` reste intact — c'est lui que _nr_after consulte à la fin du volet."""
    if not frozen:
        return frozen
    w = dict(frozen.get("wipe") or dict())
    w["softness"] = state["preset"]["wipe"].get("softness", w.get("softness", 0.10))
    return dict(frozen, wipe=w)

# ─── Keyers : adressage, mouvements, alpha (0.23.14) ─────────
def _keyer_idx(body):
    """Keyer visé, 0-based : `idx` (0-based — charge utile de l'arbre de paramètres des macros)
    ou `keyer` (1-based, comme l'interface et les actions) ; ni l'un ni l'autre = keyer 1, ce qui
    garde les appels d'avant 0.23.14 sur l'ancien keyer. None si hors plage ou illisible."""
    try:
        if body.get("idx") is not None:
            i = int(body.get("idx"))
        elif body.get("keyer") is not None:
            i = int(body.get("keyer")) - 1
        else:
            i = 0
    except Exception:
        return None
    return i if 0 <= i < N_KEYERS else None


def _keyer_part(i):
    """Mouvement d'un keyer dans une transition : il passe à l'état inverse de l'actuel."""
    a = bool(state["keyers"][i]["active"])
    return {{"from_active": a, "to_active": not a}}


def _targeted_keyers():
    return [i for i, on in enumerate(state["next_targets"]["keyers"]) if on]


def _cancel_keyer_motion(i):
    """Retire le keyer i de toute transition M/E, T-bar ou AUTO en cours (sous state_lock).
    Un mouvement qui ne porte plus rien disparaît ; la couche BGD, elle, continue."""
    for name in ("manual_mix", "transition"):
        mv = state[name]
        if mv and i in (mv.get("keyers") or dict()):
            mv["keyers"].pop(i, None)
            if not mv["keyers"] and not mv.get("bgd"):
                state[name] = None
    state["key_autos"].pop(i, None)


def _progress(start, duration_ms):
    return max(0.0, min(1.0, (time.time() - start) * 1000.0 / max(1, duration_ms)))


def _keyer_alpha(i, active, manual, trans, auto):
    """Alpha d'antenne du keyer i pour LA trame : son état, ou sa position dans le mouvement qui
    le porte — T-bar, transition M/E, ou son AUTO propre (par ordre de priorité ; un ON AIR les
    annule tous, cf. _cancel_keyer_motion)."""
    part = None
    p = 0.0
    if manual is not None and i in (manual.get("keyers") or dict()):
        part = manual["keyers"][i]
        p = max(0.0, min(1.0, manual["alpha"]))
    elif trans is not None and i in (trans.get("keyers") or dict()):
        part = trans["keyers"][i]
        p = _progress(trans["start"], trans["duration_ms"])
    elif auto is not None:
        part = auto
        p = _progress(auto["start"], auto["duration_ms"])
    if part is None:
        return 1.0 if active else 0.0
    fr = 1.0 if part["from_active"] else 0.0
    to = 1.0 if part["to_active"] else 0.0
    return fr + (to - fr) * p


def _latch_keyers(manual, trans):
    """Instantané des keyers pour LA trame (appelant sous state_lock) : copie profonde des
    réglages + alpha courant. Sert à TOUTES les bandes — jamais de changement à mi-trame."""
    out = []
    for i, k in enumerate(state["keyers"]):
        kl = dict(k)
        kl["mask"] = dict(k["mask"])
        au = state["key_autos"].get(i)
        kl["alpha"] = _keyer_alpha(i, k["active"], manual, trans, au)
        kl["wmask"] = None
        # AUTO propre en VOLET : le keyer est composé à pleine opacité, puis révélé par le masque
        # du volet (cf. _sl_wipe_band). Seulement quand c'est SON AUTO qui le porte : emporté par la
        # T-bar ou la transition M/E, il suit le mouvement commun — les deux s'excluent déjà (une
        # transition M/E annule l'AUTO du keyer, un AUTO retire le keyer de la transition).
        # Géométrie (mode, direction, sens) FIGÉE au départ de l'AUTO (`au`) ; adoucissement lu
        # VIVANT sur le keyer à chaque trame, comme le volet du fond (_live_wipe_preset).
        if au is not None and au.get("mode", k.get("auto_mode")) == "wipe":
            kl["wmask"] = _wipe_alpha_map(kl["alpha"], au.get("dir", k.get("auto_dir", "horizontal")),
                                          bool(au.get("rev", k.get("auto_rev"))),
                                          float(k.get("auto_soft", 0.10)))
            kl["alpha"] = 1.0
        out.append(kl)
    return out


def _pvw_future(pgm, pvw, manual, trans, keyers_now):
    """Sortie PVW « comme un ATEM » (0.23.29) : ce que sera le PGM APRÈS la prochaine transition,
    SANS l'habillage HTML. Appelant sous state_lock, au même latch que `keyers_now` (une trame).
    Rend (entrée du fond du PVW, keyers du PVW) — mêmes instantanés que `keyers_now`, dans le même
    ordre (1 dessous), alpha 1,0 si le keyer figure au PVW, 0,0 sinon : les fonctions de
    composition du PGM s'en servent telles quelles et sautent les keyers invisibles.

    Hors mouvement : fond = entrée PVW si BKGD est dans la prochaine transition, sinon l'entrée PGM ;
    keyer i visible ⇔ (à l'antenne) XOR (ciblé dans next_targets) — un keyer à l'antenne non ciblé
    reste, un keyer ciblé bascule.
    PENDANT une transition M/E (AUTO ou T-bar) : le PVW montre l'ÉTAT FINAL DE CETTE TRANSITION
    (fond d'arrivée si elle porte BKGD, keyers emportés à leur état d'arrivée, les autres à leur état
    d'antenne) et n'applique PAS next_targets par-dessus : la transition en cours EST la « prochaine »
    qu'on a prise (ses cibles en viennent), les appliquer deux fois montrerait l'état d'après la
    suivante. Pas de bascule au milieu du mouvement — l'image ne change qu'à son terme, comme
    l'ancien PVW (entrée PVW brute = fond d'arrivée).
    AUTO propre d'un keyer en cours (sans transition M/E) : son état d'arrivée, puis next_targets."""
    me = manual if manual is not None else trans
    if me is not None:
        bgd = me["bgd"].get("to_idx", pvw) if me.get("bgd") else pgm
        parts = me.get("keyers") or dict()
        vis = [bool(parts[i]["to_active"]) if i in parts else bool(k["active"])
               for i, k in enumerate(state["keyers"])]
    else:
        nt = state["next_targets"]
        bgd = pvw if nt["bgd"] else pgm
        vis = []
        for i, k in enumerate(state["keyers"]):
            au = state["key_autos"].get(i)
            on = bool(au["to_active"]) if au is not None else bool(k["active"])
            vis.append(on != bool(nt["keyers"][i]))
    out = []
    for kl, v in zip(keyers_now, vis):
        kp = dict(kl)
        kp["alpha"] = 1.0 if v else 0.0
        kp["wmask"] = None
        out.append(kp)
    return bgd, out


# TALLY (0.23.35) : les entrées RÉELLEMENT À L'IMAGE, calculées par la boucle à chaque trame sur
# l'état qu'elle vient de figer — le tally n'a plus à deviner ce que le mélangeur compose. Décision de
# l'utilisateur (28/10) : « le tally est général au mélangeur ; une source au PGM via un key doit être
# active aussi, sauf si le key a une opacité à 0 ou que la DVE la place hors de l'image ». Donc :
#   · fond : l'entrée PGM ; PENDANT une transition, les deux entrées encore visibles (départ tant que
#     alpha < 1, arrivée dès que alpha > 0) ;
#   · keyers : chaque keyer d'opacité > 0 dont l'image, masque compris, touche le cadre
#     (`_sl_keyer_geom` non nul — la même règle que la composition) allume son FILL et, pour un
#     linear, son KEY : « tout ce qui est utilisé à un instant t pour le PGM doit avoir un tally »
#     (l'utilisateur, 28/10 — la découpe sert au PGM même si on ne la voit pas) ;
#   · CLEAN (0.23.38) : le même fond et les seuls keyers réglés sur CLEAN — c'est la liste que
#     remonte la propagation quand c'est la sortie CLEAN qui est à l'antenne en aval ;
#   · PVW : le fond futur et les keyers visibles au PVW (cf. _pvw_future).
_tally_now = dict(pgm=[], clean=[], pvw=[])


def _tally_inputs(bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, pgm, keyers_now, pvw_bgd, pvw_keyers):
    def _vis(stack, out):
        for ks in stack or ():
            if float(ks.get("alpha", 0.0)) > 0.001 and _sl_keyer_geom(ks) is not None:
                out.add(int(ks["input"]))
                if ks.get("type") == "linear" and ks.get("key_input") is not None:
                    out.add(int(ks["key_input"]))
    on = set()
    if bgd_active:
        if bgd_alpha < 1.0:
            on.add(int(bgd_from_idx))
        if bgd_alpha > 0.0:
            on.add(int(bgd_to_idx))
    else:
        on.add(int(pgm))
    cl = set(on)                     # CLEAN = même fond, keyers réglés « sur CLEAN » seulement
    _vis(keyers_now, on)
    _vis([k for k in keyers_now if k.get("clean")], cl)
    pv = set([int(pvw_bgd)])
    _vis(pvw_keyers, pv)
    return dict(pgm=sorted(on), clean=sorted(cl), pvw=sorted(pv))


# Garde du PVW en IMAGE ENTIÈRE (0.23.30). Mesuré le 28/10 sur mes-mixwf (1 cœur, 1080p, keyers
# linear plein cadre) : ce chemin tient déjà à peine 2 keyers au PGM (20 ms par trame, 46 i/s), et
# les keyers du PVW y coûtent 4,4 ms chacun — 2 au PGM + 2 au PVW tombaient à 28 i/s. Le PVW est un
# retour : il ne doit JAMAIS faire perdre une trame au PGM. Ses keyers ne sont donc composés que si
# le temps déjà passé sur la trame + leur dernier coût mesuré tient dans PVW_BUDGET_FRAC de la
# période ; sinon la trame PVW sort sans eux (fond futur seul), et c'est COMPTÉ
# (`pvw_keyers_skipped` sur :8080). Le mode tranche n'est pas concerné (+1,7 ms par keyer mesuré,
# 14 ms au pire sur 20).
PVW_BUDGET_FRAC = 0.85
_pvw_cost_ms = 0.0     # dernier coût mesuré des keyers du PVW (image entière)
_pvw_skips = 0


def _finish_keyer_autos():
    """AUTO de keyer arrivés à terme → état final (appelant sous state_lock)."""
    for i, au in list(state["key_autos"].items()):
        if _progress(au["start"], au["duration_ms"]) >= 1.0:
            k = state["keyers"][i]
            k["active"] = au["to_active"]
            if k.get("auto_alt") and au.get("mode", k.get("auto_mode")) == "wipe":
                k["auto_rev"] = not k.get("auto_rev")      # N/R : sens inversé pour le prochain AUTO
            state["key_autos"].pop(i, None)


def _keyers_doc():
    """Réglages persistables des keyers (appelant sous state_lock)."""
    out = []
    for k in state["keyers"]:
        d = dict((f, k[f]) for f in _KEYER_PERSIST)
        d["mask"] = dict(k["mask"])
        out.append(d)
    return out


def _targets_pub():
    t = state["next_targets"]
    return {{"bgd": bool(t["bgd"]), "keyer": bool(t["keyers"][0]), "keyers": list(t["keyers"])}}


def _keyers_pub():
    """Keyers publiés dans /state (appelant sous state_lock) : réglages, état d'antenne et
    mouvement en cours — `active` est l'état ON AIR, lisible en condition de macro."""
    mm = state["manual_mix"] or dict()
    tr = state["transition"] or dict()
    out = []
    for i, k in enumerate(state["keyers"]):
        d = dict(k)
        d["mask"] = dict(k["mask"])
        d["n"] = i + 1
        d["moving"] = (i in (mm.get("keyers") or dict()) or i in (tr.get("keyers") or dict())
                       or i in state["key_autos"])
        d["auto"] = i in state["key_autos"]
        out.append(d)
    return out


def _calc_keyer_alpha(keyer_state, manual, trans):
    """Alpha du keyer 1 (compatibilité : signature d'avant 0.23.14) — cf. _keyer_alpha."""
    return _keyer_alpha(0, keyer_state["active"], manual, trans, None)


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
        # Avance des index ÉMIS sur la grille TAI + trames retenues (cf. bobimxl.index_report) :
        # seul témoin d'une sortie écrite dans le futur — la cadence reste à 50,0 pendant ce temps.
        payload["index_ahead"] = bobimxl.index_report()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass


def _state_doc():
    """Document /state complet — servi par GET /state ET poussé par le flux /events."""
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
                "keyer": 0 in (tr.get("keyers") or dict()),        # keyer 1 (compat)
                "keyers": sorted(i + 1 for i in (tr.get("keyers") or dict())),
            }}),
            "manual_mix":      (None if mm is None else {{
                "alpha": mm["alpha"],
                "bgd":   (None if mm.get("bgd") is None else {{
                    "from_idx": mm["bgd"]["from_idx"],
                    "to_idx":   mm["bgd"]["to_idx"],
                }}),
                "keyer": (dict((mm.get("keyers") or dict())[0])
                          if 0 in (mm.get("keyers") or dict()) else None),   # keyer 1 (compat)
                "keyers": dict((str(i + 1), dict(p))
                               for i, p in (mm.get("keyers") or dict()).items()),
            }}),
            "preset": {{
                "mode": state["preset"]["mode"],
                "wipe": dict(state["preset"]["wipe"]),
            }},
            "keyer":         _keyers_pub()[0],   # keyer 1 (compat)
            "keyers":        _keyers_pub(),
            "n_keyers":      N_KEYERS,
            "max_keyers":    MAX_KEYERS,
            "next_targets":  _targets_pub(),
            # Entrées À L'IMAGE (0.23.35, cf. _tally_inputs) : la source du tally de l'orchestrateur.
            "tally":         dict(_tally_now),
            # Noms des sauvegardes de keyer de CE mélangeur (0.23.16), triés.
            "keyer_presets": sorted(state["keyer_presets"]),
            "inputs":          list(state["inputs"]),
            "n_inputs":        N_INPUTS,
            "max_inputs":      MAX_INPUTS,
            "config_warning":  CONFIG_WARNING,   # écrêtage signalé, jamais muet
            "overlay_url":     state["overlay_url"],
            "overlay_enabled": state["overlay_enabled"],
            "default_dissolve_ms": state["default_dissolve_ms"],
            # Cadence CONFIGURÉE (0.23.19) : l'interface affiche les durées en
            # secondes:images. `fps` des métriques est la cadence MESURÉE, autre chose.
            "frame_rate":     FPS,
            "plugin_version": PLUGIN_VERSION,
            "log_level":      LOG_LEVEL,   # lisible en condition de macro
            "mxl_lib":        _mxl_lib_state(),
            # Délais (0.23.9) : délai par défaut, groupes, domaine des valeurs pour l'UI.
            "slice_mode":     SLICE_ON,
            "latency_mode":   state["latency_mode"],
            "delays":         _norm_delays(state["delays"]),
            "caps": {{"keyer_types": list(KEYER_TYPES),
                     "upstream_max": UPSTREAM_MAX,
                     "n_keyers": N_KEYERS,
                     "keyer_auto_ms_max": KEYER_AUTO_MS_MAX,
                     "keyer_auto_modes": list(KEYER_AUTO_MODES),
                     "wipe_directions": list(WIPE_DIRECTIONS),
                     "pattern_shapes": list(PATTERN_SHAPES),
                     "key_colorimetry": KEY_COLORIMETRY,
                     "key_colorimetry_assumed": KEY_COLORIMETRY_ASSUMED,
                     "arrival_trend_step_s": ARRIVAL_TREND_STEP_S,
                     "keyer_presets_max": KEYER_PRESETS_MAX,
                     "keyer_preset_name_max": KEYER_PRESET_NAME_MAX,
                     "key_gain": [KEY_GAIN_MIN, KEY_GAIN_MAX, 1.0],
                     "latency_modes": list(LATENCY_MODES),
                     "group_delay_max": GROUP_DELAY_MAX,
                     "group_name_max": GROUP_NAME_MAX}},
        }}
        # state_field par slot (topologie / câblage générique)
        for _i in range(N_INPUTS):
            s["input_%d" % (_i + 1)] = state["inputs"][_i]
    s["inputs_timing"] = _inputs_timing()   # hors verrou : il le reprend
    return s


# ─── Flux d'état poussé : GET /events (0.23.28) ──────────────────────────────────────────────
# La page du mélangeur interrogeait /state toutes les 2 s : une commutation faite au pupitre ATEM
# mettait jusqu'à 2 s à y apparaître. Le flux pousse le document /state complet À CHAQUE
# CHANGEMENT, en `text/event-stream`, relayé par l'orchestrateur (control.streams du manifeste,
# app/plugin_streams.py : relais morceau par morceau, coupé après 3 s de silence — d'où la ligne
# de vie chaque seconde).
# Un changement, c'est : toute requête POST traitée (pupitre, page, macros — `_state_bump`), OU
# un changement vu par la boucle vidéo (fin de transition, fin d'AUTO de keyer), relevé sur une
# signature lue toutes les EVENTS_POLL_S. Les champs MESURÉS (inputs_timing) ne déclenchent rien :
# ils bougent à chaque trame, et la page continue de les relire par /state.
EVENTS_POLL_S = 0.04
EVENTS_KEEPALIVE_S = 1.0
EVENTS_MAX_CLIENTS = 16
_events_cv = threading.Condition()
_events_rev = [0]
_events_clients = [0]


def _state_bump():
    with _events_cv:
        _events_rev[0] += 1
        _events_cv.notify_all()


def _state_signature():
    """Ce que la boucle vidéo peut changer SANS requête : bus, transition, keyers à l'antenne,
    AUTO de keyer en cours, mélange manuel."""
    with state_lock:
        return (state["pgm"], state["pvw"], state["transition"] is None,
                tuple(bool(k.get("active")) for k in state["keyers"]),
                tuple(sorted(state["key_autos"])), state["manual_mix"] is None)


# Les routes historiques restent traitées UNE PAR UNE (elles l'étaient par un HTTPServer
# mono-fil, et rien n'a été écrit pour la concurrence) ; seul /events tourne à côté.
_ctl_serial = threading.Lock()


class ControlHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/events":
            self._events()
            return
        with _ctl_serial:
            self._do_GET_serial()

    def do_POST(self):
        try:
            with _ctl_serial:
                self._do_POST_serial()
        finally:
            _state_bump()

    def _events(self):
        with _events_cv:
            if _events_clients[0] >= EVENTS_MAX_CLIENTS:
                busy = True
            else:
                busy = False
                _events_clients[0] += 1
        if busy:
            self._reply(503, {{"error": "too many event streams"}})
            return
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            rev, sig, last_tx = None, None, 0.0
            while True:
                with _events_cv:
                    if _events_rev[0] == rev:
                        _events_cv.wait(EVENTS_POLL_S)
                    cur_rev = _events_rev[0]
                cur_sig = _state_signature()
                now = time.monotonic()
                if cur_rev != rev or cur_sig != sig:
                    rev, sig = cur_rev, cur_sig
                    self.wfile.write(b"data: " + json.dumps(_state_doc()).encode() + b"\n\n")
                    self.wfile.flush()
                    last_tx = now
                elif now - last_tx >= EVENTS_KEEPALIVE_S:
                    self.wfile.write(b": ka\n\n")
                    self.wfile.flush()
                    last_tx = now
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with _events_cv:
                _events_clients[0] -= 1

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

    def _do_GET_serial(self):
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
            self._reply(200, _state_doc())
        elif self.path == "/delays":
            # Instantané persisté par l'orchestrateur (control.persist) — le document seul.
            # Le document persisté porte AUSSI les réglages des keyers (0.23.14) : l'orchestrateur ne
            # persiste qu'un document par plugin (control.persist), relu au démarrage par _keyers_init.
            with state_lock:
                d = _norm_delays(state["delays"])
                d["keyers"] = _keyers_doc()
                # Sauvegardes de keyer (0.23.16) : même document, relu par _keyer_presets_init.
                d["keyer_presets"] = dict((n, _keyer_settings(v))
                                          for n, v in state["keyer_presets"].items())
            self._reply(200, d)
        elif self.path == "/keyer/presets":
            # Liste vivante des sauvegardes (options_endpoint de l'action de macro) + leur contenu.
            with state_lock:
                names = sorted(state["keyer_presets"])
                presets = dict((n, _keyer_settings(state["keyer_presets"][n])) for n in names)
            self._reply(200, {{"names": names, "presets": presets,
                              "max": KEYER_PRESETS_MAX, "name_max": KEYER_PRESET_NAME_MAX}})
        else:
            self._reply(404, {{"error": "not found"}})

    def _do_POST_serial(self):
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

        elif self.path == "/latency":
            # Délai PAR DÉFAUT, à chaud : "fixed_1" | "fastest". Si le groupe de référence suit le
            # défaut, passer de fastest à fixed_1 recule le numéro de sortie d'un cran → UNE trame
            # retenue ; l'inverse saute une image.
            m = str(body.get("mode") or body.get("latency_mode") or "").strip().lower()
            if m not in LATENCY_MODES:
                self._reply(400, {{"error": "mode attendu : " + " | ".join(LATENCY_MODES)}}); return
            with state_lock:
                state["latency_mode"] = m
            log("mixer: default delay -> %s" % m, "info")
            self._reply(200, {{"ok": True, "latency_mode": m}})

        elif self.path == "/delays":
            # GROUPES DE DÉLAI, à chaud. Formes acceptées (combinables) :
            #   {{"groups": {{nom: 0..6}}}}              remplace la liste des groupes
            #   {{"group": nom, "frames": 0..6}}         crée ou règle un groupe
            #   {{"remove_group": nom}}                  supprime un groupe (ses entrées redeviennent sans groupe)
            #   {{"inputs": {{"1": nom|null, …}}}}        affecte des entrées (numérotées à partir de 1)
            #   {{"input": n, "group": nom|null}}        affecte une entrée — n À PARTIR DE 0, comme
            #                                          /pgm et /pvw (actions `options_from: inputs`)
            #   {{"ref_group": nom|null}}                groupe de référence (numéro du PGM)
            #   {{"upstream": {{"1": 0..5, …}}}}           décalage amont (entrées numérotées à partir de 1)
            #   {{"input": n, "upstream": 0..5}}         décalage amont d'une entrée — n À PARTIR DE 0
            # Le document est re-borné par _norm_delays : valeur hors bornes → ignorée, jamais écrêtée
            # en silence (la réponse rend le document effectif).
            with state_lock:
                d = _norm_delays(state["delays"])
                g = dict(d["groups"]); ins = dict(d["inputs"]); ref = d["ref_group"]
                up = dict(d["upstream"])
                if isinstance(body.get("groups"), dict):
                    g = dict(body["groups"])
                if "group" in body and "frames" in body and "input" not in body:
                    g[str(body.get("group") or "").strip()[:GROUP_NAME_MAX]] = body.get("frames")
                if body.get("remove_group") in g:
                    g.pop(body["remove_group"])
                if isinstance(body.get("inputs"), dict):
                    for k, v in body["inputs"].items():
                        if v: ins[str(k)] = v
                        else: ins.pop(str(k), None)
                def _set_up(k, v):
                    # Hors bornes ou illisible → ignoré (la valeur en place reste) ; 0 → retiré.
                    try: v = int(v)
                    except Exception: return
                    if v == 0: up.pop(k, None)
                    elif 1 <= v <= UPSTREAM_MAX: up[k] = v
                if isinstance(body.get("upstream"), dict):
                    for k, v in body["upstream"].items():
                        _set_up(str(k), v)
                if "input" in body:
                    try: k = str(int(body.get("input")) + 1)
                    except Exception: k = ""
                    # {{"input": n, "upstream": u}} seul ne touche PAS au groupe de l'entrée.
                    if "group" in body or "upstream" not in body:
                        v = body.get("group")
                        if v: ins[k] = v
                        else: ins.pop(k, None)
                    if k and "upstream" in body and not isinstance(body.get("upstream"), dict):
                        _set_up(k, body.get("upstream"))
                if "ref_group" in body:
                    ref = body.get("ref_group") or None
                new = _norm_delays({{"groups": g, "inputs": ins, "ref_group": ref, "upstream": up}})
                state["delays"] = new
            log("mixer: delay groups -> %s" % json.dumps(new, ensure_ascii=False), "info")
            self._reply(200, dict(new, ok=True))

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
            # Take selon next_targets : BGD et/ou les keyers ciblés. duration_ms=0 = cut sec.
            duration = body.get("duration_ms")
            if duration is None:
                with state_lock: duration = state["default_dissolve_ms"]
            try: duration = max(0, int(duration))
            except Exception:
                self._reply(400, {{"error": "duration_ms invalide"}}); return
            with state_lock:
                targets = state["next_targets"]
                bgd_part = None
                if targets["bgd"]:
                    fr = state["pgm"]; to = state["pvw"]
                    if fr != to:
                        bgd_part = {{"from_idx": fr, "to_idx": to, "flip_to_pvw": fr}}
                kparts = dict((i, _keyer_part(i)) for i in _targeted_keyers())
                if bgd_part is None and not kparts:
                    self._reply(200, {{"ok": True, "noop": True}}); return
                state["manual_mix"] = None
                for i in kparts:
                    state["key_autos"].pop(i, None)   # la transition M/E reprend ce keyer
                if duration <= 0:
                    # Cut sec : applique immédiatement
                    if bgd_part:
                        state["pgm"] = bgd_part["to_idx"]
                        state["pvw"] = bgd_part["flip_to_pvw"]
                    for i, kp in kparts.items():
                        state["keyers"][i]["active"] = kp["to_active"]
                    state["transition"] = None
                else:
                    state["transition"] = {{
                        "start": time.time(),
                        "duration_ms": duration,
                        "bgd":      bgd_part,
                        "keyers":   kparts,
                        "preset":   _snapshot_preset(),
                    }}
            self._reply(200, {{"ok": True}})

        elif self.path == "/mix":
            # Crossfade T-bar. Agit selon next_targets : BGD et/ou keyers ciblés.
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
                            _nr_after(mm.get("preset"))
                        for i, kp in (mm.get("keyers") or dict()).items():
                            state["keyers"][i]["active"] = kp["to_active"]
                    else:
                        # Pas de mix actif : commit selon next_targets directement.
                        t = state["next_targets"]
                        if t["bgd"] and state["pgm"] != state["pvw"]:
                            state["pgm"], state["pvw"] = state["pvw"], state["pgm"]
                        for i in _targeted_keyers():
                            state["keyers"][i]["active"] = not state["keyers"][i]["active"]
                            state["key_autos"].pop(i, None)
                    state["manual_mix"] = None
                elif alpha <= 0.001:
                    state["manual_mix"] = None
                else:
                    mm = state["manual_mix"]
                    if mm is None:
                        # Démarre un nouveau mix manuel selon next_targets.
                        t = state["next_targets"]
                        bgd_part = None
                        if t["bgd"] and state["pgm"] != state["pvw"]:
                            bgd_part = {{"from_idx": state["pgm"], "to_idx": state["pvw"]}}
                        kparts = dict((i, _keyer_part(i)) for i in _targeted_keyers())
                        if bgd_part is None and not kparts:
                            self._reply(200, {{"ok": True, "noop": True}}); return
                        for i in kparts:
                            state["key_autos"].pop(i, None)
                        state["manual_mix"] = {{
                            "alpha":  alpha,
                            "bgd":    bgd_part,
                            "keyers": kparts,
                            "preset": _snapshot_preset(),
                        }}
                    else:
                        mm["alpha"] = alpha
            self._reply(200, {{"ok": True}})

        elif self.path == "/targets":
            # Cibles du prochain take/mix (PATCH-like). Au moins une doit rester active.
            # Formes acceptées (combinables) :
            #   {{"bgd": bool}}                       le fond
            #   {{"keyer": bool}}                     le keyer 1 (forme d'avant 0.23.14)
            #   {{"keyer2": bool}} … {{"keyer4": bool}} un keyer par son numéro
            #   {{"keyers": [bool, …]}}               tous, dans l'ordre
            #   {{"keyer_n": n, "on": bool}}          un keyer, numéro en valeur (actions de macro)
            # ⚠ Ici `keyer` est un BOOLÉEN (cible du keyer 1), pas un numéro comme sur /keyer/*.
            with state_lock:
                t = state["next_targets"]
                ks = t["keyers"]
                if "bgd" in body: t["bgd"] = _as_bool(body.get("bgd"))
                if "keyer" in body: ks[0] = _as_bool(body.get("keyer"))
                for n in range(N_KEYERS):
                    if ("keyer%d" % (n + 1)) in body:
                        ks[n] = _as_bool(body.get("keyer%d" % (n + 1)))
                lst = body.get("keyers")
                if isinstance(lst, list):
                    for n, v in enumerate(lst[:N_KEYERS]):
                        ks[n] = _as_bool(v)
                if "keyer_n" in body:
                    try: n = int(body.get("keyer_n")) - 1
                    except Exception: n = -1
                    if not (0 <= n < N_KEYERS):
                        self._reply(400, {{"error": "keyer_n hors plage (1..%d)" % N_KEYERS}}); return
                    ks[n] = _as_bool(body.get("on", True))
                # Si tout est éteint, on rallume BGD par défaut.
                if not t["bgd"] and not any(ks): t["bgd"] = True
                pub = _targets_pub()
            self._reply(200, {{"ok": True, "next_targets": pub}})

        elif self.path == "/keyer/active":
            # ON AIR d'un keyer, COUPE SÈCHE : {{"active": bool}} ou bascule ; numéro par
            # `keyer` (1-based) ou `idx` (0-based), keyer 1 par défaut. Annule tout mouvement
            # en cours de CE keyer (transition M/E, T-bar, AUTO) — les autres continuent.
            i = _keyer_idx(body)
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            with state_lock:
                k = state["keyers"][i]
                if "active" in body:
                    k["active"] = _as_bool(body.get("active"))
                else:
                    k["active"] = not k["active"]
                _cancel_keyer_motion(i)
                on = k["active"]
            self._reply(200, {{"ok": True, "keyer": i + 1, "active": on}})

        elif self.path == "/keyer/auto":
            # AUTO d'un keyer : fondu vers l'état inverse, sur SA durée (`auto_ms`, ou
            # `duration_ms` du corps), indépendant de la transition M/E. 0 = coupe sèche.
            # Un AUTO déjà en cours sur ce keyer n'est pas relancé (réponse `busy`).
            i = _keyer_idx(body)
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            with state_lock:
                k = state["keyers"][i]
                dur = body.get("duration_ms")
                try: dur = k["auto_ms"] if dur is None else max(0, min(KEYER_AUTO_MS_MAX, int(dur)))
                except Exception:
                    self._reply(400, {{"error": "duration_ms invalide"}}); return
                if i in state["key_autos"]:
                    self._reply(200, {{"ok": True, "busy": True, "keyer": i + 1}}); return
                part = _keyer_part(i)
                _cancel_keyer_motion(i)
                if dur <= 0:
                    k["active"] = part["to_active"]
                else:
                    # Géométrie du volet figée au départ (cf. _latch_keyers).
                    state["key_autos"][i] = dict(part, start=time.time(), duration_ms=dur,
                                                 mode=k.get("auto_mode"), dir=k.get("auto_dir"),
                                                 rev=bool(k.get("auto_rev")))
            self._reply(200, {{"ok": True, "keyer": i + 1, "to_active": part["to_active"],
                              "duration_ms": dur}})

        elif self.path in _KEYER_SETTINGS:
            # Réglages d'un keyer (persistés : control.persist). Numéro par `keyer` (1-based) ou
            # `idx` (0-based), keyer 1 par défaut. Valeur hors bornes → ramenée dans les bornes ;
            # valeur ILLISIBLE → 400 qui la nomme, et rien n'est appliqué.
            i = _keyer_idx(body)
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            fields = _KEYER_SETTINGS[self.path]
            if self.path == "/keyer/mask":
                patch = {{"mask": dict((e, body[e]) for e in _MASK_EDGES if e in body)}}
                if isinstance(body.get("mask"), dict):
                    patch["mask"].update(body["mask"])
                if not patch["mask"]:
                    patch = dict()
            else:
                patch = dict((f, body[f]) for f in fields if f in body)
            if self.path == "/keyer/input" and not patch:
                self._reply(400, {{"error": "input ou key_input requis"}}); return
            with state_lock:
                k = state["keyers"][i]
                trial = dict(k)
                trial["mask"] = dict(k["mask"])
                bad = _keyer_patch(trial, patch)
                if bad:
                    self._reply(400, {{"error": "valeur refusée : " + ", ".join(bad),
                                      "rejected": bad}}); return
                k.update(trial)
                doc = dict((f, k[f]) for f in _KEYER_PERSIST)
                doc["mask"] = dict(k["mask"])
            self._reply(200, {{"ok": True, "keyer": i + 1, "settings": doc}})

        elif self.path == "/keyer/settings":
            # Réglages COMPLETS d'un keyer d'un coup (coller un keyer copié) : {{"keyer": n,
            # "settings": {{type, input, key_input, x, y, scale, border_width, mask, clip, gain,
            # invert, auto_ms, clean}}}}. L'état d'antenne n'en fait jamais partie (ignoré s'il est
            # là). Un champ refusé (entrée inexistante ici…) garde sa valeur et est NOMMÉ.
            i = _keyer_idx(body)
            doc = body.get("settings")
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            if not isinstance(doc, dict):
                self._reply(400, {{"error": "settings (objet) requis"}}); return
            with state_lock:
                k = state["keyers"][i]
                trial = dict(k)
                trial["mask"] = dict(k["mask"])
                skipped = _keyer_patch_lenient(trial, doc)
                k.update(trial)
                out = _keyer_settings(k)
            log("mixer: keyer %d settings pasted%s"
                % (i + 1, (" (skipped: %s)" % ", ".join(skipped)) if skipped else ""), "info")
            self._reply(200, {{"ok": True, "keyer": i + 1, "settings": out, "skipped": skipped}})

        elif self.path == "/keyer/preset/save":
            # Range le réglage du keyer sous un nom (écrase une sauvegarde du même nom).
            i = _keyer_idx(body)
            name = _preset_name(body.get("name"))
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            if not name:
                self._reply(400, {{"error": "name requis"}}); return
            with state_lock:
                ps = state["keyer_presets"]
                if name not in ps and len(ps) >= KEYER_PRESETS_MAX:
                    self._reply(400, {{"error": "trop de sauvegardes (max %d)" % KEYER_PRESETS_MAX,
                                      "max": KEYER_PRESETS_MAX}}); return
                ps[name] = _keyer_settings(state["keyers"][i])
                names = sorted(ps)
            log("mixer: keyer %d saved as preset %r" % (i + 1, name), "info")
            self._reply(200, {{"ok": True, "keyer": i + 1, "name": name, "names": names}})

        elif self.path == "/keyer/preset/recall":
            # Rappelle une sauvegarde sur un keyer : ses réglages, jamais son état d'antenne.
            i = _keyer_idx(body)
            name = _preset_name(body.get("name"))
            if i is None:
                self._reply(400, {{"error": "keyer hors plage (1..%d)" % N_KEYERS}}); return
            with state_lock:
                doc = state["keyer_presets"].get(name) if name else None
                if doc is None:
                    self._reply(404, {{"error": "sauvegarde inconnue : %s" % name}}); return
                k = state["keyers"][i]
                trial = dict(k)
                trial["mask"] = dict(k["mask"])
                skipped = _keyer_patch_lenient(trial, doc)
                k.update(trial)
                out = _keyer_settings(k)
            log("mixer: keyer %d recalled preset %r" % (i + 1, name), "info")
            self._reply(200, {{"ok": True, "keyer": i + 1, "name": name, "settings": out,
                              "skipped": skipped}})

        elif self.path == "/keyer/preset/delete":
            name = _preset_name(body.get("name"))
            with state_lock:
                if not name or name not in state["keyer_presets"]:
                    self._reply(404, {{"error": "sauvegarde inconnue : %s" % name}}); return
                state["keyer_presets"].pop(name)
                names = sorted(state["keyer_presets"])
            log("mixer: keyer preset %r deleted" % name, "info")
            self._reply(200, {{"ok": True, "names": names}})

        elif self.path == "/preset":
            # Met à jour le preset de transition (PATCH-like). Mode, direction et sens
            # s'appliquent au PROCHAIN /take ou /mix ; l'adoucissement agit aussi sur le volet
            # EN COURS (cf. _live_wipe_preset).
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
                    if "alternate" in w:
                        cur["alternate"] = bool(w.get("alternate"))
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
    target=lambda: ThreadingHTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
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
# Fermer un lecteur pendant qu'un fil d'échantillonnage lit sa tête = appel sur un handle libéré.
# Toute fermeture d'entrée et toute lecture hors du fil principal passent sous ce verrou (tenu
# quelques µs ; la fermeture est rare).
_reader_lock = threading.Lock()

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
            with _reader_lock:
                input_handles[idx] = None
                try: cur[0].close()
                except Exception: pass
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
        with _reader_lock:
            input_handles[idx] = None
            try: cur[0].close()
            except Exception: pass
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
    """RÉFÉRENCE du keyer DVE d'avant 0.23.14 — la boucle ne l'appelle plus (les keyers passent
    par _compose_keyers_whole / _sl_keyer_band) ; tests/verif_mixer_keyers.py vérifie que les deux
    chemins rendent ses octets, au keyer 1 DVE. Ne pas la modifier sans eux.
    Compose le keyer (resized via nearest-neighbor) sur bgd_yuv en YUV420.
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


# ─── MODE TRANCHE : état runtime + compose bandé ────────────────────────────────────────────
# Actif seulement si SLICE_ON — le chemin historique whole-frame reste STRICTEMENT intact.
_sl_backoff  = {{}}   # slot → [fi bloqué, timeouts consécutifs] ; 2 timeouts même grain → dormante
_sl_late_win = {{}}   # shm → deque(0/1) fenêtre ~10 s du taux de replis (garde-fou n°2)
_sl_flagged  = {{}}   # shm → pct : entrées signalées (hystérésis SLICE_LATE_PCT / _SL_CLEAR_PCT)
_sl_dbg      = None   # [entrées, valid0, attentes, replis, dormantes] de la dernière trame

_BLACK_Yi = 16 << (BIT_DEPTH - 8)     # noir Y (scalaire dtype pipeline)
_NEUT_Ci  = 1 << (BIT_DEPTH - 1)      # chroma neutre


_last_grid = 0
_SLOT_NS = int(round(1e9 / FPS))


# SAUT D'HORLOGE : un recul de plus de CLOCK_JUMP_SLOTS créneaux n'est pas une gigue (phc2sys
# recale par pas de quelques ms, soit 0 à 3 créneaux) mais un SAUT (horloge système recalée de
# plusieurs secondes, PTP perdu puis retrouvé). Vécu le 2026-10-25 sur dl360-1 : l'horloge a sauté
# ~149 s en avant puis est revenue ; le mélangeur a attendu le créneau « futur » — programme ARRÊTÉ —
# puis aurait retenu toute image d'un numéro inférieur au dernier publié : sortie figée jusqu'à ce
# que l'horloge réelle la rattrape. Au-delà du seuil, on se RECALE sur l'heure présente et on
# repart sur une NOUVELLE GÉNÉRATION des sorties (Writer.regenerate : flux purgé puis recréé),
# avec alarme. Rouvrir un writer sur le flux existant le RÉATTACHERAIT : la tête redescendrait
# et des grains déjà publiés seraient réécrits sous un autre contenu.
CLOCK_JUMP_SLOTS = 2 * FPS
_clock_jumps = 0
# PHASE DE LA GRILLE : `current_index()` change de créneau quand `now_tai()` est à MI-créneau
# (mesuré le 2026-10-25 : 10,00 ms dans un créneau de 20 ms, 50/50 — l'index d'un grain est le
# CENTRE de son intervalle). Dormir jusqu'à la frontière de `now_tai` réveillait donc le mélangeur
# 10 ms APRÈS le début du créneau, à chaque image et dans les deux modes : un second M/E en cascade
# ratait la première bande. La phase est CALIBRÉE (une frontière observée) au lieu d'être supposée.
_grid_phase = None


def _calibrate_grid_phase():
    """Position de `now_tai()` dans son créneau quand `current_index()` change (ns). ≤ 1 créneau."""
    g0 = int(bobimxl.current_index(FPS, 1))
    t_end = time.monotonic() + 2.5 / FPS
    while time.monotonic() < t_end:
        if int(bobimxl.current_index(FPS, 1)) != g0:
            return int(bobimxl.now_tai()) % _SLOT_NS
    return 0


def _wait_next_slot():
    """Dort jusqu'au créneau SUIVANT de la grille TAI et le rend. C'est l'horloge — et elle seule —
    qui cadence le mélangeur en mode tranche : plus d'entrée de référence dont la panne arrêterait
    ou décalerait le programme. Un tour plus long qu'un créneau saute au créneau courant ; un
    recul de plus de CLOCK_JUMP_SLOTS est un saut d'horloge → recalage immédiat."""
    global _last_grid, _clock_jumps, _grid_phase
    if _grid_phase is None:
        _grid_phase = _calibrate_grid_phase()
        log("mixer: TAI grid phase %.2f ms into the now_tai slot" % (_grid_phase / 1e6), "info")
    while True:
        g = int(bobimxl.current_index(FPS, 1))
        if g > _last_grid:
            _last_grid = g
            return g
        if _last_grid - g > CLOCK_JUMP_SLOTS:
            _clock_jumps += 1
            log("mixer: TAI clock went back %d slots (clock jump) — re-anchoring on the present"
                % (_last_grid - g), "warning")
            _last_grid = g
            return g
        left = _SLOT_NS - ((int(bobimxl.now_tai()) - _grid_phase) % _SLOT_NS)
        time.sleep(max(0.0002, left / 1e9))


# Mesure PAR ENTRÉE (vue « Entrées & délais », proposition de délai) : retard de la tête sur le
# créneau à la fin de la composition (0 = l'image du créneau courant a commencé d'arriver), et
# replis. Fenêtre de 2 s. Estimation conservatrice : une source qui arrive entre la fin de la
# composition et l'échéance de la ½ période est comptée comme demandant une image.
_in_lag = dict()        # slot → deque des retards (images)
_in_frames = dict()     # slot → trames où l'entrée a été lue
_in_late = dict()       # slot → replis (image visée absente à l'échéance)
_stamp = dict(published=0, held=0)
_stamp_last_out = -1


def _sl_bind(slot, grid, target, deadl, ts_in_per_slot, dbg):
    """Ouvre l'entrée `slot` pour l'image `target` (= créneau `grid` − délai de l'entrée) :
    sur la grille (tête entre grid − GROUP_DELAY_MAX − 2 et grid + 2) → suivie À target, attente
    de la 1ʳᵉ tranche jusqu'à `deadl` (monotonic) ; absente à l'échéance → dernière image
    complète, COMPTÉE en retard (avant 0.23.9 : 3 ms puis repli muet) ; HORS-GRILLE → sa tête.
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
        on_grid = -2 <= grid - h <= GROUP_DELAY_MAX + 2
        fi = target if on_grid else h
        got = None; static = False; bind_late = False
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
            _left = max(1, int((deadl - time.monotonic()) * 1e9))
            got = r.get_slice(fi, 1, timeout_ns=_left)
            if got is None:
                got = r.get_latest(); static = True
                bind_late = on_grid
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
                  late=bind_late,
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


def _even_down(v):
    return v - (v % 2)


def _sl_keyer_geom(ks):
    """Géométrie d'un keyer précalculée UNE fois par trame (latch) — rect, clipping, index
    nearest-neighbor (mêmes calculs que composite_keyer_yuv), puis MASQUE (fly key, tous types) :
    les fractions `mask.left/right/top/bottom` de l'image placée sont retirées, arrondies au pixel
    PAIR (chroma 4:2:x), et le rect se réduit d'autant — les index restent ceux de l'image entière,
    donc un masque ne déplace rien. None si invisible. Masque nul → octets identiques à avant."""
    dst_w = max(2, int(WIDTH * ks["scale"])); dst_h = max(2, int(HEIGHT * ks["scale"]))
    if dst_w % 2: dst_w -= 1
    if dst_h % 2: dst_h -= 1
    cx = ks["x"] * WIDTH; cy = ks["y"] * HEIGHT
    x0 = int(cx - dst_w / 2); y0 = int(cy - dst_h / 2)
    if x0 % 2: x0 -= 1
    if y0 % 2: y0 -= 1
    ix  = (np.arange(dst_w) * WIDTH  // dst_w)
    iy  = (np.arange(dst_h) * HEIGHT // dst_h)
    ix2 = (np.arange(dst_w // _CW) * UV_W // (dst_w // _CW))
    iy2 = (np.arange(dst_h // _CH) * UV_H // (dst_h // _CH))
    m = ks.get("mask") or dict()
    ml = _even_down(int(round(float(m.get("left", 0.0)) * dst_w)))
    mr = _even_down(int(round(float(m.get("right", 0.0)) * dst_w)))
    mt = _even_down(int(round(float(m.get("top", 0.0)) * dst_h)))
    mb = _even_down(int(round(float(m.get("bottom", 0.0)) * dst_h)))
    if ml or mr or mt or mb:
        if ml + mr >= dst_w or mt + mb >= dst_h:
            return None
        ix = ix[ml:dst_w - mr]; iy = iy[mt:dst_h - mb]
        ix2 = ix2[ml // _CW:(dst_w - mr) // _CW]; iy2 = iy2[mt // _CH:(dst_h - mb) // _CH]
        x0 += ml; y0 += mt
        dst_w -= ml + mr; dst_h -= mt + mb
    out_x0 = max(0, x0); out_y0 = max(0, y0)
    out_x1 = min(WIDTH, x0 + dst_w); out_y1 = min(HEIGHT, y0 + dst_h)
    if out_x1 <= out_x0 or out_y1 <= out_y0:
        return None
    src_x_off = out_x0 - x0
    vis_w = out_x1 - out_x0
    ixv = ix[src_x_off:src_x_off + vis_w]
    ix2v = ix2[src_x_off // _CW:(src_x_off + vis_w) // _CW]
    return dict(x0=x0, y0=y0, dst_w=dst_w, dst_h=dst_h,
                out_x0=out_x0, out_y0=out_y0, out_x1=out_x1, out_y1=out_y1,
                iy=iy, iy2=iy2,
                # colonnes VISIBLES précalculées (crop gauche/droite intégré à l'index)
                ixv=ixv, ix2v=ix2v,
                # colonnes CONSÉCUTIVES (échelle 1, cas courant d'un linear/luma plein cadre) :
                # la lecture du source devient une simple vue, sans copie (cf. _gather)
                xcon=_consecutive(ixv), x2con=_consecutive(ix2v),
                # bordure : propre au DVE (un keyer à découpe n'en a pas)
                bw=(int(ks["border_width"]) if ks.get("type", "dve") == "dve" else 0))


def _consecutive(idx):
    return bool(idx.size) and int(idx[-1]) - int(idx[0]) == idx.size - 1


def _gather(src, rows, cols, ccon):
    """Rectangle source rows × cols (index nearest, croissants). VUE sans copie quand lignes ET
    colonnes sont consécutives (keyer plein cadre) ; sinon une passe mvk_place_into, repli numpy
    (mêmes octets : c'est un simple prélèvement)."""
    if ccon and _consecutive(rows):
        c0 = int(cols[0])
        return src[int(rows[0]):int(rows[-1]) + 1, c0:c0 + cols.size]
    if _MVK:
        try:
            out = np.empty((rows.size, cols.size), dtype=src.dtype)
            if bobimxl.mvk_place_into(out, src, rows.astype(np.int32), col_idx=cols.astype(np.int32)):
                return out
        except Exception:
            pass
    return src[rows][:, cols]


_LUT_V = np.arange(1 << BIT_DEPTH, dtype=np.float32)
# Table IDENTITÉ (8 bits) : un linear sans réglage de découpe, plein alpha d'antenne, prend la valeur
# du key telle quelle pour alpha — aucune lecture de table par pixel (le cas courant : le flux _key
# de stills incrusté tel quel). Objet unique : reconnu par `is`.
_LUT_IDENT = np.arange(256, dtype=np.uint8)


def _key_lut(ks, ka):
    """Table alpha[valeur du key] en 8 bits (0 = transparent, 255 = opaque) d'un keyer à DÉCOUPE,
    alpha d'antenne `ka` compris ; _LUT_IDENT quand elle est l'identité ; None pour un dve (alpha
    uniforme). Calculée UNE fois par trame (latch) : la découpe d'une image se réduit à une lecture
    de table par pixel, puis au mélange entier du noyau C. Cf. l'en-tête KEYERS AMONT."""
    t = ks.get("type", "dve")
    if t == "chroma":
        return _chroma_cut(ks, ka)
    if t == "pattern":
        return _pattern_cut(ks, ka)
    if t == "linear":
        k = _LUT_V / float(255 * _SCALE)
    elif t == "luma":
        k = (_LUT_V - 16.0 * _SCALE) / (219.0 * _SCALE)
    else:
        return None
    k = np.clip(k, 0.0, 1.0)
    if ks.get("invert"):
        k = 1.0 - k
    clip = max(0.0, min(0.999, float(ks.get("clip", 0.0))))
    gain = float(ks.get("gain", 1.0))
    a = np.clip((k - clip) / (1.0 - clip) * gain, 0.0, 1.0)
    lut = np.rint(a * float(ka) * 255.0).astype(np.uint8)
    if not _DEEP and np.array_equal(lut, _LUT_IDENT):
        return _LUT_IDENT
    return lut


# ─── Keyer CHROMA (0.23.22) ─────────────────────────────────────────────────────────────────────
# L'alpha ne dépend que de la chroma (U, V) du fill : UNE table de 256 × 256 (index U8 · 256 + V8, chroma
# ramenée à 8 bits), calculée UNE fois par réglage ; l'alpha d'antenne s'applique à la table (65 536
# entrées), jamais par pixel. 8 bits : la table est EMPAQUETÉE — alpha | U corrigé << 8 | V corrigé << 16
# en uint32 — et UNE lecture par échantillon chroma donne l'alpha ET le fill débarrassé du débordement.
# Mesuré 1080p 4:2:2 (un cœur, noyaux mvk) : trois lectures séparées (alpha, U, V) coûtaient ~11 ms par
# image, la table empaquetée ~6 ms. 10/12 bits : index sur les 8 bits hauts, alpha + tables d'ÉCART
# int16 ajoutées à l'échantillon (plus lent : trois lectures).
_UV8 = (np.arange(256, dtype=np.float32) - 128.0) / 224.0    # chroma 8 bits légale → ±0,5
_chroma_cache = dict()
_chroma_warned = [False]


def _hex_rgb01(hexstr):
    h = _norm_hex(hexstr) or CHROMA_KEY_DEFAULT
    return int(h[1:3], 16) / 255.0, int(h[3:5], 16) / 255.0, int(h[5:7], 16) / 255.0


def _key_uv01(hexstr):
    """#RRGGBB → (U, V) ±0,5 dans la matrice déclarée (cf. KEY_COLORIMETRY)."""
    r, g, b = _hex_rgb01(hexstr)
    kr, kb = _KEY_KRKB[KEY_COLORIMETRY]
    y = kr * r + (1.0 - kr - kb) * g + kb * b
    return (b - y) / (2.0 * (1.0 - kb)), (r - y) / (2.0 * (1.0 - kr))


def _chroma_base(ks):
    """(alpha float32[65536], écart U, écart V) d'un réglage, mis en cache. Écarts en float32 (unités
    de l'échantillon), None sans débordement à retirer. Index de table = U8 · 256 + V8."""
    key = (_norm_hex(ks.get("chroma_color")) or CHROMA_KEY_DEFAULT, float(ks.get("chroma_tol", 0.2)),
           float(ks.get("chroma_soft", 0.15)), float(ks.get("chroma_spill", 0.5)), bool(ks.get("invert")))
    got = _chroma_cache.get(key)
    if got is not None:
        return got
    if KEY_COLORIMETRY_ASSUMED and not _chroma_warned[0]:
        _chroma_warned[0] = True
        log("mixer: chroma key colour converted with BT.709 (colorimetry not declared: %r)"
            % _KEY_COL_RAW, "warning")
    color, tol, soft, spill, inv = key
    ku, kv = _key_uv01(color)
    U = _UV8[:, None]; V = _UV8[None, :]
    du = U - ku; dv = V - kv
    dist = np.sqrt(du * du + dv * dv) / 0.5            # 0..~1,4 : ±0,5 = pleine amplitude
    a = np.clip((dist - tol) / max(soft, 1.0 / 255.0), 0.0, 1.0)
    if inv:
        a = 1.0 - a
    a = np.ascontiguousarray(a, dtype=np.float32).ravel()
    tu = tv = None
    kn = float(np.hypot(ku, kv))
    if spill > 0.0 and kn > 1e-6:
        eu, ev = ku / kn, kv / kn
        p = np.maximum(U * eu + V * ev, 0.0) * spill       # part du fill DANS la direction de la clé
        tu = np.ascontiguousarray(np.broadcast_to(np.rint(-p * eu * 224.0 * _SCALE), (256, 256)),
                                  dtype=np.float32).ravel()
        tv = np.ascontiguousarray(np.broadcast_to(np.rint(-p * ev * 224.0 * _SCALE), (256, 256)),
                                  dtype=np.float32).ravel()
    if len(_chroma_cache) > 16:
        _chroma_cache.clear()
    got = (a, tu, tv)
    _chroma_cache[key] = got
    return got


_CODE_U = np.repeat(np.arange(256, dtype=np.uint32), 256)     # U8 de chaque entrée de table
_CODE_V = np.tile(np.arange(256, dtype=np.uint32), 256)       # V8 de chaque entrée de table


# Dernière table construite : (réglage, alpha d'antenne quantifié) → table. Hors transition, la même
# sert à chaque image (≈ 0,5 ms épargnée par image en 1080p).
_chroma_cut_last = [None, None]


def _chroma_cut(ks, ka):
    base = _chroma_base(ks)
    q = int(round(float(ka) * 255.0))
    last = _chroma_cut_last[0]
    if last is not None and last[0] is base and last[1] == q:
        return _chroma_cut_last[1]
    ct = _chroma_cut_build(base, q)
    _chroma_cut_last[0] = (base, q)
    _chroma_cut_last[1] = ct
    return ct


def _chroma_cut_build(base, q):
    a, tu, tv = base
    a8 = np.rint(a * float(q)).astype(np.uint32)
    if _DEEP:
        return dict(kind="chroma", alpha=a8.astype(np.uint8),
                    du=None if tu is None else tu.astype(np.int16),
                    dv=None if tv is None else tv.astype(np.int16))
    cu = _CODE_U if tu is None else np.clip(_CODE_U + tu, 0, 255).astype(np.uint32)
    cv = _CODE_V if tv is None else np.clip(_CODE_V + tv, 0, 255).astype(np.uint32)
    return dict(kind="chroma", packed=(a8 | (cu << 8) | (cv << 16)))


def _uv_index(U, V):
    """Index (U8 << 8) | V8 dans la table chroma — chroma ramenée à 8 bits en 10/12 bits."""
    if _DEEP:
        sh = BIT_DEPTH - 8
        u = U >> sh
        v = V >> sh
    else:
        u = U.astype(np.uint16)
        v = V
    u <<= 8
    u |= v
    return u


def _gather_c(src, rows, cols, ccon):
    """_gather rendu CONTIGU (alpha et sources du noyau de mélange), `src` éventuellement stridé :
    une seule copie quand lignes et colonnes sont consécutives."""
    if ccon and _consecutive(rows):
        c0 = int(cols[0])
        return np.ascontiguousarray(src[int(rows[0]):int(rows[-1]) + 1, c0:c0 + cols.size])
    return _gather(np.ascontiguousarray(src), rows, cols, False)


def _sl_chroma_band(py, pu, pv, F, g, ct, a, b):
    """Keyer CHROMA sur la bande [a,b) du PGM. L'alpha est lu dans la table `ct` sur la chroma du
    fill, dans le rectangle source qui couvre la bande (index nearest de la géométrie fly/masque) ;
    luma : l'échantillon chroma qui porte le pixel ; chroma : le sien. Fill corrigé du débordement."""
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    ca2 = max(a // _CH, g["out_y0"] // _CH); cb2 = min(b // _CH, g["out_y1"] // _CH)
    if a2 >= b2 and ca2 >= cb2:
        return
    rl = src_c = None
    rr = []; cc = []
    if a2 < b2:
        sr = g["iy"][a2 - g["y0"]:b2 - g["y0"]]
        rl = sr // _CH; cl = g["ixv"] // _CW
        rr += [int(rl[0]), int(rl[-1])]; cc += [int(cl[0]), int(cl[-1])]
    if ca2 < cb2:
        src_c = g["iy2"][ca2 - g["y0"] // _CH:cb2 - g["y0"] // _CH]
        rr += [int(src_c[0]), int(src_c[-1])]
        cc += [int(g["ix2v"][0]), int(g["ix2v"][-1])]
    R0 = min(rr); R1 = max(rr) + 1; C0 = min(cc); C1 = max(cc) + 1
    idx = _uv_index(F["u"][R0:R1, C0:C1], F["v"][R0:R1, C0:C1])
    if _DEEP:
        A = np.take(ct["alpha"], idx)
        CU = CV = None
    else:
        B = np.take(ct["packed"], idx).view(np.uint8).reshape(idx.shape + (4,))   # x86 : petit-boutiste
        A = np.ascontiguousarray(B[..., 0])
        CU = B[..., 1]; CV = B[..., 2]
    x0o = g["out_x0"]; x1o = g["out_x1"]
    if rl is not None:
        am = _gather(A, rl - R0, cl - C0, False)
        _blend8(py[a2:b2, x0o:x1o], _gather(F["y"], sr, g["ixv"], g["xcon"]), am)
    if src_c is None:
        return
    rc = src_c - R0; ccol = g["ix2v"] - C0
    mc = _gather_c(A, rc, ccol, g["x2con"])
    cx0 = x0o // _CW; cx1 = x1o // _CW
    if CU is not None:
        for P, C in ((pu, CU), (pv, CV)):
            _blend8(P[ca2:cb2, cx0:cx1], _gather_c(C, rc, ccol, g["x2con"]), mc)
        return
    ic = _gather(idx, rc, ccol, g["x2con"]) if ct["du"] is not None else None
    for P, S, d in ((pu, F["u"], ct["du"]), (pv, F["v"], ct["dv"])):
        fs = _gather(S, src_c, g["ix2v"], g["x2con"])
        if d is not None:
            fs = np.clip(fs.astype(np.int32) + np.take(d, ic), 0, _MAXV).astype(_NP_DT)
        _blend8(P[ca2:cb2, cx0:cx1], fs, mc)


# ─── Keyer MOTIF (0.23.22) ──────────────────────────────────────────────────────────────────────
# Alpha = une FORME en coordonnées ÉCRAN (le motif ne bouge pas avec le fly : il découpe ce que le
# fill y montre), carte uint8 plein cadre calculée UNE fois par réglage. Distance normalisée d au
# centre (cx, cy) : barres = |dx| / (W/2), |dy| / (H/2), |dx + dy| / ((W+H)/2) ; iris = rayon / demi-
# diagonale ; boîte = max des deux barres ; cœur = profondeur du volet Cœur, translatée. Rayon
# R = taille × (1 + douceur) : taille 1 couvre l'écran (motif centré), taille 0 n'est rien, la douceur
# est une rampe INTÉRIEURE (alpha = clamp((R − d) / douceur, 0, 1)). Alpha d'antenne : table de 256
# appliquée par bande, sur le seul rectangle visible.
_pattern_cache = dict()


def _pattern_profile(d, R, sw):
    return np.rint(np.clip((R - d) / sw, 0.0, 1.0) * 255.0).astype(np.uint8)


def _pattern_map(ks):
    shape = ks.get("pattern", "iris")
    if shape not in PATTERN_SHAPES:
        shape = "iris"
    if shape == "heart":
        _kick_heart_precompute()
        if not _heart_ready.is_set() or _heart_depth is None:
            shape = "iris"           # Cœur pas encore précalculé : iris le temps du calcul
    key = (shape, float(ks.get("pattern_size", 0.5)), float(ks.get("pattern_x", 0.5)),
           float(ks.get("pattern_y", 0.5)), float(ks.get("pattern_soft", 0.1)), bool(ks.get("invert")))
    got = _pattern_cache.get(key)
    if got is not None:
        return got
    _shape, size, px, py_, soft, inv = key
    cx = px * WIDTH; cy = py_ * HEIGHT
    xs = np.arange(WIDTH, dtype=np.float32) + 0.5 - cx
    ys = np.arange(HEIGHT, dtype=np.float32) + 0.5 - cy
    hx = WIDTH / 2.0; hy = HEIGHT / 2.0
    if shape == "horizontal":            # barre verticale (le volet horizontal, figé)
        sw = max(soft, 1.0 / hx); R = size * (1.0 + sw)
        m = np.broadcast_to(_pattern_profile(np.abs(xs) / hx, R, sw)[None, :], (HEIGHT, WIDTH))
    elif shape == "vertical":            # barre horizontale
        sw = max(soft, 1.0 / hy); R = size * (1.0 + sw)
        m = np.repeat(_pattern_profile(np.abs(ys) / hy, R, sw)[:, None], WIDTH, axis=1)
    elif shape == "box":
        sw = max(soft, 1.0 / min(hx, hy)); R = size * (1.0 + sw)
        m = np.minimum.outer(_pattern_profile(np.abs(ys) / hy, R, sw),
                             _pattern_profile(np.abs(xs) / hx, R, sw))
    elif shape == "diagonal":
        hd = (WIDTH + HEIGHT) / 2.0
        sw = max(soft, 1.0 / hd); R = size * (1.0 + sw)
        s_ = np.arange(WIDTH + HEIGHT - 1, dtype=np.float32) + 1.0 - cx - cy
        prof = _pattern_profile(np.abs(s_) / hd, R, sw)
        m = prof[np.arange(HEIGHT)[:, None] + np.arange(WIDTH)[None, :]]
    elif shape == "heart":
        sw = max(soft, 0.002); R = size * (1.0 + sw)
        rows = np.clip(np.arange(HEIGHT) + int(round(hy - cy)), 0, HEIGHT - 1)
        cols = np.clip(np.arange(WIDTH) + int(round(hx - cx)), 0, WIDTH - 1)
        d = _heart_depth[rows][:, cols] * 2.2           # profondeur → ~demi-diagonales (cf. _compute_heart_depth)
        m = _pattern_profile(d, R, sw)
    else:                                # iris
        hd = float(np.hypot(hx, hy))
        sw = max(soft, 1.0 / hd); R = size * (1.0 + sw)
        d = np.sqrt(ys[:, None] * ys[:, None] + xs[None, :] * xs[None, :]) / hd
        m = _pattern_profile(d, R, sw)
    if inv:
        m = 255 - m
    if len(_pattern_cache) > 8:
        _pattern_cache.clear()
    _pattern_cache[key] = m
    return m


def _pattern_cut(ks, ka):
    q = int(round(float(ka) * 255.0))
    kt = None if q >= 255 else np.rint(np.arange(256, dtype=np.float32) * (q / 255.0)).astype(np.uint8)
    return dict(kind="pattern", map=_pattern_map(ks), ka=kt)


def _sl_pattern_band(py, pu, pv, F, g, pc, a, b):
    """Keyer MOTIF sur la bande [a,b) : alpha = la carte du motif sous le rectangle visible (vue, sans
    copie), alpha d'antenne par table ; chroma : alpha au pixel luma PAIR (convention du linear)."""
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    M = pc["map"]; kt = pc["ka"]
    x0o = g["out_x0"]; x1o = g["out_x1"]
    if a2 < b2:
        am = M[a2:b2, x0o:x1o]
        if kt is not None:
            am = np.take(kt, am)
        sr = g["iy"][a2 - g["y0"]:b2 - g["y0"]]
        _blend8(py[a2:b2, x0o:x1o], _gather(F["y"], sr, g["ixv"], g["xcon"]), am)
    ca2 = max(a // _CH, g["out_y0"] // _CH); cb2 = min(b // _CH, g["out_y1"] // _CH)
    if ca2 >= cb2:
        return
    src_c = g["iy2"][ca2 - g["y0"] // _CH:cb2 - g["y0"] // _CH]
    cx0 = x0o // _CW; cx1 = x1o // _CW
    mc = M[ca2 * _CH:cb2 * _CH:_CH, x0o:x1o:_CW][:, :cx1 - cx0]
    mc = np.ascontiguousarray(mc if kt is None else np.take(kt, mc))
    for P, S in ((pu, F["u"]), (pv, F["v"])):
        _blend8(P[ca2:cb2, cx0:cx1], _gather(S, src_c, g["ix2v"], g["x2con"]), mc)


def _blend8(dst, src, am):
    """dst ← (dst·(255−α) + src·α) // 255, IN-PLACE, α uint8 par pixel : noyau C mvk_blend_into,
    repli numpy aux MÊMES octets (formule entière du multiview)."""
    if _MVK:
        try:
            if bobimxl.mvk_blend_into(dst, src, am):
                return
        except Exception:
            pass
    a32 = am.astype(np.uint32)
    dst[...] = ((dst.astype(np.uint32) * (255 - a32) + src.astype(np.uint32) * a32) // 255).astype(_NP_DT)


def _sl_keyer_alpha_band(py, pu, pv, F, KS, g, lut, a, b):
    """Keyer à DÉCOUPE (linear, luma) sur la bande [a,b) du PGM : alpha PAR PIXEL = lut[key], le
    key lu aux MÊMES index que le fill (même géométrie fly/masque, et le même grain — cf. le bind
    des paires dans _slice_frame). Luma : mélange per-pixel ; chroma : alpha prélevé au pixel luma
    PAIR de chaque échantillon (convention de l'habillage, cf. composite_overlay)."""
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    if a2 >= b2:
        return
    x0o = g["out_x0"]; x1o = g["out_x1"]
    sr = g["iy"][a2 - g["y0"]:b2 - g["y0"]]
    ky = _gather(KS["y"], sr, g["ixv"], g["xcon"])
    am = ky if lut is _LUT_IDENT else np.take(lut, ky, mode="clip")    # (lignes, colonnes) uint8
    fy = ky if KS is F else _gather(F["y"], sr, g["ixv"], g["xcon"])
    _blend8(py[a2:b2, x0o:x1o], fy, am)
    ca2 = max(a // _CH, g["out_y0"] // _CH); cb2 = min(b // _CH, g["out_y1"] // _CH)
    if ca2 >= cb2:
        return
    src_c = g["iy2"][ca2 - g["y0"] // _CH:cb2 - g["y0"] // _CH]
    cx0 = x0o // _CW; cx1 = x1o // _CW
    # alpha de chaque échantillon chroma : la ligne luma de sa rangée, la colonne luma paire
    mc = np.ascontiguousarray(am[ca2 * _CH - a2::_CH, ::_CW][:cb2 - ca2, :cx1 - cx0])
    for P, S in ((pu, F["u"]), (pv, F["v"])):
        _blend8(P[ca2:cb2, cx0:cx1], _gather(S, src_c, g["ix2v"], g["x2con"]), mc)


def _planes_of(buf):
    pl = np.frombuffer(buf, dtype=np.uint8)
    return dict(y=pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                v=pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))


def _read_pair(rf, rk):
    """Chemin image entière : (fill, key, ts) LUS AU MÊME INDEX — le key est demandé à l'index du
    fill (attente bornée à 2 ms), sinon sa dernière image. Lire chacun « au plus récent » pouvait
    apparier le fill N avec le key N±1 : une découpe décalée d'une image sur tout ce qui bouge.
    Source entrelacée ou format ≠ programme : lecture simple, sans appariement."""
    try:
        got = rf.get_latest()
    except Exception:
        got = None
    if got is None or len(got[2]) != V_FRAME_SIZE:
        f, ts = lire_frame_yuv(rf)
        k, _ts = lire_frame_yuv(rk)
        return f, k, ts
    fill = bytes(got[2])
    try:
        ts = rf.last_write_time()
    except Exception:
        ts = None
    if rk is rf:
        return fill, fill, ts
    try:
        gk = rk.get(int(got[0]), timeout_ns=2_000_000)
    except Exception:
        gk = None
    if gk is not None and len(gk[2]) == V_FRAME_SIZE:
        return fill, bytes(gk[2]), ts
    k, _ts = lire_frame_yuv(rk)
    return fill, k, ts


def _sl_key_one(kl, y, u, v, a, b, deadl, dbg):
    """Pose UN keyer sur la bande [a, b) des plans (y, u, v) — attend d'abord les lignes du fill (et
    du key d'un linear). Rend les ns attendues. Fill ou key absent : rien à découper."""
    F = kl["F"]; KK = kl["KK"]
    if F is None or KK is None:
        return 0
    w = 0
    nr = _sl_keyer_need(kl["g"], a, b)
    lut = kl["lut"]
    if nr >= 0 and isinstance(lut, dict) and lut["kind"] == "chroma":
        nr = (nr // _CH + 1) * _CH - 1      # chroma : l'alpha luma vient de l'échantillon chroma
    if nr >= 0:
        w += _sl_wait(F, nr, deadl, dbg)
        if KK is not F:
            w += _sl_wait(KK, nr, deadl, dbg)
    base = None
    if kl.get("wm") is not None:
        ca = a // _CH; cb = b // _CH
        base = (y[a:b].copy(), u[ca:cb].copy(), v[ca:cb].copy())
    _sl_keyer_band(y, u, v, F, kl["g"], kl["ka"], a, b, KK, kl["lut"])
    if base is not None:
        _sl_wipe_band(y, u, v, base, kl["wm"], a, b)
    return w


def _sl_wipe_band(py, pu, pv, base, m, a, b):
    """AUTO de keyer en VOLET : bande [a,b) = base (avant le keyer) · (1 − m) + keyée · m, avec m le
    masque plein trame du volet (_wipe_alpha_map). Même noyau que l'overlay (mvk_mixmap_into),
    repli numpy à l'identique."""
    ca = a // _CH; cb = b // _CH
    by, bu, bv = base
    mf = m[a:b]
    ky = py[a:b].copy(); ku = pu[ca:cb].copy(); kv = pv[ca:cb].copy()
    if _MVK:
        try:
            mh = mf[::_CH]
            if (bobimxl.mvk_mixmap_into(py[a:b], by, ky, mf) and
                    bobimxl.mvk_mixmap_into(pu[ca:cb], bu, ku, mh, m_colstep=_CW) and
                    bobimxl.mvk_mixmap_into(pv[ca:cb], bv, kv, mh, m_colstep=_CW)):
                return
        except Exception:
            pass
    mc = mf[::_CH, ::_CW]
    py[a:b] = (by.astype(np.float32) * (1.0 - mf) + ky.astype(np.float32) * mf).astype(_NP_DT)
    pu[ca:cb] = (bu.astype(np.float32) * (1.0 - mc) + ku.astype(np.float32) * mc).astype(_NP_DT)
    pv[ca:cb] = (bv.astype(np.float32) * (1.0 - mc) + kv.astype(np.float32) * mc).astype(_NP_DT)


def _compose_keyers_whole(yuv, keyers, ts_in_per_slot):
    """Chemin image entière : les keyers visibles, dans l'ordre (1 dessous), sur une COPIE du
    fond — mêmes fonctions de bande que le mode tranche, appliquées à la trame [0, HEIGHT)."""
    buf = np.frombuffer(yuv, dtype=np.uint8).copy()
    P = _planes_of(buf)
    for ks in keyers:
        ka = float(ks.get("alpha", 0.0))
        if ka <= 0.001:
            continue
        g = _sl_keyer_geom(ks)
        if g is None:
            continue
        fi = int(ks["input"])
        if ks.get("type") == "linear":
            rf = ensure_input(fi)
            rk = ensure_input(int(ks.get("key_input", fi)))
            if rf is None or rk is None:
                continue
            f, k, ts = _read_pair(rf, rk)
        else:
            f, ts = lire_frame_yuv(ensure_input(fi))
            k = f
        if ts:
            ts_in_per_slot[fi] = ts
        if not f or not k:
            continue
        F = _planes_of(f)
        KK = F if k is f else _planes_of(k)
        wmk = ks.get("wmask")
        base = (P["y"].copy(), P["u"].copy(), P["v"].copy()) if wmk is not None else None
        _sl_keyer_band(P["y"], P["u"], P["v"], F, g, ka, 0, HEIGHT, KK, _key_lut(ks, ka))
        if base is not None:
            _sl_wipe_band(P["y"], P["u"], P["v"], base, wmk, 0, HEIGHT)
    return buf.tobytes()

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


def _sl_keyer_band(py, pu, pv, K, g, ka, a, b, KK=None, lut=None):
    """Compose un keyer sur la bande [a,b) du PGM. DVE (`lut` None) — mêmes maths que
    composite_keyer_yuv (nearest + blend float32), en ne lisant du SOURCE que les lignes requises
    par la bande (gather par lignes → équivalence octet avec le fancy-index plein trame tranché).
    Découpe (`lut` fourni : linear, luma) → _sl_keyer_alpha_band, key `KK` (le fill si None)."""
    if isinstance(lut, dict):
        if lut["kind"] == "chroma":
            _sl_chroma_band(py, pu, pv, K, g, lut, a, b)
        else:
            _sl_pattern_band(py, pu, pv, K, g, lut, a, b)
        return
    if lut is not None:
        _sl_keyer_alpha_band(py, pu, pv, K, (K if KK is None else KK), g, lut, a, b)
        return
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


def _slice_frame(grid, _unused, bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, bgd_preset,
                 keyer_alpha, keyer_state, ovl, pgm, pvw, ts_in_per_slot, keyers=None,
                 pvw_keyers=None):
    """MODE TRANCHE — compose et PUBLIE la trame BANDE PAR BANDE aux 3 sorties PGM/CLEAN/PVW.

    PVW (0.23.29) : `pvw` = entrée du FOND du PVW (cf. _pvw_future : entrée PVW ou PGM selon la
    prochaine transition) ; `pvw_keyers` = keyers du PVW (même forme que `keyers`). Aucun keyer
    visible au PVW (ou None) → copie de l'entrée, comme avant 0.23.29 : zéro surcoût. Sinon les
    keyers visibles sont posés bande par bande avec _sl_key_one — même code, mêmes attentes bornées
    par le budget de trame, même repli sur le dernier grain qu'au PGM ; leurs entrées sont ouvertes
    même hors antenne. Jamais d'habillage au PVW.

    KEYERS (0.23.14) : `keyers` = instantané de TOUS les keyers (cf. _latch_keyers, alpha compris),
    composés dans l'ordre sur le PGM. Absent → appel d'avant 0.23.14 : le seul keyer 1, décrit par
    (`keyer_alpha`, `keyer_state`). Un keyer linear lit son KEY AU GRAIN EXACT de son fill, quel
    que soit le groupe de délai du key : la paire reste appariée.

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
    global _sl_dbg, _stamp_last_out
    dbg = [0, -1, 0, 0, 0]   # [entrées, valid0, attentes, replis, dormantes]
    with state_lock:
        _mode = state["latency_mode"]
        _delays = state["delays"]
    # Échéance de la 1ʳᵉ tranche, COMMUNE à toutes les entrées : une demi-période après le réveil
    # sur le créneau. En délai ≥ 1 l'image visée est déjà là ; le budget ne sert qu'aux retardataires.
    bind_deadl = time.monotonic() + 0.5 / FPS
    # ─ LATCH IMMÉDIAT (avant toute attente/bind) : mode, masque wipe (précalculé UNE fois par
    #   (alpha, préréglage) puis TRANCHÉ par bande) et géométrie keyer sont figés ICI.
    if keyers is None:
        _k1 = _keyer_default()
        _k1.update(keyer_state)
        _k1["alpha"] = keyer_alpha
        keyers = [_k1]
    def _visible(kset):
        """Keyers VISIBLES d'une pile : latch, géométrie, table d'alpha, entrées."""
        res = []
        for ks in kset or ():
            ka = float(ks.get("alpha", 0.0))
            if ka <= 0.001:
                continue
            g = _sl_keyer_geom(ks)
            if g is None:
                continue
            lin = ks.get("type") == "linear"
            res.append(dict(g=g, ka=ka, lut=_key_lut(ks, ka), fill=int(ks["input"]),
                            key=(int(ks.get("key_input", ks["input"])) if lin else None),
                            clean=bool(ks.get("clean")), wm=ks.get("wmask")))
        return res
    kls = _visible(keyers)          # PGM / CLEAN
    kls_pv = _visible(pvw_keyers)   # PVW (vide → copie de l'entrée, zéro surcoût)
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
    for kl in kls + kls_pv:
        slots.append(kl["fill"])
    slots.append(pvw)
    srcs = dict()
    for s in slots:
        if s not in srcs:
            _d = _read_delay(s, _mode, _delays)
            # Entrée CONNUE pour arriver trop tard (délai nécessaire mesuré > son délai) : on ne
            # l'attend pas — elle passe avec sa dernière image, comptée. Sans ça, une seule source
            # lente (un split en délai fixe sur le PVW d'un mélangeur « au plus rapide ») brûlait
            # la demi-période à CHAQUE image et retardait toute la sortie : mesuré le 2026-10-25,
            # une cascade de deux M/E rapides ratait alors 151 images sur 198. La mesure continue :
            # une source redevenue rapide est de nouveau attendue.
            _need = _lag_of(s)
            _dl = time.monotonic() if (_need is not None and _need > _d) else bind_deadl
            srcs[s] = _sl_bind(s, grid, grid - _d, _dl, ts_in_per_slot, dbg)
    # ─ KEY d'un keyer linear : lu au GRAIN RÉELLEMENT LU de son fill (`fi`), jamais à « créneau −
    #   son propre délai » : un fill et son key rangés dans deux groupes de délai différents, ou un
    #   fill retombé sur sa dernière image, seraient sinon décalés d'une image ou plus — découpe
    #   fausse sur tout ce qui bouge. Un slot déjà ouvert au même grain est réutilisé.
    keysrcs = dict()
    for kl in kls + kls_pv:
        F = srcs.get(kl["fill"])
        kl["F"] = F
        if kl["key"] is None or F is None:
            kl["KK"] = F            # dve, luma : le « key » est le fill lui-même
            continue
        want = F["fi"]
        hit = srcs.get(kl["key"])
        if hit is not None and hit["fi"] == want:
            kl["KK"] = hit
            continue
        ck = (kl["key"], want)
        if ck not in keysrcs:
            keysrcs[ck] = _sl_bind(kl["key"], grid, want, bind_deadl, ts_in_per_slot, dbg)
        kl["KK"] = keysrcs[ck]
    dbg[0] = (sum(1 for x in srcs.values() if x is not None)
              + sum(1 for x in keysrcs.values() if x is not None))
    # ─ Couche BGD latchée : blend si les DEUX sources présentes, sinon copie (to ou from),
    #   sinon noir — même repli que l'historique (f_to or f_from or empty), décidé UNE fois.
    if bgd_active:
        A = srcs.get(bgd_from_idx); B = srcs.get(bgd_to_idx)
        blend_on = A is not None and B is not None
        S = B if B is not None else A
    else:
        A = B = None; blend_on = False
        S = srcs.get(pgm)
    V = srcs.get(pvw)
    # ─ 3 grains de sortie au numéro du GROUPE DE RÉFÉRENCE (créneau − son délai) : c'est l'image
    #   que le PGM contient pour toute source de ce groupe, et il ne dépend NI de l'entrée à
    #   l'antenne NI d'aucune entrée — une coupe ne fait ni saut ni retour. Avant 0.23.9 : index de
    #   grille du créneau, soit une image d'avance sur le contenu (une ré-entrée M/E était décalée).
    #   Numéro pas plus récent que le dernier publié (deux tours dans le même créneau, délai de
    #   référence augmenté) → trame RETENUE : open_grain(index=…) rend un brouillon, rien n'est
    #   publié. Jamais `src_index` ici : il rendrait « compteur + 1 », un cran d'avance permanent.
    idx_out = grid - _ref_delay(_mode, _delays)
    if _stamp_last_out - idx_out > CLOCK_JUMP_SLOTS:
        # Saut d'horloge (cf. CLOCK_JUMP_SLOTS) : le dernier numéro publié est loin dans le futur ;
        # retenir attendrait que l'horloge le rattrape. On repart sur une sortie neuve.
        _reopen_outputs()
        _stamp["reanchored"] = _stamp.get("reanchored", 0) + 1
        _stamp_last_out = -1
    if idx_out <= _stamp_last_out:
        _stamp["held"] += 1
    else:
        idx_out = out_pgm.next_index(src_index=idx_out)
        _stamp["published"] += 1
        _stamp_last_out = idx_out
    _i, gi_p, vw_p = out_pgm.open_grain(index=idx_out)
    _i, gi_c, vw_c = out_clean.open_grain(index=idx_out)
    _i, gi_v, vw_v = out_pvw.open_grain(index=idx_out)
    # Les trois sorties partagent idx_out : une seule observation suffit, sur le PROGRAMME.
    # `_sl_bind` renvoie r (reader) et fi (index réellement lu) par entrée — y compris sur son
    # chemin dégradé hors-grille, où fi est la tête de l'entrée et non idx_ref.
    delai_etage.observe(out_pgm, idx_out,
                        [(q["r"], q["fi"]) for q in list(srcs.values()) + list(keysrcs.values())
                         if q and q.get("r") is not None and q.get("fi") is not None],
                        propage=True)
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
    kls_clean = [kl for kl in kls if kl["clean"]]
    clean_prefix = all(kl["clean"] for kl in kls[:len(kls_clean)])
    try:
        for k in range(_SL_NB):
            a = k * SLICE_LINES; b = a + SLICE_LINES
            ca = a // _CH; cb = b // _CH
            # ─ CLEAN = couche BGD, puis les keyers RÉGLÉS sur le clean (jamais l'overlay HTML) ─
            if blend_on:
                wait_ns += _sl_wait(A, b - 1, deadl, dbg)
                wait_ns += _sl_wait(B, b - 1, deadl, dbg)
                _sl_blend_band(cl_y, cl_u, cl_v, A, B, bgd_alpha, bgd_mode, wm, a, b)
            elif S is not None:
                wait_ns += _sl_wait(S, b - 1, deadl, dbg)
                cl_y[a:b] = S["y"][a:b]; cl_u[ca:cb] = S["u"][ca:cb]; cl_v[ca:cb] = S["v"][ca:cb]
            else:
                cl_y[a:b] = _BLACK_Yi; cl_u[ca:cb] = _NEUT_Ci; cl_v[ca:cb] = _NEUT_Ci
            # ─ PGM = fond + TOUS les keyers (1 dessous, N au-dessus) + overlay HTML ─
            # Si les keyers du clean sont les PREMIERS de la pile, PGM = CLEAN + les suivants (aucun
            # calcul en double) ; sinon le PGM repart du fond et le clean est composé à part.
            if clean_prefix:
                for kl in kls_clean:
                    wait_ns += _sl_key_one(kl, cl_y, cl_u, cl_v, a, b, deadl, dbg)
                pg_y[a:b] = cl_y[a:b]; pg_u[ca:cb] = cl_u[ca:cb]; pg_v[ca:cb] = cl_v[ca:cb]
                for kl in kls[len(kls_clean):]:
                    wait_ns += _sl_key_one(kl, pg_y, pg_u, pg_v, a, b, deadl, dbg)
            else:
                pg_y[a:b] = cl_y[a:b]; pg_u[ca:cb] = cl_u[ca:cb]; pg_v[ca:cb] = cl_v[ca:cb]
                for kl in kls:
                    wait_ns += _sl_key_one(kl, pg_y, pg_u, pg_v, a, b, deadl, dbg)
                for kl in kls_clean:
                    wait_ns += _sl_key_one(kl, cl_y, cl_u, cl_v, a, b, deadl, dbg)
            if ovl is not None:
                _sl_overlay_band(pg_y, pg_u, pg_v, ovl, a, b)
            # ─ PVW = fond futur + keyers dans leur état futur, sans habillage (cf. _pvw_future) ─
            if V is not None:
                wait_ns += _sl_wait(V, b - 1, deadl, dbg)
                pv_y[a:b] = V["y"][a:b]; pv_u[ca:cb] = V["u"][ca:cb]; pv_v[ca:cb] = V["v"][ca:cb]
            else:
                pv_y[a:b] = _BLACK_Yi; pv_u[ca:cb] = _NEUT_Ci; pv_v[ca:cb] = _NEUT_Ci
            for kl in kls_pv:
                wait_ns += _sl_key_one(kl, pv_y, pv_u, pv_v, a, b, deadl, dbg)
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
    # Un key lu à part (grain de son fill) compte pour son slot — une fois par trame.
    for s, st in list(srcs.items()) + [(ck[0], st) for ck, st in keysrcs.items()
                                       if srcs.get(ck[0]) is None]:
        if st is not None and s not in late:
            late[s] = late.get(s, False) or bool(st["late"])
            _in_frames[s] = _in_frames.get(s, 0) + 1
            if st["late"]:
                _in_late[s] = _in_late.get(s, 0) + 1
    _open_wired_inputs()
    _sl_dbg = dbg
    return wait_ns, late


def _reopen_outputs():
    """Nouvelle génération des trois sorties (saut d'horloge) : Writer.regenerate purge chaque
    flux puis le recrée — les lecteurs aval le retrouvent par leur garde de génération (écriture
    figée → réouverture). Pas de garbage_collect : il est GLOBAL au domaine et a déjà tué des flux
    vivants. Binding trop ancien (sans regenerate) : fermer et rouvrir, faute de mieux."""
    global out_pgm, out_clean, out_pvw
    log("mixer: new generation of the outputs after a clock jump", "warning")
    if all(hasattr(w, "regenerate") for w in (out_pgm, out_clean, out_pvw)):
        for w in (out_pgm, out_clean, out_pvw):
            w.regenerate()
        return
    for w in (out_pgm, out_clean, out_pvw):
        try: w.close()
        except Exception: pass
    out_pgm = _ouvrir_sortie(SHM_OUT_PGM)
    out_clean = _ouvrir_sortie(SHM_OUT_CLEAN)
    out_pvw = _ouvrir_sortie(SHM_OUT_PVW)


def _open_wired_inputs():
    """Ouvre les lecteurs de TOUTES les entrées câblées (pas seulement celles à l'antenne), pour que
    l'échantillonneur mesure aussi les entrées hors PGM/PVW. Thread principal seul : ensure_input
    ferme et rouvre des lecteurs."""
    with state_lock:
        wired = [i for i, n in enumerate(state["inputs"]) if n]
    for i in wired:
        ensure_input(i)


_lag_lock = threading.Lock()


def _sample_lags(grid):
    """Retard de chaque entrée ouverte sur le créneau `grid`, en images (0 = l'image du créneau a
    commencé d'arriver). Donnée de la vue « Entrées & délais », de la proposition de délai et du
    « ne pas attendre une entrée connue en retard »."""
    for i, h in enumerate(list(input_handles)):
        if h is None:
            continue
        try:
            with _reader_lock:
                if input_handles[i] is not h:
                    continue
                hi = h[0].head_index()
        except Exception:
            continue
        if hi == bobimxl.MXL_UNDEFINED_INDEX:
            continue
        lag = grid - int(hi)
        with _lag_lock:
            if -2 <= lag <= GROUP_DELAY_MAX + 2:
                q = _in_lag.get(i)
                if q is None:
                    q = _in_lag[i] = deque(maxlen=max(1, 2 * FPS))
                q.append(max(0, lag))
            else:
                _in_lag.pop(i, None)       # hors grille : pas de délai à proposer


def _lag_of(slot):
    """Délai nécessaire mesuré (max sur 2 s), ou None."""
    with _lag_lock:
        q = _in_lag.get(slot)
        return max(q) if q else None


def _lag_sampler():
    """Échantillonne à un instant FIXE de chaque créneau — l'échéance de l'attente (½ période) —
    et JAMAIS à la fin de l'image composée. Mesuré le 2026-10-25 : échantillonné en fin d'image,
    un mélangeur à l'heure finissait avant l'arrivée de sa source (DeckLink à 8,3 ms), la jugeait
    en retard, cessait de l'attendre, finissait plus tôt encore — mesure auto-entretenue, 252
    images sur 252 comptées en retard."""
    while True:
        try:
            if _grid_phase is None:
                time.sleep(0.05)
                continue
            pos = (int(bobimxl.now_tai()) - _grid_phase) % _SLOT_NS
            time.sleep(max(0.0002, ((_SLOT_NS // 2 - pos) % _SLOT_NS) / 1e9))
            _sample_lags(int(bobimxl.current_index(FPS, 1)))
            time.sleep(0.002)          # ne pas ré-échantillonner le même créneau
        except Exception:
            time.sleep(0.1)


# ARRIVÉES (vue « Entrées & délais ») : à quel moment de son créneau la 1ʳᵉ tranche de chaque image
# d'une entrée arrive. 0 = début du créneau de son numéro d'image, sur la MÊME phase que le réveil
# du mélangeur (_grid_phase). L'entrée lue avec un délai d en a jusqu'à (d + ½) créneau : c'est
# l'échéance de la 1ʳᵉ tranche (bind_deadl), le repère dessiné par l'interface.
# Méthode : la tête du flux et lastWriteTime sont posés au même commit, tranche partielle comprise.
# Vu au moment où la tête passe à k, lastWriteTime est l'heure de la 1ʳᵉ tranche de k — à condition
# de regarder avant la tranche suivante. On guette donc chaque entrée toutes les ½ ms, mais
# seulement à partir de sa PLUS PRÉCOCE arrivée des 2 dernières secondes moins 1,5 ms ; le reste du
# temps, toutes les 10 ms. L'heure relevée est celle du WRITER, pas celle de notre réveil : ni la
# gigue du fil ni le GIL n'entrent dans la mesure.
ARRIVAL_WINDOW_S = 4          # statistiques (médiane, p95, max) sur les 4 dernières secondes
ARRIVAL_TREND_STEP_S = 2      # courbe de dérive : une médiane toutes les 2 s…
ARRIVAL_TREND_POINTS = 30     # …sur 60 s
_ARR_MARGIN_NS = 1_500_000    # le guet s'ouvre 1,5 ms avant l'arrivée la plus précoce récente
_ARR_FAST_NS = 500_000        # pas du guet
_ARR_IDLE_NS = 10_000_000     # pas hors guet (et pour une entrée pas encore calée)
_arr_lock = threading.Lock()
_arr = dict()                 # slot → dict(win=deque, trend=deque, bucket=list, t0)


def _slot_ref():
    """(index de grille, instant TAI du début de son créneau), lus sans chevaucher une frontière."""
    while True:
        t1 = int(bobimxl.now_tai())
        g = int(bobimxl.current_index(FPS, 1))
        t2 = int(bobimxl.now_tai())
        if (t1 - _grid_phase) // _SLOT_NS == (t2 - _grid_phase) // _SLOT_NS:
            return g, t2 - ((t2 - _grid_phase) % _SLOT_NS)


def _arr_record(slot, off_ns, now_mono):
    """Range un relevé (ns depuis le début du créneau, ou None = hors grille)."""
    with _arr_lock:
        a = _arr.get(slot)
        if a is None:
            a = _arr[slot] = dict(win=deque(maxlen=2 * ARRIVAL_WINDOW_S * FPS),
                                  trend=deque(maxlen=ARRIVAL_TREND_POINTS), bucket=[], t0=now_mono)
        a["win"].append((now_mono, off_ns))
        if off_ns is not None:
            a["bucket"].append(off_ns)
        if now_mono - a["t0"] >= ARRIVAL_TREND_STEP_S:
            b = sorted(a["bucket"])
            a["trend"].append(b[len(b) // 2] if b else None)
            a["bucket"] = []
            a["t0"] = now_mono


def _arrival_watch():
    """Fil du relevé des arrivées. Les décalages BRUTS (`off`) sont comptés depuis le début du
    créneau du numéro du grain, sans filtre : pour une source hors grille ils sont énormes mais
    COHÉRENTS entre eux, et l'ouverture du guet (créneau suivant + plus précoce récente) reste
    juste — seule la PUBLICATION les écarte."""
    last = dict()     # slot → dernière tête vue
    opens = dict()    # slot → instant TAI d'ouverture du guet de la prochaine image
    armed = set()     # slots vus « pas encore arrivée » depuis l'ouverture du guet
    early = dict()    # slot → deque des décalages bruts récents (2 s)
    low = dict()      # slot → (borne basse posée après un raté ou au calage, instant monotone)
    lo_ok = -_SLOT_NS
    hi_ok = (GROUP_DELAY_MAX + 4) * _SLOT_NS
    while True:
        try:
            if _grid_phase is None:
                time.sleep(0.05)
                continue
            ref = None                      # (g0, s0) lu seulement si une arrivée est vue
            now = int(bobimxl.now_tai())
            mono = time.monotonic()
            wake = now + _ARR_IDLE_NS
            for i, h in enumerate(list(input_handles)):
                if h is None:
                    for d in (last, opens, early, low):
                        d.pop(i, None)
                    armed.discard(i)
                    continue
                e = opens.get(i)
                if e is not None and now < e:
                    wake = min(wake, e)
                    continue
                with _reader_lock:
                    if input_handles[i] is not h:
                        continue
                    hw = h[0].head_and_write_time()
                if hw is None or hw[0] == bobimxl.MXL_UNDEFINED_INDEX:
                    continue
                hd, lwt = int(hw[0]), int(hw[1])
                p = last.get(i)
                last[i] = hd
                if p is None or hd <= p:
                    if e is not None and (hd < (p or 0) or now > e + 3 * _SLOT_NS):
                        opens.pop(i, None)          # source arrêtée ou recalée : pas lent
                        armed.discard(i)
                    elif e is not None:
                        armed.add(i)
                        wake = min(wake, now + _ARR_FAST_NS)
                    continue
                if ref is None:
                    ref = _slot_ref()
                start_k = ref[1] - (ref[0] - hd) * _SLOT_NS  # début du créneau du grain hd
                off = lwt - start_k
                q = early.get(i)
                if q is None:
                    q = early[i] = deque(maxlen=2 * FPS)
                if i in armed and hd == p + 1:
                    # Vu « pas encore là » puis là : lastWriteTime est la 1ʳᵉ tranche.
                    q.append(off)
                    _arr_record(i, off if lo_ok <= off <= hi_ok else None, mono)
                elif e is not None:
                    # Raté : la 1ʳᵉ tranche est passée AVANT l'ouverture du guet (lastWriteTime
                    # peut être celui d'une tranche suivante — on ne le range pas). On ouvre plus
                    # tôt : une source à forte gigue serait sinon mesurée sur ses seules arrivées
                    # tardives, et celle dont les tranches couvrent tout le créneau, sur sa dernière.
                    low[i] = (e - start_k - _SLOT_NS // 4, mono)
                else:
                    low[i] = (off - _SLOT_NS // 2, mono)        # calage : lwt peut être tardif
                armed.discard(i)
                cands = list(q)
                lw = low.get(i)
                if lw is not None and mono - lw[1] <= 2.0:
                    cands.append(lw[0])
                nxt = start_k + _SLOT_NS + min(cands) - _ARR_MARGIN_NS
                if not (now - _SLOT_NS <= nxt <= now + 2 * _SLOT_NS):
                    # Décalages incohérents (numérotation de la source recalée) : on recale.
                    q.clear()
                    low[i] = (off - _SLOT_NS // 2, mono)
                    nxt = start_k + _SLOT_NS + low[i][0] - _ARR_MARGIN_NS
                opens[i] = nxt
                wake = min(wake, nxt)
            left = wake - int(bobimxl.now_tai())
            time.sleep(min(_ARR_IDLE_NS, max(200_000, left)) / 1e9)
        except Exception:
            time.sleep(0.1)


def _arrival_of(slot):
    """Publication d'une entrée : statistiques en ms (bas = 5ᵉ centile), courbe, hors grille.
    Fenêtre en TEMPS, pas en nombre : une entrée qui cesse d'être mesurée ne garde pas ses
    anciennes valeurs à l'écran."""
    cut = time.monotonic() - ARRIVAL_WINDOW_S
    with _arr_lock:
        a = _arr.get(slot)
        if a is None:
            return None
        win = [o for t, o in a["win"] if t >= cut]; trend = list(a["trend"])
    v = sorted(x for x in win if x is not None)
    ms = lambda x: round(x / 1e6, 2)
    out = dict(n=len(v), offgrid=(not v and bool(win)),
               trend=[(None if x is None else ms(x)) for x in trend])
    if v:
        q = lambda f: v[min(len(v) - 1, int(f * len(v)))]
        out.update(lo=ms(q(0.05)), med=ms(q(0.5)), p95=ms(q(0.95)), max=ms(v[-1]))
    return out


def _inputs_timing():
    """Publication par entrée (clés 1-based) : délai appliqué, groupe, délai nécessaire mesuré
    (max sur 2 s ; None = hors grille ou jamais lue), taux de replis."""
    with state_lock:
        mode = state["latency_mode"]; delays = state["delays"]
        names = list(state["inputs"])
    out = dict()
    for i, name in enumerate(names):
        if not name:
            continue
        need = _lag_of(i)
        fr = _in_frames.get(i, 0)
        # `need` = délai nécessaire MESURÉ (arrivée) ; ce que le groupe doit offrir = need + upstream.
        out[str(i + 1)] = {{
            "shm": name,
            "group": delays["inputs"].get(str(i + 1)),
            "delay": _input_delay(i, mode, delays),
            "upstream": _upstream_of(i, delays),
            "read_delay": _read_delay(i, mode, delays),
            "need": need,
            "frames": fr,
            "late": _in_late.get(i, 0),
            "arrival": _arrival_of(i),
        }}
    return out


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
    log(f"mixer: slice mode on — {{SLICE_LINES}} lines/band ({{_SL_NB}} slices/frame), "
        "paced by the TAI grid, inputs read at slot minus their delay", "info")

def _ecrire_sortie(w, yuv, frame_index):
    try:
        _i, gi, vw = w.open_grain(index=frame_index)
        vw[:len(yuv)] = np.frombuffer(yuv, dtype=np.uint8)
        w.commit(gi)
    except Exception:
        pass

if SLICE_ON:
    threading.Thread(target=_lag_sampler, daemon=True).start()
    threading.Thread(target=_arrival_watch, daemon=True).start()

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
                with _reader_lock:
                    input_handles[i] = None
                    try: h[0].close()
                    except Exception: pass
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
                    _nr_after(trans.get("preset"))
                for _ki, _kp in (trans.get("keyers") or dict()).items():
                    state["keyers"][_ki]["active"] = _kp["to_active"]
                state["transition"] = None
                pgm = state["pgm"]; pvw = state["pvw"]
                trans = None
        ovl_active   = state["overlay_enabled"] and bool(state["overlay_url"])
        _finish_keyer_autos()
        keyer_state  = dict(state["keyer"])
        # KEYERS : réglages + alpha FIGÉS pour toute la trame (latch par grain).
        keyers_now   = _latch_keyers(manual, trans)
        # PVW (0.23.29) : fond et keyers de l'état FUTUR, figés au même latch (cf. _pvw_future).
        pvw_bgd, pvw_keyers = _pvw_future(pgm, pvw, manual, trans, keyers_now)
        # Volet du fond : géométrie figée au départ, adoucissement VIVANT (lu sous le même verrou).
        manual_preset = _live_wipe_preset(manual.get("preset")) if manual is not None else None
        trans_preset  = _live_wipe_preset(trans.get("preset")) if trans is not None else None

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
        bgd_preset   = manual_preset
    elif trans is not None and trans.get("bgd"):
        bgd_active = True
        bgd_alpha  = max(0.0, min(1.0,
            (time.time() - trans["start"]) * 1000.0 / trans["duration_ms"]))
        bgd_from_idx = trans["bgd"]["from_idx"]
        bgd_to_idx   = trans["bgd"]["to_idx"]
        bgd_preset   = trans_preset
    _tally_now = _tally_inputs(bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, pgm,
                               keyers_now, pvw_bgd, pvw_keyers)

    # ─ Cadence : genlock verrou sur l'entrée de RÉFÉRENCE (choix opérateur, FIXE), ou free-run ─
    # On émet 1 sortie par nouvelle trame de la réf (verrou 1:1, latence mini, pas de requantif). La
    # réf est `state["sync_ref"]` — INDÉPENDANTE de PGM/PVW → ne change pas à la commutation. Les
    # autres entrées sont alignées sur T = write_ts de la réf, dans une fenêtre d'ALIGN_BUDGET trames ;
    # au-delà → présentées en retard + flag. Réf absente/morte → repli free-run (programme vivant)+flag.
    T = None; ref_lost = False
    with state_lock:
        ref_idx = max(0, min(N_INPUTS - 1, int(state["sync_ref"])))
    grid_now = None
    if SLICE_ON:
        # MODE TRANCHE (0.23.9) : cadencé par l'HORLOGE — un tour par créneau de la grille TAI,
        # chaque entrée lue à « créneau − son délai ». `sync_ref` ne sert plus (chemin image
        # entière seulement) : une caméra en panne ne retient ni ne décale plus rien.
        grid_now = _wait_next_slot()
        ref_idx = None
    elif GENLOCK:
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
                cand = set([bgd_from_idx, bgd_to_idx, pgm, pvw, pvw_bgd])
                for _kn in keyers_now + pvw_keyers:
                    if _kn["alpha"] > 0.001:
                        cand.add(_kn["input"])
                        if _kn["type"] == "linear":
                            cand.add(_kn["key_input"])
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
    _sl_waited = 0   # mode tranche : cumul des attentes get_slice de la trame (EXCLUES de own_latency)
    if SLICE_ON:
        # ─ MODE TRANCHE : trame bandée à l'INDEX de la référence (verrou-référence INDEXÉ).
        # LATCH d'état par grain : bgd_alpha/bgd_preset/sélections (calculés ci-dessus, avant la
        # cadence), keyer et overlay sont FIGÉS ICI et servent pour TOUTES les bandes de la trame
        # — jamais un changement d'état à mi-trame (une trame moitié-cut serait fausse).
        keyer_alpha_now = keyers_now[0]["alpha"]
        ovl = None
        if ovl_active:
            with overlay_lock:
                ovl = overlay_prep
        _sl_late = dict()
        try:
            _sl_waited, _sl_late = _slice_frame(int(grid_now), None,
                                                bgd_active, bgd_alpha,
                                                bgd_from_idx, bgd_to_idx, bgd_preset,
                                                keyer_alpha_now, keyer_state, ovl,
                                                pgm, pvw_bgd, ts_in_per_slot, keyers=keyers_now,
                                                pvw_keyers=pvw_keyers)
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
        # ─ Chemin HISTORIQUE whole-frame (slice_mode off ou inéligible) — inchangé : cadence
        # genlock sur sync_ref, alignement par write_ts, index de grille.
        out_index = out_pgm.next_index()
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

        # Le CLEAN contient le fond + les keyers RÉGLÉS sur le clean (niveau de clean, 0.23.15).
        _kclean = [_kn for _kn in keyers_now if _kn.get("clean")]
        yuv_clean = yuv_bgd
        if any(_kn["alpha"] > 0.001 for _kn in _kclean):
            yuv_clean = _compose_keyers_whole(yuv_bgd, _kclean, ts_in_per_slot)

        # Le PGM contient BGD + keyers (1 dessous, N au-dessus) + overlay HTML.
        yuv_pgm = yuv_bgd
        if any(_kn["alpha"] > 0.001 for _kn in keyers_now):
            yuv_pgm = _compose_keyers_whole(yuv_pgm, keyers_now, ts_in_per_slot)
        if ovl_active:
            with overlay_lock:
                ovl = overlay_prep
            if ovl is not None:
                yuv_pgm = composite_overlay(yuv_pgm, ovl)

        # ─ Bus PVW = fond futur + keyers dans leur état futur, SANS overlay (cf. _pvw_future).
        #   Aucun keyer visible au PVW → la copie de l'entrée, comme avant 0.23.29.
        yuv_pvw_src, ts_pvw = lire_frame_yuv(ensure_input(pvw_bgd))
        if ts_pvw: ts_in_per_slot[pvw_bgd] = ts_pvw
        yuv_pvw = yuv_pvw_src or empty_frame
        if any(_kn["alpha"] > 0.001 for _kn in pvw_keyers):
            _t_pv = time.time_ns()
            if (_t_pv - ts_cycle_start) / 1e6 + _pvw_cost_ms > PVW_BUDGET_FRAC * 1000.0 / FPS:
                _pvw_skips += 1          # PGM d'abord : le PVW sort sans ses keyers (cf. PVW_BUDGET_FRAC)
            else:
                yuv_pvw = _compose_keyers_whole(yuv_pvw, pvw_keyers, ts_in_per_slot)
                _pvw_cost_ms = (time.time_ns() - _t_pv) / 1e6

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
            metrics["pvw_keyers_skipped"] = _pvw_skips
            metrics["delai_etage_trames"] = delai_etage.publish()
            if SLICE_ON:
                # Observabilité mode tranche : compteurs de la dernière trame + garde-fou n°2.
                _sd = _sl_dbg
                if _sd:
                    metrics["slice"] = {{"inputs": _sd[0], "valid0": _sd[1], "waits": _sd[2],
                                        "fallbacks": _sd[3], "dormant": _sd[4],
                                        "backoff": len(_sl_backoff)}}
                metrics["slice_late_inputs"] = dict(_sl_flagged)
                with state_lock:
                    metrics["latency_mode"] = state["latency_mode"]
                metrics["stamp"] = dict(_stamp, clock_jumps=_clock_jumps)
                metrics["inputs_timing"] = _inputs_timing()
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
