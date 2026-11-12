# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

import mmap, struct, time, threading, json, os, signal, io, sys
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
# BANDES DE CALCUL REGROUPÉES (0.23.41, 2026-10-29) : le découpage PUBLIÉ ne change pas
# (slice_height = SLICE_LINES, commit validSlices=k), mais quand toutes les entrées qu'une trame lit
# sont DÉJÀ COMPLÈTES (tranches valides réelles, pas le réglage de délai), on compose SLICE_GROUP
# bandes publiées d'un coup — moins d'itérations Python, moins d'appels ctypes. Coût fixe mesuré
# ~50-80 µs par bande, par keyer et par opération ; banc hors ligne, un keyer linéaire mvk :
# 8 bandes 3,3 ms, 30 bandes 4,6 ms, 90 bandes 9,3 ms. Une entrée incomplète → bande par bande
# (attente au plus près), et la décision se reprend à chaque bande : dès que tout est arrivé, on
# regroupe. Les octets sont identiques (toutes les opérations sont locales à la ligne).
# DÉFAUT G = 6 (216 lignes, 5 bandes de calcul par image 1080p), choisi sur MESURE le 2026-10-29,
# banc mes-mix 1277 (dell-1, 1080p50, délai 1 image, keyer linear plein cadre fill + key du player),
# 3 passes par G, « own » / commit de la 1re tranche PGM / travail propre du fil PVW, en ms :
#   B2 (1 keyer au PGM et au PVW) : G=1 12,7 / 1,5 / 10,6 ; G=2 9,9 / 2,0 / 7,7 ; G=3 7,5 / 1,8 / 6,0 ;
#     G=5 7,3 / 2,2 / 5,6 ; G=6 6,8 / 2,2 / 5,3 ; G=10 7,3 / 3,4 / 5,2 ; G=15 6,6 / 4,0 / 4,5 ;
#     G=30 7,0 / 6,8 / 5,3 (image entière).
#   C2 (2 keyers) : G=1 20,1 (fps min 42,1 : la 0.23.40 ne tenait pas la cadence) ; G=6 9,2 / 3,1 ;
#     G=15 8,6 / 5,3 ; G=30 9,0 / 8,9.
# Au-delà de G=6 le calcul ne gagne plus que dans le bruit (±1 ms) alors que la 1re tranche, celle
# qu'attend l'aval, recule : on garde des bandes moyennes plutôt que l'image entière.
SLICE_GROUP = max(1, min(_SL_NB or 1, int(CONFIG.get("slice_group") or 6)))
_sl_grouping = dict(pgm=dict(grouped=0, fine=0, mixed=0), pvw=dict(grouped=0, fine=0, mixed=0))
_pgm_first_ms = deque(maxlen=50)   # ms : ouverture de trame → commit de la 1re tranche PGM (aval)


# ─── IMAGE MANQUANTE : RECOPIE ou NOIR (0.23.60, décision de Cyril du 2026-10-30) ─────────────
# Une capture DeckLink qui reçoit une trame à plus d'un créneau de retard SAUTE son index et laisse
# un TROU (elle ne fabrique volontairement rien : le trou est l'information). Lu ici, ce trou ne
# rendait AUCUN grain : le fond passait au NOIR (`S is None` du mode tranche, `empty_frame` de
# l'image entière, `V is None` du fil PVW), et une couche dont le fill manquait disparaissait —
# les flashs vus en exploitation, et les `invalid_grains` sur Atelier_1/Atelier_2.
#
# `missing_frame_mode_effective` (résolu par l'orchestrateur AU RENDU, jamais lu dans un réglage) :
#   · `repeat` (défaut) — l'entrée est remplacée par SA DERNIÈRE IMAGE VALIDE ;
#   · `black` — comportement d'avant 0.23.60, pour la mise au point.
# Modifiable à chaud (POST /hot_config, `control.hot_config`). Un CONFIG sans la clé (rendu par un
# orchestrateur antérieur) vaut `repeat`, le défaut du global.
#
# ★ LA RECOPIE N'EST JAMAIS MUETTE : `missing_frames_repeated` (:8080 et /state), total et par
#   entrée = images ÉMISES qui ne sont pas une image fraîche de la source — ici, une entrée
#   remplacée par sa dernière image valide (une fois par trame et par entrée, même si plusieurs
#   couches la lisent). `invalid_grains` continue de compter les trous. Au-delà de
#   MISSING_ALERT_N recopies en MISSING_ALERT_WINDOW_S sur une entrée : avis à l'orchestrateur.
#   (Le repli « dernière image complète » d'une entrée simplement EN RETARD existait avant et reste
#   compté à part : `slice_late_inputs`, retards de la vue Entrées & délais.)
#
# ★ CE QUI EST GARDÉ, ET POURQUOI C'EST SÛR — AUCUNE COPIE SUR LE CHEMIN NOMINAL :
#   · MODE TRANCHE : on garde l'INDEX du dernier grain lu (et son lecteur), jamais une vue mémoire.
#     Une vue pointe dans l'anneau MXL : le producteur la réécrit quand il revient sur la case, et
#     ce n'est alors plus la dernière image valide — on composerait une image FUTURE, ou à moitié
#     écrite, en la croyant ancienne. À la recopie, le grain est RELU par son index
#     (`get_slice_nonblocking`, toutes tranches exigées) et accepté seulement si (1) le lecteur est
#     le même (une réouverture rend l'index sans objet), (2) l'en-tête de la case porte TOUJOURS
#     cet index (sinon elle a été réécrite), (3) il est complet et pas un bouche-trou, (4) la tête
#     du producteur n'est pas plus de REPEAT_MAX_AGE grains devant lui. (4) borne le risque de
#     réécriture PENDANT la composition : l'anneau compte au moins 5 grains (historique MXL ≥ 100 ms
#     à 50 i/s, 10 au défaut de 200 ms) et la composition dure au plus une période, PVW compris ;
#     au-delà, l'entrée retombe au noir — ce n'est plus une image manquante, c'est une source perdue.
#   · IMAGE ENTIÈRE : `lire_frame_yuv` rend déjà une COPIE (bytes) — on garde une RÉFÉRENCE à ces
#     octets, immuables, donc sûrs ; même horizon de REPEAT_MAX_AGE trames de boucle.
MISSING_FRAME_MODES = ("repeat", "black")
# Clés que /hot_config prend À CHAUD ou ignore dans la comparaison (`plugin_version` du CONFIG est
# l'estampille du déploiement PRÉCÉDENT ; la vraie garde est celle du corps de la requête).
_HOT_CONFIG_KEYS = ("missing_frame_mode", "missing_frame_mode_effective", "plugin_version")
def _mf_norm(v):
    s = str(v or "").strip().lower()
    return s if s in MISSING_FRAME_MODES else "repeat"
MISSING_FRAME_MODE = _mf_norm(CONFIG.get("missing_frame_mode_effective"))
REPEAT_MAX_AGE = 3
MISSING_ALERT_N = 5
MISSING_ALERT_WINDOW_S = 10.0
_mf_last = dict()        # slot → (lecteur, index, tranches)   mode tranche : par INDEX
_mf_whole = dict()       # slot → (lecteur, octets, tour)       image entière : référence aux octets
_mf_count = dict()       # slot → images recopiées (depuis le démarrage)
_mf_total = [0]
_mf_loop = [0]           # tour de boucle courant (base de l'âge en image entière)
_mf_frame = set()        # entrées déjà comptées dans la trame courante
_mf_recent = deque()     # (monotone, slot) des recopies récentes (fenêtre d'avis)
_mf_flagged = ()         # entrées signalées (numérotées à partir de 1)


def _mf_frame_start():
    """Début d'une trame : une entrée n'est comptée qu'UNE fois par trame (cf. compteur)."""
    _mf_loop[0] += 1
    _mf_frame.clear()


def _mf_counted(slot):
    if slot in _mf_frame:
        return
    _mf_frame.add(slot)
    _mf_count[slot] = _mf_count.get(slot, 0) + 1
    _mf_total[0] += 1
    _mf_recent.append((time.monotonic(), slot))


def _mf_note_slice(slot, r, fi, total):
    _mf_last[slot] = (r, int(fi), int(total))


def _mf_repeat_slice(slot, r):
    """MODE TRANCHE : le dernier grain valide de l'entrée, RELU par son index — ou None (noir)."""
    if MISSING_FRAME_MODE != "repeat" or r is None:
        return None
    ref = _mf_last.get(slot)
    if ref is None or ref[0] is not r:
        return None
    idx, total = ref[1], ref[2]
    try:
        h = r.head_index()
        if h == bobimxl.MXL_UNDEFINED_INDEX or not (0 <= int(h) - idx <= REPEAT_MAX_AGE):
            return None
        g = r.get_slice_nonblocking(idx, total)
    except Exception:
        return None
    if (g is None or int(g[1].index) != idx or g[2].size != V_FRAME_SIZE
            or int(g[1].validSlices or 0) < total):
        return None
    _mf_counted(slot)
    return g


def _mf_note_whole(slot, r, f):
    _mf_whole[slot] = (r, f, _mf_loop[0])


def _mf_repeat_whole(slot, r):
    """IMAGE ENTIÈRE : les octets de la dernière image lue sur cette entrée — ou None (noir)."""
    if MISSING_FRAME_MODE != "repeat" or r is None:
        return None
    ref = _mf_whole.get(slot)
    if ref is None or ref[0] is not r or _mf_loop[0] - ref[2] > REPEAT_MAX_AGE:
        return None
    _mf_counted(slot)
    return ref[1]


def _read_whole(slot):
    """IMAGE ENTIÈRE : (octets, ts) de l'entrée `slot`, sa dernière image valide si l'image manque
    (mode repeat, ts None : une recopie n'a pas d'âge de transit), sinon (None, None)."""
    r = ensure_input(slot)
    f, ts = lire_frame_yuv(r)
    if f:
        _mf_note_whole(slot, r, f)
        return f, ts
    return _mf_repeat_whole(slot, r), None


def _mf_publish():
    """(compteurs publiés, avis ou None) — appelé une fois par seconde, fil vidéo."""
    global _mf_flagged
    now = time.monotonic()
    while _mf_recent and now - _mf_recent[0][0] > MISSING_ALERT_WINDOW_S:
        _mf_recent.popleft()
    win = dict()
    for _t, s in _mf_recent:
        win[s] = win.get(s, 0) + 1
    _mf_flagged = tuple(sorted(s + 1 for s, n in win.items() if n >= MISSING_ALERT_N))
    pub = dict(missing_frame_mode=MISSING_FRAME_MODE,
               missing_frames_repeated=_mf_total[0],
               missing_frames_repeated_inputs=dict((str(s + 1), n) for s, n in sorted(_mf_count.items())))
    adv = ({{"key": "plugin.mixer.advisory.missing_frames_repeated",
             "params": {{"entrees": ", ".join("#%d" % n for n in _mf_flagged)}}}}
           if _mf_flagged else None)
    return pub, adv


def _mf_set_mode(mode):
    """Bascule À CHAUD (hot_config). Rend True si le mode a changé."""
    global MISSING_FRAME_MODE
    m = _mf_norm(mode)
    if m == MISSING_FRAME_MODE:
        return False
    log("mixer: missing-frame mode %s -> %s" % (MISSING_FRAME_MODE, m), "info")
    MISSING_FRAME_MODE = m
    return True


def _sl_all_full(states):
    """Vrai si chaque entrée lue a TOUTES ses tranches valides (aucune attente possible)."""
    for st in states:
        if st is not None and st["valid"] < st["total"]:
            return False
    return True


def _sl_count_grouping(which, n_coarse, n_fine):
    c = _sl_grouping[which]
    if n_fine == 0:
        c["grouped"] += 1
    elif n_coarse == 0:
        c["fine"] += 1
    else:
        c["mixed"] += 1

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


def _rename_delay_group(d, old, new):
    """Renomme un groupe EN PLACE (document déjà normalisé) : délai, entrées rangées et groupe de
    référence suivent le nouveau nom, rien n'est recalculé. Rend (document, code d'erreur|None) ;
    l'erreur est un CODE (unknown_group, name_required, name_taken) que l'interface traduit."""
    old = str(old or "").strip()
    new = str(new or "").strip()[:GROUP_NAME_MAX]
    groups = d["groups"]
    if old not in groups:
        return d, "unknown_group"
    if not new:
        return d, "name_required"
    if new == old:
        return d, None
    if new in groups:
        return d, "name_taken"
    return {{"groups": dict(((new if k == old else k), v) for k, v in groups.items()),
            "inputs": dict((k, (new if v == old else v)) for k, v in d["inputs"].items()),
            "ref_group": new if d["ref_group"] == old else d["ref_group"],
            "upstream": dict(d["upstream"])}}, None


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
#   web    — (0.23.52) une PAGE HTML rendue dans le conteneur (Chromium partagé, un onglet par
#            couche web) sert de fill ET d'alpha : couleur de la page, transparence de la page.
#            Composée comme un linear dont le key serait l'alpha de la page (découpe, inversion,
#            masque, taille et position compris) — cf. COUCHES WEB plus bas.
# Les transitions, l'ON AIR, l'AUTO et le CLEAN sont communs à tous les types.
KEYER_TYPES = ("dve", "linear", "luma", "chroma", "pattern", "web")
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

# ─── DSK (0.23.52, 2026-10-29, décision de l'utilisateur) ───────────────────────────────────
# Keyers AVAL, comme les DSK d'un ATEM : composés APRÈS la transition du M/E et au-dessus de TOUS
# les keyers amont, jamais dans le CLEAN (prélevé avant eux). Nombre fixé au déploiement (`n_dsk`,
# 0 à 2). Une couche DSK a les mêmes types et réglages qu'un keyer (dve, linear, luma, chroma,
# motif, web ; masque, taille et position, découpe), son ON AIR (CUT) et son AUTO (durée propre),
# et un TIE : lié, il est emporté par la prochaine transition du fond et entre ou sort avec elle.
# Le PVW le montre dans son état futur (à l'antenne XOR lié), et le tally compte ses sources.
# HABILLAGE HISTORIQUE (overlay_url / overlay_enabled, /overlay, /config overlay_*, /preview.png,
# /state.overlay_*) : c'est le DSK 1, de type web. Un mélangeur déployé SANS `n_dsk` (ancien
# schéma) a UN DSK, de type web, dont l'URL et l'état d'antenne viennent de overlay_url et
# overlay_enabled — mêmes valeurs, mêmes endpoints.
# Indexation interne : une COUCHE est un keyer (0 à N_KEYERS − 1) ou un DSK (N_KEYERS + j) ; les
# mouvements (transition M/E, T-bar, AUTO propre) sont rangés par numéro de couche. Publication :
# `keyers` et `dsks` séparés, numérotés à partir de 1 chacun.
MAX_DSK = 2
_ND_RAW = CONFIG.get("n_dsk")
DSK_LEGACY = _ND_RAW in (None, "")
try:
    _ND_ASKED = 1 if DSK_LEGACY else int(_ND_RAW)
except (TypeError, ValueError):
    _ND_ASKED = 1
N_DSK = max(0, min(MAX_DSK, _ND_ASKED))
if _ND_ASKED != N_DSK:
    _nd_warn = ("n_dsk=%s hors bornes (0..%d) — %d DSK appliqué(s)" % (_ND_RAW, MAX_DSK, N_DSK))
    CONFIG_WARNING = (CONFIG_WARNING + " ; " + _nd_warn) if CONFIG_WARNING else _nd_warn
    log("CONFIG: n_dsk=%s out of range (0..%d), %d DSK applied" % (_ND_RAW, MAX_DSK, N_DSK), "warning")
N_LAYERS = N_KEYERS + N_DSK
LAYER_URL_MAX = 2048

# ─── COUCHES WEB (0.23.52) : limite ─────────────────────────────────────────────────────────
# Une couche web = un onglet du Chromium PARTAGÉ du conteneur, capturé en boucle (capture PNG,
# décodage, conversion YUV quand l'image change). Elle ne compte que si elle a une URL : sans URL,
# aucun onglet. Le nombre de couches web AVEC URL est borné (WEB_LAYERS_MAX, publié dans
# /state.caps) : au-delà, le réglage est REFUSÉ (409, code `web_limit`, traduit par l'interface),
# jamais appliqué en silence. Le coût mesuré par onglet est publié sur :8080 (`web_layers`).
WEB_LAYERS_MAX_DEFAULT = 2
try:
    WEB_LAYERS_MAX = max(0, min(N_LAYERS, int(CONFIG.get("web_layers_max", WEB_LAYERS_MAX_DEFAULT))))
except (TypeError, ValueError):
    WEB_LAYERS_MAX = min(N_LAYERS, WEB_LAYERS_MAX_DEFAULT)
_MASK_EDGES = ("left", "right", "top", "bottom")


def _to_int(v):
    return int(round(float(v)))


# ─── VOLETS (0.23.44, 2026-10-29) : formes, angle, centre, bordure, symétrie, répétition ─────────
# Un volet = un CHAMP de distance d (float32 plein cadre, 0 ≤ d ≤ M) qui ne dépend que de la
# GÉOMÉTRIE (forme, angle, centre, symétrie, répétition, sens) — calculé UNE fois par géométrie et
# mis en cache (précalcul en tâche de fond dès que le préréglage change, cf. _wipe_prefetch) — puis,
# à chaque trame, une RAMPE le long de d : le bord est en e(α), B là où d < e, A au-delà.
# La course de e part d'avant le premier pixel (d = 0) et finit après le dernier (d = M), marges de
# douceur et de bordure comprises : α = 0 → tout A, α = 1 → tout B, quels que soient les réglages
# (garde tests/verif_mixer_volets.py, sur toutes les combinaisons).
# REV : d → M − d pour TOUTES les formes — le linéaire part de l'autre bord, une forme se referme
# (B arrive par l'extérieur) ; N/R alterne les deux, inchangé.
# Unités : d est NORMALISÉ (1 ≈ demi-diagonale de l'image, ou de la cellule en répétition ; le
# linéaire en demi-étendue de sa projection). Douceur, largeur et douceurs de bordure sont des
# FRACTIONS DE LA COURSE M : réglage indépendant de la définition, et qui suit la répétition.
WIPE_SHAPES = ("linear", "iris", "box", "diamond", "star", "heart", "clock")
# Les trois anciens noms sont des volets LINÉAIRES à angle prédéfini : acceptés partout (presets
# persistés, macros, ATEM), normalisés en « linear » + angle là où l'angle se règle.
WIPE_LEGACY_ANGLES = {{"horizontal": 0.0, "vertical": 90.0, "diagonal": 45.0}}
WIPE_NAMES = ("horizontal", "vertical", "diagonal") + WIPE_SHAPES
WIPE_REPEAT_MAX = 16
WIPE_BORDER_MAX = 0.5
WIPE_DEFAULT = dict(direction="linear", angle=0.0, center_x=0.5, center_y=0.5, symmetry=False,
                    repeat=1, reverse=False, softness=0.10, alternate=False,
                    border_width=0.0, border_color="#ffffff", border_soft_in=0.0, border_soft_out=0.0)
_WIPE_NUM = dict(softness=(0.0, 1.0), center_x=(0.0, 1.0), center_y=(0.0, 1.0),
                 border_width=(0.0, WIPE_BORDER_MAX), border_soft_in=(0.0, 1.0),
                 border_soft_out=(0.0, 1.0))
# Réglages VIVANTS pendant un volet (relus à chaque trame, cf. _live_wipe_preset) : ce qu'on tourne
# en regardant le résultat. Tout le reste (forme, angle, centre, symétrie, répétition, sens) est
# FIGÉ au départ : changé en cours de volet, le bord sauterait.
WIPE_LIVE = ("softness", "border_width", "border_color", "border_soft_in", "border_soft_out")
_STAR_K = 0.381966          # rayon intérieur / extérieur de l'étoile régulière à 5 branches


def _wipe_patch(cur, w):
    """Applique à `cur` (réglage de volet) les champs présents dans `w`, bornés. Ancien nom
    (horizontal / vertical / diagonal) → « linear » + son angle ; un angle EXPLICITE dans le même
    appel l'emporte. Rend la liste des champs refusés — les autres sont appliqués."""
    bad = []
    if "direction" in w:
        d = str(w.get("direction") or "").strip().lower()
        if d in WIPE_LEGACY_ANGLES:
            cur["direction"] = "linear"
            cur["angle"] = WIPE_LEGACY_ANGLES[d]
        elif d in WIPE_SHAPES:
            cur["direction"] = d
        else:
            bad.append("direction")
    if "angle" in w:
        try:
            a = float(w.get("angle"))
            if a != a or a in (float("inf"), float("-inf")):
                raise ValueError
            cur["angle"] = round(a % 360.0, 3)
        except Exception:
            bad.append("angle")
    for key, (lo, hi) in _WIPE_NUM.items():
        if key in w:
            try:
                x = float(w.get(key))
                if x != x:
                    raise ValueError
                cur[key] = max(lo, min(hi, x))
            except Exception:
                bad.append(key)
    for key in ("reverse", "alternate", "symmetry"):
        if key in w:
            cur[key] = _as_bool(w.get(key))
    if "repeat" in w:
        try:
            cur["repeat"] = max(1, min(WIPE_REPEAT_MAX, int(round(float(w.get("repeat"))))))
        except Exception:
            bad.append("repeat")
    if "border_color" in w:
        c = _norm_hex(w.get("border_color"))
        if c:
            cur["border_color"] = c
        else:
            bad.append("border_color")
    return bad


def _wipe_norm(raw):
    """Réglage de volet complet (défauts + `raw` relu à travers les bornes)."""
    w = dict(WIPE_DEFAULT)
    if isinstance(raw, dict):
        _wipe_patch(w, raw)
        for key in ("reverse", "alternate"):        # pas dans _wipe_patch si absents : défauts
            if key in raw:
                w[key] = _as_bool(raw[key])
    return w


def _wipe_geom(w):
    """Clé de GÉOMÉTRIE d'un réglage (forme, angle effectif, centre, symétrie, répétition).
    Un ancien nom donne son angle prédéfini, auquel s'ajoute `angle` s'il y en a un (AUTO d'un
    keyer : pas d'angle propre, le nom suffit)."""
    name = str(w.get("direction") or "linear").strip().lower()
    shape = "linear" if name in WIPE_LEGACY_ANGLES else (name if name in WIPE_SHAPES else "linear")
    try:
        ang = (WIPE_LEGACY_ANGLES.get(name, 0.0) + float(w.get("angle", 0.0) or 0.0)) % 360.0
    except Exception:
        ang = WIPE_LEGACY_ANGLES.get(name, 0.0)
    def _f(k, dflt):
        try:
            return round(max(0.0, min(1.0, float(w.get(k, dflt)))), 4)
        except Exception:
            return dflt
    try:
        rep = max(1, min(WIPE_REPEAT_MAX, int(w.get("repeat", 1) or 1)))
    except Exception:
        rep = 1
    return (shape, round(ang, 3), _f("center_x", 0.5), _f("center_y", 0.5),
            bool(w.get("symmetry")), rep)


# Champs numériques réglables d'un keyer : (min, max, conversion). Une valeur hors bornes est
# ramenée dans les bornes (comme /keyer/transform l'a toujours fait) ; une valeur ILLISIBLE est
# refusée et nommée dans la réponse.
_KEYER_NUM = dict(x=(0.0, 1.0, float), y=(0.0, 1.0, float), scale=(0.05, 1.0, float),
                  border_width=(0, 50, _to_int), clip=(0.0, 1.0, float),
                  gain=(KEY_GAIN_MIN, KEY_GAIN_MAX, float), auto_ms=(0, KEYER_AUTO_MS_MAX, _to_int),
                  auto_soft=(0.0, 1.0, float),
                  chroma_tol=(0.0, 1.0, float), chroma_soft=(0.0, 1.0, float), chroma_spill=(0.0, 1.0, float),
                  pattern_size=(0.0, 1.0, float), pattern_x=(0.0, 1.0, float), pattern_y=(0.0, 1.0, float),
                  pattern_soft=(0.0, 1.0, float),
                  # motif : angle, répétition, bordure (0.23.44, 2026-10-29)
                  pattern_angle=(0.0, 360.0, float), pattern_repeat=(1, WIPE_REPEAT_MAX, _to_int),
                  pattern_border=(0.0, WIPE_BORDER_MAX, float), pattern_border_soft_in=(0.0, 1.0, float),
                  pattern_border_soft_out=(0.0, 1.0, float))
# Type de l'AUTO PROPRE d'un keyer (0.23.19) : fondu (mix) ou volet (wipe, mêmes motifs que le fond).
# Ne vaut que pour l'AUTO du keyer : emporté par la transition M/E, le keyer suit celle-ci.
KEYER_AUTO_MODES = ("mix", "wipe")
# Motifs de l'AUTO d'un keyer : tous les noms de volet (anciens noms compris), sans angle propre.
WIPE_DIRECTIONS = WIPE_NAMES
# Formes du keyer MOTIF (0.23.22 ; 0.23.44 : celles du volet, la boîte en fait partie). Vocabulaire
# RANGÉ ; les anciens noms sont acceptés en entrée (→ « linear » + pattern_angle).
PATTERN_SHAPES = WIPE_SHAPES
CHROMA_KEY_DEFAULT = "#00b140"
# Ce qui survit à un redéploiement (document persisté par l'orchestrateur, cf. GET /delays).
# L'état d'antenne (active) n'en fait PAS partie : un redéploiement ne remet rien à l'antenne.
_KEYER_PERSIST = ("type", "input", "key_input", "x", "y", "scale", "border_width",
                  "clip", "gain", "invert", "auto_ms", "clean",
                  "auto_mode", "auto_dir", "auto_rev", "auto_soft", "auto_alt",
                  "chroma_color", "chroma_tol", "chroma_soft", "chroma_spill",
                  "pattern", "pattern_size", "pattern_x", "pattern_y", "pattern_soft",
                  "pattern_angle", "pattern_repeat", "pattern_border", "pattern_border_color",
                  "pattern_border_soft_in", "pattern_border_soft_out", "pattern_composite",
                  # couche web (0.23.52) : l'URL de la page
                  "url")


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
                pattern="iris", pattern_size=0.50, pattern_x=0.5, pattern_y=0.5, pattern_soft=0.10,
                # motif (0.23.44) : sans rotation, une forme, sans bordure (blanche quand on la pose)
                pattern_angle=0.0, pattern_repeat=1, pattern_border=0.0, pattern_border_color="#ffffff",
                pattern_border_soft_in=0.0, pattern_border_soft_out=0.0,
                # volet composé comme forme du motif (0.23.45) : son nom, vide = forme simple
                pattern_composite="",
                # couche web (0.23.52) : page rendue (vide = aucun onglet, rien à composer)
                url="")


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
                       ("pattern", WIPE_NAMES)):
        if key in body:
            v = str(body.get(key) or "").strip().lower()
            if v in vocab:
                k[key] = v
            else:
                bad.append(key)
    # Motif : un ancien nom (horizontal, vertical, diagonal) est une barre LINÉAIRE à angle
    # prédéfini (0.23.44) — rangée « linear » + pattern_angle, sauf angle explicite dans l'appel.
    if k.get("pattern") in WIPE_LEGACY_ANGLES:
        if "pattern_angle" not in body:
            k["pattern_angle"] = WIPE_LEGACY_ANGLES[k["pattern"]]
        k["pattern"] = "linear"
    if "pattern_composite" in body:
        v = _composite_name(body.get("pattern_composite")) or ""
        if not v or v in _wipe_composites:
            k["pattern_composite"] = v
        else:
            bad.append("pattern_composite")
    if "pattern_border_color" in body:
        c = _norm_hex(body.get("pattern_border_color"))
        if c:
            k["pattern_border_color"] = c
        else:
            bad.append("pattern_border_color")
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
    if "url" in body:
        u = body.get("url")
        u = str(u if u is not None else "").strip()
        if len(u) <= LAYER_URL_MAX:
            k["url"] = u
        else:
            bad.append("url")
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


# ─── VOLETS COMPOSÉS (0.23.45, 2026-10-29) ────────────────────────────────────────────────────
# Deux volets complets (forme, angle, centre, symétrie, répétition, sens, douceur, bordure) combinés,
# rangés SOUS UN NOM, propres à ce mélangeur et persistés dans le document /delays (clé
# `wipe_composites`, comme les sauvegardes de keyer). Combinaisons (mA, mB = part de B de chaque
# volet ; voir _composite_masks pour la formule exacte, bordures comprises) :
#   · union        : max(mA(α), mB(α)) ;
#   · intersection : min(mA(α), mB(α)) ;
#   · décalage     : le second part à `offset` de la durée et RONGE ce que le premier a découvert,
#                    puis se RETIRE : m = clamp(mA(α) − mB(a2)), a2 = depth · (1 − |2u − 1|),
#                    u = clamp((α − offset) / (1 − offset)). a2 vaut 0 à α = 1 (u = 1) : le second
#                    volet ne couvre plus rien et m = mA(1) = 1 — la transition FINIT sur B. Une
#                    soustraction pure (a2 = u) finirait sur A : exclue.
# α = 0 → tout A, α = 1 → tout B, pour toute combinaison (chaque volet le garantit déjà, et les trois
# opérations conservent 0 et 1). REV / N/R du préréglage inversent les DEUX volets.
WIPE_COMPOSITE_OPS = ("union", "intersection", "offset")
WIPE_COMPOSITES_MAX = 32
WIPE_COMPOSITE_NAME_MAX = 32
WIPE_COMPOSITE_DEFAULT = dict(op="union", offset=0.30, depth=0.50)


def _composite_name(v):
    s_ = str(v if v is not None else "").strip()[:WIPE_COMPOSITE_NAME_MAX]
    return s_ or None


def _composite_norm(raw):
    """(volet composé normalisé, champs refusés). Les deux volets passent par _wipe_patch (bornes,
    anciens noms) ; N/R n'appartient qu'au préréglage (alternate forcé à faux)."""
    if not isinstance(raw, dict):
        return None, ["composite"]
    bad = []
    doc = dict(WIPE_COMPOSITE_DEFAULT)
    if "op" in raw:
        op = str(raw.get("op") or "").strip().lower()
        if op in WIPE_COMPOSITE_OPS:
            doc["op"] = op
        else:
            bad.append("op")
    for key, (lo, hi) in (("offset", (0.0, 0.9)), ("depth", (0.05, 1.0))):
        if key in raw:
            try:
                x = float(raw.get(key))
                if x != x:
                    raise ValueError
                doc[key] = max(lo, min(hi, x))
            except Exception:
                bad.append(key)
    for part in ("a", "b"):
        sub = raw.get(part)
        if not isinstance(sub, dict):
            bad.append(part)
            continue
        w = dict(WIPE_DEFAULT)
        bad += [part + "." + x for x in _wipe_patch(w, sub)]
        w["alternate"] = False
        doc[part] = w
    # 0.23.54 : copie LOCALE d'un volet de la bibliothèque COMMUNE de l'installation (id de la
    # sauvegarde côté orchestrateur, app/plugin_library.py). Gardée telle quelle : si la sauvegarde
    # est supprimée là-bas, la copie reste ici et joue — l'interface la marque « retirée ».
    # Un enregistrement depuis l'éditeur (/wipe/composite/save, sans `lib`) en fait un volet local.
    lid = _composite_lib(raw.get("lib"))
    if lid:
        doc["lib"] = lid
    return doc, bad


def _composite_lib(v):
    """Id de sauvegarde de la bibliothèque commune (entier > 0), sinon None."""
    try:
        lid = int(v)
    except (TypeError, ValueError):
        return None
    return lid if 0 < lid < 2 ** 53 else None


# Bibliothèque COMMUNE (0.23.54) : le genre que ce plugin déclare dans control.recall.kinds.
LIBRARY_KIND = "wipe_composite"


def _composite_core(doc):
    """Le volet composé sans son lien de bibliothèque : ce qui se JOUE."""
    return dict((k, v) for k, v in (doc or {{}}).items() if k != "lib")


def _composites_init():
    """Volets composés au démarrage, relus du document persisté (bornés ; un volet illisible garde
    ses défauts, une entrée sans ses deux volets est écartée)."""
    d = CONFIG.get("delays")
    raw = d.get("wipe_composites") if isinstance(d, dict) else None
    out = dict()
    if isinstance(raw, dict):
        for name, doc in raw.items():
            name = _composite_name(name)
            if not name or name in out or len(out) >= WIPE_COMPOSITES_MAX:
                continue
            norm, _bad = _composite_norm(doc)
            if norm is not None and "a" in norm and "b" in norm:
                out[name] = norm
    return out


_wipe_composites = _composites_init()

_KEYERS_INIT = _keyers_init()


def _dsk_default():
    """Réglages d'un DSK : ceux d'un keyer, type web par défaut (sans URL : aucun onglet, rien à
    composer), jamais dans le CLEAN, non lié."""
    d = _keyer_default()
    d.update(type="web", clean=False, tie=False)
    return d


# Champs d'un DSK persistés en plus de ceux d'un keyer : son état d'antenne — l'habillage
# historique survivait au redéploiement (overlay_enabled), le DSK 1 qui le remplace aussi, et les
# autres DSK suivent la même règle. Le TIE n'est pas persisté (comme les cibles de la transition).
_DSK_PERSIST_EXTRA = ("active",)


def _dsks_init():
    """DSK au démarrage : défauts, puis le document PERSISTÉ (clé `dsks` du document des délais) ;
    sans document `dsks` (mélangeur d'avant 0.23.52), le DSK 1 reprend l'HABILLAGE : type web,
    URL = overlay_url, à l'antenne = overlay_enabled — les valeurs mêmes de l'ancien schéma."""
    ds = [_dsk_default() for _ in range(N_DSK)]
    d = CONFIG.get("delays")
    saved = d.get("dsks") if isinstance(d, dict) else None
    if isinstance(saved, list):
        for k, doc in zip(ds, saved):
            if isinstance(doc, dict):
                for key in list(doc):
                    if key == "active":
                        k["active"] = _as_bool(doc[key])
                    elif key in _KEYER_PERSIST or key == "mask":
                        _keyer_patch(k, {{key: doc[key]}})
    elif ds:
        ds[0]["url"] = str(INITIAL_OVERLAY_URL or "").strip()[:LAYER_URL_MAX]
        ds[0]["active"] = bool(INITIAL_OVERLAY_ENABLED)
    # (0.23.53 : le `clean` d'un DSK ne dit plus rien — un DSK est sur une sortie par la liste de
    # celle-ci, cf. SORTIES PROGRAMME ; sur le CLEAN, aucun par défaut.)
    return ds


_DSKS_INIT = _dsks_init()


def _is_web_layer(k):
    """Couche qui occupe un onglet : type web AVEC une URL."""
    return k.get("type") == "web" and bool(k.get("url"))


def _web_count(layers, skip=None):
    return sum(1 for k in layers if k is not skip and _is_web_layer(k))


def _web_limit_init():
    """Au démarrage, plus de couches web que la limite (document persisté sous une limite plus
    haute, configuration modifiée) : les DSK passent d'abord (le DSK 1 est l'habillage), puis les
    keyers dans l'ordre ; les suivantes perdent leur URL — SIGNALÉ, jamais en silence."""
    global CONFIG_WARNING
    n = 0
    dropped = []
    for name, lst in (("dsk", _DSKS_INIT), ("keyer", _KEYERS_INIT)):
        for i, k in enumerate(lst):
            if _is_web_layer(k):
                if n < WEB_LAYERS_MAX:
                    n += 1
                else:
                    k["url"] = ""
                    dropped.append("%s %d" % (name, i + 1))
    if dropped:
        w = ("couches web au-delà de la limite (%d) : URL retirée (%s)" % (WEB_LAYERS_MAX, ", ".join(dropped)))
        CONFIG_WARNING = (CONFIG_WARNING + " ; " + w) if CONFIG_WARNING else w
        log("CONFIG: web layers above the limit (%d), URL dropped on %s" % (WEB_LAYERS_MAX, ", ".join(dropped)),
            "warning")


_web_limit_init()

# ─── SORTIES PROGRAMME (0.23.53, 2026-10-29, décision de l'utilisateur) ─────────────────────
# De 1 à 4 sorties programme (`params.outputs = [{{id, name, shm, tally, layers}}]`), PGM toujours là.
# Toutes partagent le FOND, la TRANSITION et l'ÉTAT des couches ; chacune choisit SES couches
# (keyers et DSK, dans l'ordre de la pile) et dit si elle compte pour le tally. CLEAN n'est plus un
# cas à part : c'est la sortie 1, sans couche par défaut. Le PVW (optionnel, `pvw_base` : None = pas
# de PVW) montre l'état futur d'UNE sortie au choix.
#   · id = slot NMOS (UUID du Sender) : 0 PGM, 1 CLEAN, 2 réservé au PVW, ≥ 3 les nouvelles ;
#   · flux = <hostname>_<slug(nom)> : « PGM » → _pgm, « CLEAN » → _clean, les noms d'avant ;
#   · forme (nombre, noms, PVW oui/non) = REDÉPLOIEMENT (hook control_action « outputs ») ; couches,
#     tally et base du PVW = à chaud (POST /outputs), persistés dans le document des délais.
# ANCIEN SCHÉMA (pas de `outputs`) : PGM (toutes les couches, compte pour le tally) + CLEAN (les
# keyers réglés « clean », aucun DSK) + PVW sur le PGM — exactement ce qui tournait avant.
# Appartenance d'un KEYER à la sortie 1 : son booléen `clean`, inchangé (endpoint /keyer/clean,
# sauvegardes et copier-coller de keyer gardent leur sens) ; tout le reste vit dans `layers`.
# Une couche qui n'est sur AUCUNE sortie au démarrage (keyer ou DSK ajouté au redéploiement) va sur
# la sortie de base du PVW, seule (décision du 29/10).
MAX_OUTPUTS = 4
OUTPUT_ID_PGM, OUTPUT_ID_CLEAN, OUTPUT_ID_PVW = 0, 1, 2
OUTPUT_NAME_MAX = 24
_OUT_VAR = {{OUTPUT_ID_PGM: "out_pgm", OUTPUT_ID_CLEAN: "out_clean"}}


def _output_slug(name, oid):
    """Suffixe du flux d'une sortie : minuscules ASCII, [a-z0-9_], 24 au plus ; vide → out<id>.
    MÊME règle que plugins/mixer/hooks.py:output_slug (le câblage de l'orchestrateur)."""
    import unicodedata
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii").lower()
    s = "".join(c if (c.isalnum() or c == "_") else "_" for c in s)
    while "__" in s:
        s = s.replace("__", "_")
    s = s.strip("_")[:OUTPUT_NAME_MAX].strip("_")
    return s or ("out%d" % oid)


def _layer_key(i):
    """Clé STABLE d'une couche : « k1 »… pour les keyers, « d1 »… pour les DSK (indépendante du
    nombre de keyers, qui décale le numéro de couche des DSK)."""
    return ("k%d" % (i + 1)) if i < N_KEYERS else ("d%d" % (i - N_KEYERS + 1))


def _layer_of_key(key):
    """Numéro de couche d'une clé « k<n> » / « d<n> », ou None (inconnue, hors du nombre déployé)."""
    s = str(key or "").strip().lower()
    try:
        n = int(s[1:])
    except (TypeError, ValueError):
        return None
    if s[:1] == "k" and 1 <= n <= N_KEYERS:
        return n - 1
    if s[:1] == "d" and 1 <= n <= N_DSK:
        return N_KEYERS + n - 1
    return None


def _out_has(o, li, k):
    """La couche `li` (réglages `k`) est-elle sur la sortie `o` ? Sortie 1 : un keyer par son `clean`."""
    if o["id"] == OUTPUT_ID_CLEAN and li < N_KEYERS:
        return bool(k.get("clean"))
    return li in o["layers"]


def _out_set(o, li, on):
    """Met (ou retire) la couche `li` sur la sortie `o` (appelant sous state_lock s'il y en a un)."""
    if o["id"] == OUTPUT_ID_CLEAN and li < N_KEYERS:
        _KEYERS_INIT[li]["clean"] = bool(on)
        return
    if on:
        o["layers"].add(li)
    else:
        o["layers"].discard(li)


def _outputs_init():
    """(sorties, base du PVW) au démarrage : `outputs` du déploiement, ou l'ancien schéma ; puis
    l'état à chaud persisté (`delays.outputs` : couches, tally, base du PVW) pour les mêmes ids."""
    global CONFIG_WARNING
    warn = []
    every = set(range(N_LAYERS))
    raw = CONFIG.get("outputs")
    outs = []
    if not isinstance(raw, list) or not raw:
        outs = [dict(id=OUTPUT_ID_PGM, name="PGM", shm=SHM_OUT_PGM, tally=True, layers=set(every)),
                dict(id=OUTPUT_ID_CLEAN, name="CLEAN", shm=SHM_OUT_CLEAN, tally=False, layers=set())]
        pvw_base = OUTPUT_ID_PGM
    else:
        seen = set()
        for d in raw:
            if not isinstance(d, dict):
                continue
            try:
                oid = int(d.get("id"))
            except (TypeError, ValueError):
                continue
            if oid < 0 or oid == OUTPUT_ID_PVW or oid in seen or len(outs) >= MAX_OUTPUTS:
                warn.append("output %r ignored" % (d.get("id"),))
                continue
            seen.add(oid)
            name = str(d.get("name") or "").strip()[:OUTPUT_NAME_MAX] or ("OUT %d" % oid)
            shm = str(d.get("shm") or "").strip() or (HOSTNAME + "_" + _output_slug(name, oid))
            lay = set()
            for key in d.get("layers") or ():
                li = _layer_of_key(key)
                if li is not None:
                    lay.add(li)
            outs.append(dict(id=oid, name=name, shm=shm, tally=_as_bool(d.get("tally", oid == OUTPUT_ID_PGM)),
                             layers=lay))
        if not any(o["id"] == OUTPUT_ID_PGM for o in outs):
            # PGM toujours présent : sorties illisibles → le PGM de l'ancien schéma, SIGNALÉ.
            warn.append("PGM missing, restored")
            outs.insert(0, dict(id=OUTPUT_ID_PGM, name="PGM", shm=SHM_OUT_PGM, tally=True, layers=set(every)))
            outs = outs[:MAX_OUTPUTS]
        outs.sort(key=lambda o: o["id"])
        if "pvw_base" in CONFIG:
            pvw_base = CONFIG.get("pvw_base")
            pvw_base = None if pvw_base in (None, "", -1, "none") else pvw_base
        else:
            pvw_base = OUTPUT_ID_PGM
        if pvw_base is not None:
            try:
                pvw_base = int(pvw_base)
            except (TypeError, ValueError):
                pvw_base = -1
            if not any(o["id"] == pvw_base for o in outs):
                warn.append("pvw_base %r unknown, PGM used" % (pvw_base,))
                pvw_base = OUTPUT_ID_PGM
        # Un keyer sur la sortie 1 = son booléen `clean` (cf. en-tête) : la liste du déploiement fait foi.
        o1 = next((o for o in outs if o["id"] == OUTPUT_ID_CLEAN), None)
        if o1 is not None:
            for i in range(N_KEYERS):
                _KEYERS_INIT[i]["clean"] = i in o1["layers"]
    for o in outs:
        o["var"] = _OUT_VAR.get(o["id"], "out_o%d" % o["id"])
    # État à chaud persisté (POST /outputs) : il l'emporte, sortie par sortie.
    dl = CONFIG.get("delays")
    doc = dl.get("outputs") if isinstance(dl, dict) else None
    if isinstance(doc, dict):
        byid = dict((o["id"], o) for o in outs)
        for d in doc.get("list") or ():
            o = byid.get(d.get("id")) if isinstance(d, dict) else None
            if o is None:
                continue
            if "tally" in d:
                o["tally"] = _as_bool(d["tally"])
            if isinstance(d.get("layers"), list):
                want = set(li for li in (_layer_of_key(x) for x in d["layers"]) if li is not None)
                for li in every:
                    _out_set(o, li, li in want)
        b = doc.get("pvw_base")
        if pvw_base is not None and b in byid:
            pvw_base = b
    # Couche sur AUCUNE sortie (ajoutée au redéploiement) → la sortie de base du PVW, seule.
    base = next((o for o in outs if o["id"] == (OUTPUT_ID_PGM if pvw_base is None else pvw_base)), outs[0])
    lays = _KEYERS_INIT + _DSKS_INIT
    for li in every:
        if not any(_out_has(o, li, lays[li]) for o in outs):
            _out_set(base, li, True)
    if warn:
        w = "sorties : " + " ; ".join(warn)
        CONFIG_WARNING = (CONFIG_WARNING + " ; " + w) if CONFIG_WARNING else w
        log("CONFIG: outputs: " + "; ".join(warn), "warning")
    return outs, pvw_base


OUTPUTS, _PVW_BASE_INIT = _outputs_init()
HAS_PVW = _PVW_BASE_INIT is not None


def _output(oid):
    return next((o for o in OUTPUTS if o["id"] == oid), None)


def _out_w(o):
    """Writer MXL de la sortie (variable globale : out_pgm, out_clean, out_o<id>)."""
    return globals().get(o["var"])


def _layer_label(i):
    return ("Keyer %d" % (i + 1)) if i < N_KEYERS else ("DSK %d" % (i - N_KEYERS + 1))


def _out_layers_pub(o):
    """Clés des couches de la sortie, dans l'ordre de la pile (appelant sous state_lock)."""
    lays = state["keyers"] + state["dsks"]
    return [_layer_key(li) for li in range(N_LAYERS) if _out_has(o, li, lays[li])]


def _outputs_pub():
    """Sorties publiées (/state.outputs) : id, nom, flux, tally, couches (appelant sous state_lock)."""
    return [dict(id=o["id"], name=o["name"], shm=o["shm"], tally=bool(o["tally"]), layers=_out_layers_pub(o))
            for o in OUTPUTS]


def _outputs_doc():
    """Part à chaud des sorties, persistée avec les délais et relue par _outputs_init."""
    return dict(list=[dict(id=o["id"], tally=bool(o["tally"]), layers=_out_layers_pub(o)) for o in OUTPUTS],
                pvw_base=state["pvw_base"])


def _latch_outputs():
    """Instantané des sorties pour LA trame (appelant sous state_lock) : [(id, couches hors keyers
    de la sortie 1, tally)] — l'appartenance d'un keyer à la sortie 1 est son `clean`, figé au latch."""
    return [dict(id=o["id"], var=o["var"], tally=bool(o["tally"]), layers=frozenset(o["layers"]))
            for o in OUTPUTS]


def _out_plan(seqs):
    """Plan de composition de N sorties qui partagent leurs PRÉFIXES de couches (arbre des
    préfixes) : chaque couche commune à plusieurs sorties est composée UNE fois. `seqs[t]` = rangs,
    dans la pile visible, des couches de la sortie t. Rend des opérations, à rejouer par bande :
      ("bg", t)        le fond dans la sortie t
      ("copy", s, t)   la sortie s, telle qu'elle est à ce point, recopiée dans t
      ("key", j, t)    la couche j composée sur la sortie t
    Une copie part toujours AVANT toute couche posée sur sa source à partir de ce point.
    PGM + CLEAN préfixe (cas courant) : fond, couches communes, une copie, couches du PGM seul — le
    chemin d'avant 0.23.53, opération pour opération."""
    ops = []
    n = len(seqs)
    if not n:
        return ops
    root = max(range(n), key=lambda t: (len(seqs[t]), -t))
    ops.append(("bg", root))

    def node(owner, group, depth):
        branches = dict()
        for t in group:
            if len(seqs[t]) == depth:
                if t != owner:
                    ops.append(("copy", owner, t))
            else:
                branches.setdefault(seqs[t][depth], []).append(t)
        reps = []
        for j, ts in branches.items():
            rep = owner if owner in ts else ts[0]
            if rep != owner:
                ops.append(("copy", owner, rep))
            reps.append((j, rep, ts))
        for j, rep, ts in reps:
            ops.append(("key", j, rep))
            node(rep, ts, depth + 1)
    node(root, list(range(n)), 0)
    return ops

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
    ("/keyer/pattern", ("pattern", "pattern_size", "pattern_x", "pattern_y", "pattern_soft",
                        "pattern_angle", "pattern_repeat", "pattern_border", "pattern_border_color",
                        "pattern_border_soft_in", "pattern_border_soft_out", "pattern_composite")),
    # couche web (0.23.52) : l'URL de la page (le type se choisit par /keyer/type)
    ("/keyer/web", ("url",)),
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


TRANSITION_MODES = ("dissolve", "additif", "wipe")


def _preset_init():
    """Préréglage de transition au démarrage (0.23.44, 2026-10-29) : défauts, puis le document
    PERSISTÉ (clé `preset` du document des délais, cf. GET /delays). Avant 0.23.44 il n'était PAS
    persisté : type de transition et réglages du volet revenaient aux défauts à chaque redéploiement."""
    d = CONFIG.get("delays")
    saved = d.get("preset") if isinstance(d, dict) else None
    saved = saved if isinstance(saved, dict) else dict()
    mode = str(saved.get("mode") or "dissolve").strip().lower()
    comp = _composite_name(saved.get("composite"))
    return {{"mode": mode if mode in TRANSITION_MODES else "dissolve",
            "wipe": _wipe_norm(saved.get("wipe")),
            # volet composé du préréglage (0.23.45) : son nom, None = volet simple
            "composite": comp if comp in _wipe_composites else None}}


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
    # Volet : forme, angle, centre, symétrie, répétition, bordure (0.23.44), sens, N/R (0.23.20 : le
    # sens s'inverse de lui-même à la fin de chaque volet exécuté), douceur — cf. WIPE_DEFAULT.
    "preset": _preset_init(),
    # KEYERS AMONT (0.23.14) : N keyers sur le PGM (pas dans le clean). `keyer` = le keyer 1,
    # MÊME objet que keyers[0] (compatibilité des endpoints et de /state.keyer).
    "keyers":         _KEYERS_INIT,
    "keyer":          _KEYERS_INIT[0],
    # AUTO propre à chaque keyer : {{idx: {{start, duration_ms, from_active, to_active}}}}.
    "key_autos":      {{}},
    # Cibles du prochain take/mix : le fond (BGD) et/ou chaque keyer, ensemble.
    "next_targets": {{"bgd": True, "keyers": [False] * N_KEYERS}},
    "inputs":          [str(CONFIG.get("input_%d" % (i + 1)) or "") for i in range(N_INPUTS)],
    # DSK (0.23.52) : keyers aval, hors CLEAN, au-dessus de tous les keyers. L'ancien habillage
    # (overlay_url / overlay_enabled) EST le DSK 1 de type web : plus de double état.
    "dsks":            _DSKS_INIT,
    # SORTIES (0.23.53) : mêmes objets que OUTPUTS (forme figée au déploiement ; couches, tally et
    # base du PVW réglables à chaud par POST /outputs). pvw_base None = pas de sortie PVW.
    "outputs":         OUTPUTS,
    "pvw_base":        _PVW_BASE_INIT,
    # Dissolve par défaut (ms) — mutable à chaud via POST /config (persisté par le hook
    # control_action puis repoussé ici ; survit ainsi au redeploy).
    "default_dissolve_ms": INITIAL_DISSOLVE_MS,
    # Délai par défaut (⚙, à chaud via /latency) et groupes de délai (/delays, persistés par
    # l'orchestrateur — control.persist).
    "latency_mode": _latency_mode(CONFIG.get("latency_mode")),
    "delays": _norm_delays(CONFIG.get("delays")),
    # Sauvegardes de keyer nommées, propres à ce mélangeur (persistées avec les délais).
    "keyer_presets": _keyer_presets_init(),
    # Volets composés nommés (0.23.45), persistés avec les délais — même objet que _wipe_composites.
    "wipe_composites": _wipe_composites,
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
    snap = {{"mode": p["mode"], "wipe": dict(p["wipe"])}}
    # Volet composé (0.23.45) : sa définition est FIGÉE au départ, comme la géométrie d'un volet.
    doc = state["wipe_composites"].get(p.get("composite") or "")
    if doc:
        snap["composite"] = p["composite"]
        snap["composite_doc"] = dict(doc, a=dict(doc["a"]), b=dict(doc["b"]))
    return snap


def _live_wipe_preset(frozen):
    """Preset d'une transition du fond pour LA trame (appelant sous state_lock) : la géométrie
    figée au départ (`frozen`, cf. _snapshot_preset) + l'adoucissement VIVANT du preset courant —
    un paramètre continu qu'on tourne en regardant le résultat (pupitre, macro) doit agir sur le
    volet EN COURS, pas au suivant (relevé 2026-09-01, corrigé 2026-10-26). Lu une fois par trame :
    toutes les bandes d'une trame en mode tranche voient la même valeur, donc aucune couture.
    `frozen` reste intact — c'est lui que _nr_after consulte à la fin du volet.
    0.23.44 : la bordure (largeur, couleur, douceurs) est vivante comme la douceur (WIPE_LIVE)."""
    if not frozen:
        return frozen
    w = dict(frozen.get("wipe") or dict())
    cur = state["preset"]["wipe"]
    for f in WIPE_LIVE:             # douceur et bordure (0.23.44) : vivantes ; la géométrie, figée
        if f in cur:
            w[f] = cur[f]
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


def _layers():
    """Toutes les couches, dans l'ordre de composition : keyers (1 dessous), puis DSK."""
    return state["keyers"] + state["dsks"]


def _layer(i):
    """Couche i (0-based) : un keyer si i < N_KEYERS, sinon le DSK i − N_KEYERS."""
    return state["keyers"][i] if i < N_KEYERS else state["dsks"][i - N_KEYERS]


def _dsk_items():
    """[(numéro de couche, DSK)] — le numéro de couche indexe les mouvements."""
    return [(N_KEYERS + j, d) for j, d in enumerate(state["dsks"])]


def _keyer_part(i):
    """Mouvement d'une couche (keyer ou DSK) dans une transition : elle passe à l'état inverse de
    l'actuel."""
    a = bool(_layer(i)["active"])
    return {{"from_active": a, "to_active": not a}}


def _targeted_keyers():
    """Couches emportées par la prochaine transition : les keyers ciblés, puis les DSK LIÉS (tie)."""
    return ([i for i, on in enumerate(state["next_targets"]["keyers"]) if on]
            + [L for L, d in _dsk_items() if d.get("tie")])


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
    for i, k in enumerate(_layers()):          # keyers, puis DSK (0.23.52) : ordre de composition
        kl = dict(k)
        kl["mask"] = dict(k["mask"])
        kl["_i"] = i                # numéro du keyer (carte de motif servie pendant un recalcul)
        # couche web (0.23.52) : image de la page figée pour toute la trame (None = pas encore rendue)
        kl["_web"] = _web_frame(i) if kl.get("type") == "web" else None
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
            kl["wmask"] = _wipe_masks(kl["alpha"], dict(direction=au.get("dir", k.get("auto_dir", "horizontal")),
                                                        reverse=bool(au.get("rev", k.get("auto_rev"))),
                                                        softness=float(k.get("auto_soft", 0.10))),
                                      owner=("k", i))[0]
            kl["alpha"] = 1.0
        out.append(kl)
    return out


def _pvw_future(pgm, pvw, manual, trans, keyers_now):
    """Sortie PVW « comme un ATEM » (0.23.29) : ce que sera le PGM APRÈS la prochaine transition.
    0.23.52 : DSK compris (l'habillage est le DSK 1) — un DSK y figure dans son état futur, à
    l'antenne XOR lié (tie), comme un keyer à l'antenne XOR ciblé ; avant, jamais d'habillage. Appelant sous state_lock, au même latch que `keyers_now` (une trame).
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
        # DSK (0.23.52) : même règle — emporté (lié au départ), son état d'arrivée ; sinon son état
        vis += [bool(parts[L]["to_active"]) if L in parts else bool(d["active"]) for L, d in _dsk_items()]
    else:
        nt = state["next_targets"]
        bgd = pvw if nt["bgd"] else pgm
        vis = []
        for i, k in enumerate(state["keyers"]):
            au = state["key_autos"].get(i)
            on = bool(au["to_active"]) if au is not None else bool(k["active"])
            vis.append(on != bool(nt["keyers"][i]))
        # DSK (0.23.52) : visible ⇔ (à l'antenne, ou état d'arrivée de son AUTO) XOR (lié)
        for L, d in _dsk_items():
            au = state["key_autos"].get(L)
            on = bool(au["to_active"]) if au is not None else bool(d["active"])
            vis.append(on != bool(d.get("tie")))
    # 0.23.53 : le PVW montre l'état futur de SA sortie de base — ses couches seulement.
    base = _output(state["pvw_base"]) if state.get("pvw_base") is not None else None
    out = []
    for pos, (kl, v) in enumerate(zip(keyers_now, vis)):
        kp = dict(kl)
        if v and base is not None and not _out_has(base, kl.get("_i", pos), kl):
            v = False
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
#   · SORTIES (0.23.53) : la liste de CHAQUE sortie (`outputs`, par id) — le fond et ses couches
#     visibles ; `pgm` = l'UNION des sorties qui comptent pour le tally (ancien schéma : le PGM seul),
#     `clean` = la sortie 1 (vide sans elle).
_tally_now = dict(pgm=[], clean=[], pvw=[], outputs=dict())


def _tally_inputs(bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, pgm, keyers_now, pvw_bgd, pvw_keyers,
                  outs=None):
    def _vis(stack, out):
        for ks in stack or ():
            if ks.get("type") == "web":
                continue                 # couche web (0.23.52) : une page, aucune entrée du mélangeur
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
    if outs is None:
        with state_lock:
            outs = _latch_outputs()
    per = dict()
    union = set()
    for o in outs:                   # chaque sortie : même fond, SES couches
        s_ = set(on)
        _vis([k for pos, k in enumerate(keyers_now) if _out_has(o, k.get("_i", pos), k)], s_)
        per[str(o["id"])] = sorted(s_)
        if o["tally"]:
            union |= s_
    pv = set([int(pvw_bgd)])
    _vis(pvw_keyers, pv)
    return dict(pgm=sorted(union), clean=per.get(str(OUTPUT_ID_CLEAN), []), pvw=sorted(pv), outputs=per)


# Garde du PVW en IMAGE ENTIÈRE (0.23.30). Mesuré le 28/10 sur mes-mixwf (1 cœur, 1080p, keyers
# linear plein cadre) : ce chemin tient déjà à peine 2 keyers au PGM (20 ms par trame, 46 i/s), et
# les keyers du PVW y coûtent 4,4 ms chacun — 2 au PGM + 2 au PVW tombaient à 28 i/s. Le PVW est un
# retour : il ne doit JAMAIS faire perdre une trame au PGM. Ses keyers ne sont donc composés que si
# le temps déjà passé sur la trame + leur dernier coût mesuré tient dans PVW_BUDGET_FRAC de la
# période ; sinon la trame PVW sort sans eux (fond futur seul), et c'est COMPTÉ
# (`pvw_keyers_skipped` sur :8080).
# 2026-10-29 (0.23.40) : en image entière, le PGM et le CLEAN sont désormais ÉCRITS avant que les
# keyers du PVW ne soient composés — le PVW ne retarde plus la publication du PGM ; il continue de
# prendre sur la période de la boucle, d'où cette garde, conservée. Le mode tranche a son propre
# fil PVW (ci-dessous) : il n'utilise pas cette garde.
PVW_BUDGET_FRAC = 0.85
_pvw_cost_ms = 0.0     # dernier coût mesuré des keyers du PVW (image entière)
_pvw_skips = 0         # trames PVW sorties SANS leurs keyers (les deux chemins) → pvw_keyers_skipped

# ─── PVW SUR SON PROPRE FIL (mode tranche, 0.23.40, 2026-10-29) ──────────────────────────────
# MESURÉ le 2026-10-28 sur le mélangeur Z8 #227 (1080p50, keyer linear fill/key plein cadre) :
# own 5 ms keyer off, 14,2 ms keyer au PGM seul, 17,5 ms au PGM + PVW. Une seule boucle posait,
# bande par bande, CLEAN → PGM → PVW et ne committait la bande k du PGM qu'APRÈS la bande k du
# PVW : le retour coûtait ~3,3 ms au PROGRAMME sur 20 ms de budget, et la sortie SDI répétait 6 à
# 8 images/s. La garde PVW_BUDGET_FRAC en avait exclu ce chemin sur une mesure (1,7 ms/keyer)
# devenue fausse.
# Désormais le fil principal ne compose et ne committe que CLEAN et PGM. Le PVW est un TRAVAIL
# confié à un fil persistant (un seul, jamais un fil par trame) : le fil principal lui passe, à
# l'ouverture de la trame, l'index de sortie, le fond futur et les keyers du PVW déjà figés (même
# latch que le PGM) et une COPIE de l'état de chaque entrée (vues sur les grains, tranches
# valides, budget) — le PVW attend et se replie de son côté sans toucher à l'état du PGM.
# POLITIQUE DE DÉPASSEMENT — le PGM n'attend JAMAIS le PVW :
#  • fil PVW encore occupé quand la trame suivante s'ouvre → la nouvelle trame PVW est mise en
#    attente (une seule place) et sortira SANS SES KEYERS (copie du fond futur, ~1 ms) pour
#    rattraper : comptée dans `pvw_keyers_skipped` (comme en image entière) et `pvw_thread.queued` ;
#  • une trame déjà en attente quand la suivante arrive est ABANDONNÉE au profit de la plus
#    récente : `pvw_thread.dropped` (aucun grain PVW à cet index — libmxl y pose un bouche-trou, que les
#    lecteurs aval écartent et comptent) ;
#  • lecteur d'entrée ou sortie fermé entre-temps (recâblage, flux recréé, saut d'horloge, bus
#    error) → trame en attente abandonnée : `pvw_thread.stale`.
# Un grain PVW OUVERT est toujours committé en entier (complément noir + commit final dans le
# `finally` du fil PVW), comme le contrat le veut pour toute sortie.
# DURÉE DE VIE DES VUES : les vues numpy portent sur la mémoire des lecteurs MXL. Le fil PVW tient
# `_pvw_busy` pendant tout un travail ; toute fermeture ou réouverture d'un lecteur d'entrée
# (ensure_input, réouverture de génération par bobimxl, bus error) et de la sortie PVW
# (_reopen_outputs) prend ce verrou d'abord et incrémente `_pvw_epoch` — un travail plus ancien
# n'est alors plus exécuté. Le fil PVW n'appelle jamais `get`/`get_latest` (qui peuvent rouvrir
# un lecteur sous les pieds du fil principal) : ses replis passent par `get_slice`.
# PRIORITÉ : le fil PVW tourne à nice +PVW_NICE. Sur un conteneur à UN cœur, le parallélisme ne
# gagne rien en débit, mais l'ordonnanceur sert le PGM d'abord et le PVW prend ce qui reste.
PVW_NICE = 10
# MODE (clé `pvw_mode` des paramètres, hors interface, 2026-10-29) :
#  • « thread » = ci-dessus ;
#  • « after » = le PVW est composé par le FIL PRINCIPAL, mais APRÈS le commit final du PGM — le
#    PGM ne l'attend pas non plus, et sans concurrence sur le GIL ; le PVW prend alors sur la
#    période : s'il ne tient plus (temps écoulé + son dernier coût > PVW_BUDGET_FRAC), il sort sans
#    ses keyers, compté comme ci-dessus ;
#  • « auto » (défaut) = « thread » si le conteneur a au moins 2 cœurs, « after » sur UN cœur, où
#    un second fil n'apporte rien et disputerait le GIL au PGM.
# MESURÉ le 2026-10-29 sur mes-mix (dell-1, 50 cœurs partagés, 1080p50, 2 keyers linear plein
# cadre au PGM et au PVW) : 0.23.39 own 17,5 ms ; « thread » PGM publié à 15,0 ms, PVW complet à
# chaque trame ; « after » PGM publié à 11,2 ms, mais PVW sans keyers 485 trames sur 750. Le fil
# PVW n'est pas gratuit : ses parties Python/numpy disputent le GIL au PGM (~4 ms ici avec 2 keyers).
# Les deux variantes partagent _pvw_frame : seul l'endroit où elle tourne change.
try:
    _CPUS = len(os.sched_getaffinity(0))   # cœurs RÉELLEMENT utilisables (cpuset), publiés `cpus`
except Exception:
    _CPUS = os.cpu_count() or 1
_pm = str(CONFIG.get("pvw_mode") or "auto").strip().lower()
PVW_MODE = _pm if _pm in ("thread", "after") else ("thread" if _CPUS > 1 else "after")
_pvw_after_cost_ms = 0.0    # mode « after » : dernier coût mesuré d'une trame PVW AVEC keyers
_pvw_cv = threading.Condition()
_pvw_pending = None         # travail en attente (au plus UN)
_pvw_running = False        # le fil PVW exécute un travail
_pvw_busy = threading.RLock()   # tenu par le fil PVW pendant un travail (cf. DURÉE DE VIE)
_pvw_epoch = 0              # incrémenté SOUS _pvw_busy à chaque fermeture de lecteur/sortie
_pvw_thread = None
_pvw_stats = dict(frames=0, queued=0, dropped=0, stale=0, errors=0)
_pvw_late_slots = set()     # entrées en retard vues par le PVW, reversées au garde-fou n°2
_pvw_dbg = None             # [entrées, valid0, attentes, replis, dormantes] du dernier travail PVW
_pvw_own = deque(maxlen=50)      # ms : travail propre du PVW (attentes exclues)
_pvw_commit_ms = deque(maxlen=50)   # ms : ouverture de trame → commit final du PVW
_pgm_commit_ms = deque(maxlen=50)   # ms : ouverture de trame → commit final du PGM (ce que voit le SDI)


def _pvw_invalidate():
    """À appeler SOUS `_pvw_busy`, avant de fermer un lecteur d'entrée ou une sortie : un travail
    PVW soumis avant n'est plus exécuté (ses vues pourraient porter sur une mémoire libérée)."""
    global _pvw_epoch
    _pvw_epoch += 1


def _guard_reopen(r):
    """La réouverture AUTOMATIQUE de bobimxl (garde de génération, dans get/get_latest) relâche le
    lecteur : elle passe donc par le même verrou que nos propres fermetures."""
    orig = getattr(r, "reopen", None)
    if orig is None:
        return r

    def reopen(*a, **k):
        with _pvw_busy:
            _pvw_invalidate()
            return orig(*a, **k)
    try:
        r.reopen = reopen
    except Exception:
        pass
    return r


def _finish_keyer_autos():
    """AUTO de keyer arrivés à terme → état final (appelant sous state_lock)."""
    for i, au in list(state["key_autos"].items()):
        if _progress(au["start"], au["duration_ms"]) >= 1.0:
            k = _layer(i)               # keyer ou DSK (0.23.52)
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
    return {{"bgd": bool(t["bgd"]), "keyer": bool(t["keyers"][0]), "keyers": list(t["keyers"]),
            # DSK liés (0.23.52) : emportés par la prochaine transition du fond
            "dsks": [bool(d.get("tie")) for d in state["dsks"]]}}


def _dsks_doc():
    """Réglages persistables des DSK, état d'antenne compris (appelant sous state_lock)."""
    out = []
    mv = dict()
    for name in ("transition", "manual_mix"):
        mv.update((state[name] or dict()).get("keyers") or dict())
    mv.update(state["key_autos"])
    for L, d in _dsk_items():
        doc = dict((f, d[f]) for f in _KEYER_PERSIST if f != "clean")
        doc["mask"] = dict(d["mask"])
        for f in _DSK_PERSIST_EXTRA:
            doc[f] = bool(d.get(f))
        # En mouvement (AUTO, transition liée, T-bar) : l'état d'ARRIVÉE — l'instantané persisté
        # est relu une seconde après la commande, souvent avant la fin du mouvement.
        if L in mv and "to_active" in mv[L]:
            doc["active"] = bool(mv[L]["to_active"])
        out.append(doc)
    return out


def _dsks_pub():
    """DSK publiés dans /state (appelant sous state_lock) : réglages, ON AIR (`active`), TIE,
    mouvement en cours — numérotés à partir de 1 (`n`)."""
    mm = state["manual_mix"] or dict()
    tr = state["transition"] or dict()
    out = []
    for j, (L, k) in enumerate(_dsk_items()):
        d = dict(k)
        d["mask"] = dict(k["mask"])
        d["n"] = j + 1
        d["tie"] = bool(k.get("tie"))
        d["moving"] = (L in (mm.get("keyers") or dict()) or L in (tr.get("keyers") or dict())
                       or L in state["key_autos"])
        d["auto"] = L in state["key_autos"]
        d["web"] = _web_status(L) if k.get("type") == "web" else None
        out.append(d)
    return out


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
        d["web"] = _web_status(i) if k.get("type") == "web" else None
        out.append(d)
    return out


def _calc_keyer_alpha(keyer_state, manual, trans):
    """Alpha du keyer 1 (compatibilité : signature d'avant 0.23.14) — cf. _keyer_alpha."""
    return _keyer_alpha(0, keyer_state["active"], manual, trans, None)


# COUCHES WEB (0.23.52) : par numéro de couche, la dernière capture de sa page — (H, W, 4) uint8
# RGBA (aperçus /preview.png) + sa version PRÉ-CONVERTIE (cf. prep_web : y/u/v du fill, `k` = alpha
# de la page au format d'un key plein échelle). Les deux sont remplacés ENSEMBLE par overlay_loop,
# sous _web_lock, jamais modifiés en place : la boucle vidéo fige la référence au latch de la trame.
_web_lock = threading.Lock()
_web_store = dict()     # couche -> {{"rgba", "prep", "url"}}
_web_stats = dict()     # couche -> mesures de capture (cf. overlay_loop), publiées sur :8080 et /state


def _web_frame(i):
    """Image pré-convertie de la couche web i, ou None (page pas encore rendue, en erreur)."""
    with _web_lock:
        e = _web_store.get(i)
    return e["prep"] if e else None


def _web_rgba(i):
    with _web_lock:
        e = _web_store.get(i)
    return e["rgba"] if e else None


def _layer_name(i):
    return ("keyer%d" % (i + 1)) if i < N_KEYERS else ("dsk%d" % (i - N_KEYERS + 1))


def _web_metrics():
    """{{"keyer2" | "dsk1": mesures de capture}} des couches web ouvertes (publié sur :8080)."""
    with _web_lock:
        return dict((_layer_name(i), dict(st)) for i, st in sorted(_web_stats.items()))


def _web_status(i):
    """État de l'onglet de la couche i : chargé, erreur, cadence et coût de capture mesurés."""
    with _web_lock:
        st = _web_stats.get(i)
        return dict(st) if st else dict(loaded=False)

# Métriques
metrics_lock = threading.Lock()
metrics = {{"fps": 0.0, "frame_index": 0, "pgm": 0, "pvw": 1 if N_INPUTS > 1 else 0,
           "inputs_latency_ms": {{}}, "own_latency_ms": None, "inputs_skew_ms": {{}}, "inputs_late": [],
           "sync_ref": 0, "sync_ref_lost": False,
           # MODE TRANCHE : compteurs + garde-fou n°2 (signalement des sources inaptes)
           "slice_mode": SLICE_ON, "slice_late_inputs": {{}}, "advisory": None,
           # Départs de transition retardés jusqu'au champ du volet (0.23.58, cf. _await_wipe_fields)
           "wipe_waits": {{"count": 0, "max_ms": 0.0, "last_ms": 0.0, "fallbacks": 0}},
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
        _ctl_quiet()    # plan de contrôle : au repos de la boucle vidéo (cf. _ctl_quiet)
        with metrics_lock: payload = dict(metrics)
        # Bouches-trous amont ÉCARTÉS (cf. bobimxl.MXL_GRAIN_FLAG_INVALID) : seul témoin du
        # comblement d'un trou par le producteur — cadence, index et statut de lecture restent
        # tous parfaits pendant l'épisode. Calculé à la requête : aucun coût par trame.
        payload["invalid_grains"] = bobimxl.invalid_grains_report()
        # Avance des index ÉMIS sur la grille TAI + trames retenues (cf. bobimxl.index_report) :
        # seul témoin d'une sortie écrite dans le futur — la cadence reste à 50,0 pendant ce temps.
        payload["index_ahead"] = bobimxl.index_report()
        # Version RÉELLEMENT en cours (contrat :8080) : l'orchestrateur la compare à la base après un
        # redéploiement et à chaque tour de surveillance (deploy.confirm_running_version,
        # metrics.note_running_version). Sans elle, un processus resté sur l'ancienne version est
        # invisible — mesuré sur la Z8 le 29/10 (#227, 0.23.41 sous une base à 0.23.42).
        payload["plugin_version"] = PLUGIN_VERSION
        payload["loop_timing"] = _lt_report()      # cf. TEMPS PERDU PAR LA BOUCLE PGM
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass


# ─── Sources NON SYNCHRONES (0.23.57) ───────────────────────
# Retour de Cyril (30/10) : « il faut être alerté dessus pour savoir que ça existe et que ça se
# règle ». Une entrée SANS groupe suit le délai par défaut (⚙, `latency_mode`) ; si sa demande
# mesurée (+ amont) le dépasse, elle passe avec son image précédente — c'est une source non
# synchrone (HF, liaison, traitement extérieur) qu'il faudrait ranger dans un groupe de délai.
# Signalée dans /state (`unsync_inputs`, `unsync_count`) avec une HYSTÉRÉSIS en secondes : elle
# entre après UNSYNC_ON_S secondes de retard consécutives et sort après UNSYNC_OFF_S secondes à
# l'heure ; rangée dans un groupe ou débranchée, elle sort aussitôt. La liste n'est réaffectée qu'à
# une TRANSITION, et elle fait partie de la signature de /events : un changement, une poussée —
# jamais une par image. Mesurée en mode tranche seulement (l'échantillonneur de retards n'existe
# qu'en tranche) : en image entière la liste reste vide.
UNSYNC_ON_S = 3
UNSYNC_OFF_S = 10
_unsync_now = ()            # entrées signalées (numérotées à partir de 1), réaffecté en bloc
_unsync_cnt = dict()        # clé "n" → compteur signé : > 0 secondes en retard, < 0 à l'heure


def _unsync_step(timing, flagged, cnt):
    """Une seconde de plus, depuis `_inputs_timing()` : rend (entrées signalées, compteurs). Fonction
    PURE (aucun global) : la garde la rejoue seconde par seconde."""
    out = set(flagged)
    new_cnt = dict()
    free = set()
    for k, t in timing.items():
        if t.get("group"):
            continue                      # rangée dans un groupe : réglée, rien à signaler
        free.add(int(k))
        need = t.get("need")
        if need is None:                  # hors grille ou pas encore mesurée : on ne conclut rien
            new_cnt[k] = cnt.get(k, 0)
            continue
        late = int(need) + int(t.get("upstream") or 0) > int(t.get("delay") or 0)
        c = cnt.get(k, 0)
        c = (max(c, 0) + 1) if late else (min(c, 0) - 1)
        new_cnt[k] = c
        if late and c >= UNSYNC_ON_S:
            out.add(int(k))
        elif not late and -c >= UNSYNC_OFF_S:
            out.discard(int(k))
    return tuple(sorted(out & free)), new_cnt


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
                "keyers": sorted(i + 1 for i in (tr.get("keyers") or dict()) if i < N_KEYERS),
                # DSK emportés par cette transition (liés au départ), numérotés à partir de 1
                "dsks": sorted(i - N_KEYERS + 1 for i in (tr.get("keyers") or dict()) if i >= N_KEYERS),
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
                               for i, p in (mm.get("keyers") or dict()).items() if i < N_KEYERS),
                "dsks": dict((str(i - N_KEYERS + 1), dict(p))
                             for i, p in (mm.get("keyers") or dict()).items() if i >= N_KEYERS),
            }}),
            "preset": {{
                "mode": state["preset"]["mode"],
                "wipe": dict(state["preset"]["wipe"]),
                "composite": state["preset"].get("composite"),
            }},
            # Volets composés de CE mélangeur (0.23.45), triés.
            "wipe_composites": sorted(state["wipe_composites"]),
            "keyer":         _keyers_pub()[0],   # keyer 1 (compat)
            "keyers":        _keyers_pub(),
            "n_keyers":      N_KEYERS,
            "max_keyers":    MAX_KEYERS,
            # DSK (0.23.52) : keyers aval, hors CLEAN ; le DSK 1 de type web est l'ancien habillage
            "dsks":          _dsks_pub(),
            "n_dsk":         N_DSK,
            "max_dsk":       MAX_DSK,
            # SORTIES PROGRAMME (0.23.53) : id (= slot NMOS), nom, flux, tally, couches (clés k<n>/d<n>,
            # ordre de la pile) ; pvw_base = sortie dont le PVW montre l'état futur (None = pas de PVW).
            "outputs":       _outputs_pub(),
            "pvw_base":      state["pvw_base"],
            "pvw_shm":       SHM_OUT_PVW if HAS_PVW else None,
            # case tally par POSITION de sortie, toujours MAX_OUTPUTS valeurs (faux au-delà) : un
            # chemin d'état de macro reste lisible quel que soit le nombre de sorties déployées
            "outputs_tally": ([bool(o["tally"]) for o in OUTPUTS] + [False] * MAX_OUTPUTS)[:MAX_OUTPUTS],
            # couches, pour les listes vivantes des actions de macro (options_endpoint)
            "layer_choices": [{{"value": _layer_key(i), "label": _layer_label(i)}} for i in range(N_LAYERS)],
            "web_layers":    _web_count(_layers()),
            "web_layers_max": WEB_LAYERS_MAX,
            # navigateur des couches web : running / idle / unavailable (image sans Chromium) / error
            "web_engine":    dict(_web_engine),
            "next_targets":  _targets_pub(),
            # Entrées À L'IMAGE (0.23.35, cf. _tally_inputs) : la source du tally de l'orchestrateur.
            "tally":         dict(_tally_now),
            # Noms des sauvegardes de keyer de CE mélangeur (0.23.16), triés.
            "keyer_presets": sorted(state["keyer_presets"]),
            "inputs":          list(state["inputs"]),
            "n_inputs":        N_INPUTS,
            "max_inputs":      MAX_INPUTS,
            "config_warning":  CONFIG_WARNING,   # écrêtage signalé, jamais muet
            # Habillage historique = DSK 1 (0.23.52) : mêmes clés, lues sur lui (None/False sans DSK).
            "overlay_url":     (state["dsks"][0]["url"] or None) if N_DSK else None,
            "overlay_enabled": bool(state["dsks"][0]["active"]) if N_DSK else False,
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
            # Sources non synchrones (0.23.57) : entrées sans groupe en retard sur le délai par
            # défaut (numérotées à partir de 1), avec hystérésis — cf. _unsync_step.
            "unsync_inputs":  list(_unsync_now),
            "unsync_count":   len(_unsync_now),
            # Entrées MORTES (0.23.64) : flux introuvable ou producteur qui n'écrit plus, rouvertes
            # à intervalle croissant (2 s → 30 s) — numérotées à partir de 1, cf. _input_dead.
            "dead_inputs":    _dead_inputs_pub(),
            "dead_count":     len(_in_dead),
            # Départs de transition retardés jusqu'au champ du volet (0.23.58) : nombre, attente
            # maximale et dernière (ms), départs en fondu faute de champ à la borne.
            "wipe_waits":     dict(_wipe_waits),
            # Image manquante (0.23.60) : mode EFFECTIF (repeat | black) et images recopiées — total
            # et par entrée (numérotées à partir de 1). Hors signature /events : un compteur qui
            # bouge ne pousse rien ; le mode, lui, change par un POST (qui pousse).
            "missing_frame_mode": MISSING_FRAME_MODE,
            "missing_frames_repeated": _mf_total[0],
            "missing_frames_repeated_inputs": dict((str(s_ + 1), n_) for s_, n_ in sorted(_mf_count.items())),
            "missing_frames_flagged": list(_mf_flagged),
            "caps": {{"keyer_types": list(KEYER_TYPES),
                     "upstream_max": UPSTREAM_MAX,
                     "n_keyers": N_KEYERS,
                     "n_dsk": N_DSK,
                     "max_dsk": MAX_DSK,
                     "max_outputs": MAX_OUTPUTS,
                     "output_name_max": OUTPUT_NAME_MAX,
                     "layer_keys": [_layer_key(i) for i in range(N_LAYERS)],
                     "web_layers_max": WEB_LAYERS_MAX,
                     "layer_url_max": LAYER_URL_MAX,
                     "keyer_auto_ms_max": KEYER_AUTO_MS_MAX,
                     "keyer_auto_modes": list(KEYER_AUTO_MODES),
                     "wipe_directions": list(WIPE_DIRECTIONS),
                     "pattern_shapes": list(PATTERN_SHAPES),
                     # Volets (0.23.44) : formes rangées, noms acceptés (anciens = linéaire à angle),
                     # bornes, réglages vivants pendant un volet.
                     "wipe_shapes": list(WIPE_SHAPES),
                     "wipe_legacy_angles": dict(WIPE_LEGACY_ANGLES),
                     "wipe_repeat_max": WIPE_REPEAT_MAX,
                     "wipe_border_max": WIPE_BORDER_MAX,
                     "wipe_live": list(WIPE_LIVE),
                     "wipe_composite_ops": list(WIPE_COMPOSITE_OPS),
                     "wipe_composites_max": WIPE_COMPOSITES_MAX,
                     "wipe_composite_name_max": WIPE_COMPOSITE_NAME_MAX,
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
    # Temps perdu par la boucle PGM (0.23.69) — HORS du verrou d'état, que la boucle prend au latch :
    # même objet que :8080 (cf. TEMPS PERDU PAR LA BOUCLE PGM).
    s["loop_timing"] = _lt_report()
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
# 0.23.48 (2026-10-29) — le TALLY et les OPÉRATIONS sont poussés. Relevé par le relais d'état de
# l'orchestrateur : la liste `tally` est calculée par la boucle vidéo (_tally_now) mais n'entrait pas
# dans la signature — le document poussé juste après une coupe portait la liste de l'image
# PRÉCÉDENTE, et rien ne le repoussait ; pendant un AUTO, rien n'était poussé du tout (l'orchestrateur
# relisait /state à côté). Désormais :
#  · la liste tally (pgm, clean, pvw) fait partie de la signature : tout changement est poussé ;
#  · pendant une opération (AUTO, T-bar, AUTO de keyer), un changement vu par la boucle est poussé
#    au plus EVENTS_MOTION_HZ fois par seconde, et une opération MINUTÉE (AUTO, AUTO de keyer :
#    `elapsed_ms` avance) est repoussée à cette cadence même sans autre changement — le document
#    relayé n'a jamais plus de 1/EVENTS_MOTION_HZ + EVENTS_POLL_S de retard ; une T-bar immobile ne
#    pousse rien. Les commandes (POST) restent poussées sans attente, comme avant.
# Chaque poussée passe par _state_doc_shared (UN calcul pour tous les abonnés) et attend le repos de
# la boucle vidéo (_ctl_quiet) : la contention que la 0.23.46 a supprimée ne revient pas.
EVENTS_POLL_S = 0.04
EVENTS_MOTION_HZ = 10.0
EVENTS_KEEPALIVE_S = 1.0
EVENTS_MAX_CLIENTS = 16

# ★ LE PLAN DE CONTRÔLE NE PREND PAS LE GIL PENDANT LA COMPOSITION (2026-10-29).
# Mesuré sur mes-mixwf (1278, mode tranche, keyer linear, toutes entrées en délai 2) : une coupe ne
# coûte RIEN à la lecture des entrées (lecteurs de toutes les entrées câblées ouverts, bind à
# 0,1 ms avant comme après) — mais chaque changement d'état réveille tous les abonnés /events, et
# chacun recalculait /state (inputs_timing compris : 1,1 à 1,3 ms de Python PUR par abonné) au
# moment où la boucle vidéo compose. Le fil principal, qui relâche le GIL à chaque noyau C et à
# chaque get_slice, attendait de le reprendre : la trame de la coupe passait de 7 à 15-23 ms
# (8 abonnés + un sondeur /state à 0,3 s : p95 20,5 ms, max 22,7 ms, jusqu'à 14,7 ms hors CPU), et
# `pgm_first_slice` montait à 15 ms quand ça tombait pendant les binds. Les répétitions SDI du
# mélangeur Z8 #227 arrivaient précisément dans ces secondes-là.
# Deux règles, sans toucher à la latence d'une commande :
#  · `_vid_idle` : levé quand PGM et CLEAN sont publiés, baissé au réveil sur le créneau. Le travail
#    de LECTURE du plan de contrôle (/state, /events, /preview.png, :8080…) attend ce repos — et
#    pas trop près du réveil suivant (CTL_WAKE_MARGIN_S) — borné à deux périodes : une boucle
#    arrêtée ne bloque personne. Les POST ne l'attendent PAS : leur coût propre est faible et l'état
#    est figé au début de la trame suivante ; les retenir risquerait de reporter une coupe d'une trame.
#  · `/state` est calculé UNE fois par changement et partagé (clé = révision + signature, au plus
#    STATE_DOC_TTL_S) : N abonnés réveillés ensemble ne le calculent plus N fois.
CTL_WAKE_MARGIN_S = 0.004
STATE_DOC_TTL_S = 0.1
_vid_idle = threading.Event()
_vid_idle.set()             # pas de boucle vidéo (démarrage, tests) : rien à protéger


def _ctl_quiet():
    """Attend que la boucle vidéo soit au repos (PGM publié) et loin du prochain réveil ; borné."""
    end = time.monotonic() + 2.0 / FPS
    while True:
        left = end - time.monotonic()
        if left <= 0 or not _vid_idle.wait(left):
            return
        _gp = globals().get("_grid_phase")      # défini plus bas, calibré par la boucle
        if not SLICE_ON or _gp is None:
            return
        _sn = int(round(1e9 / FPS))
        to_wake = (_sn - ((int(bobimxl.now_tai()) - _gp) % _sn)) / 1e9
        if to_wake >= CTL_WAKE_MARGIN_S:
            return
        time.sleep(min(left, to_wake + 0.001))   # laisse passer le réveil ; la boucle rebaisse


_sd_lock = threading.Lock()
_sd_cache = [None, 0.0, None, b""]    # clé, instant (monotone), document, document sérialisé


def _state_doc_shared():
    """(document /state, sérialisé) partagés entre abonnés et sondeurs (cf. plan de contrôle
    ci-dessus). Le document rendu est COMMUN : ne pas le modifier. L'appelant attend lui-même le
    repos (_ctl_quiet) — une seule attente bornée par requête."""
    with _sd_lock:
        key = (_events_rev[0], _state_signature())
        if _sd_cache[0] == key and time.monotonic() - _sd_cache[1] < STATE_DOC_TTL_S:
            return _sd_cache[2], _sd_cache[3]
        doc = _state_doc()
        _sd_cache[:] = [key, time.monotonic(), doc, json.dumps(doc).encode()]
        return doc, _sd_cache[3]


_events_cv = threading.Condition()
_events_rev = [0]
_events_clients = [0]


def _hot_bus(bus, target):
    """BUS CHAUDS (0.23.42, 2026-10-29, décision de l'utilisateur) : pendant une transition, A (PGM)
    et B (PVW) restent chauds, comme sur un grand mélangeur. Appelant sous state_lock.
    Si l'AUTO (`transition`) ou la T-bar (`manual_mix`) porte le fond : A remplace la source
    SORTANTE (`from_idx`, et `flip_to_pvw` — le PVW prendra en fin de transition la sortante
    RÉELLE), B la source ENTRANTE (`to_idx`). Départ, durée, alpha et keyers emportés ne bougent pas.
    Le dict est REMPLACÉ, jamais modifié en place : la boucle vidéo lit l'objet qu'elle a figé au
    latch de la trame (from ET to du même objet) — le changement prend à la trame suivante, entier.
    Cas limite : A = la source déjà sur B (ou l'inverse) est ACCEPTÉ tel quel — les deux côtés
    portent la même entrée, l'image est cette entrée, la transition finit sans effet visible et
    laisse PGM = PVW (même état qu'un CUT avec PGM = PVW ; le TAKE suivant est alors un no-op sur
    le fond, comme avant). Aucun échange implicite des bus.
    Rend True si une partie fond a été modifiée (sinon le bus seul change, à l'appelant)."""
    for name in ("transition", "manual_mix"):
        me = state[name]
        if me is None or not me.get("bgd"):
            continue
        b = dict(me["bgd"])
        if bus == "pgm":
            b["from_idx"] = target
            if "flip_to_pvw" in b:
                b["flip_to_pvw"] = target
        else:
            b["to_idx"] = target
        state[name] = dict(me, bgd=b)
        return True
    return False


def _state_bump():
    with _events_cv:
        _events_rev[0] += 1
        _events_cv.notify_all()


def _state_signature():
    """Ce que la boucle vidéo peut changer SANS requête : bus, transition, keyers à l'antenne,
    AUTO de keyer en cours, mélange manuel — et, depuis la 0.23.48, la liste tally de l'image
    (_tally_now, réaffectée en bloc par la boucle : lue sans verrou)."""
    with state_lock:
        sig = (state["pgm"], state["pvw"], state["transition"] is None,
               tuple(bool(k.get("active")) for k in state["keyers"]),
               tuple(sorted(state["key_autos"])), state["manual_mix"] is None,
               # DSK (0.23.52) : à l'antenne et liés — un CUT de DSK au pupitre est poussé
               tuple((bool(d.get("active")), bool(d.get("tie"))) for d in state["dsks"]))
    t = _tally_now
    # Sources non synchrones (0.23.57) : la liste ne change qu'à une transition → une poussée.
    return sig + ((tuple(t.get("pgm") or ()), tuple(t.get("clean") or ()), tuple(t.get("pvw") or ())),
                  tuple(_unsync_now), tuple(_dead_inputs_pub()))


def _sig_motion(sig):
    """(opération en cours, opération MINUTÉE) d'une signature : AUTO / T-bar / AUTO de keyer ;
    minutée = AUTO ou AUTO de keyer (le temps écoulé avance sans requête)."""
    timed = (not sig[2]) or bool(sig[4])
    return timed or (not sig[5]), timed


# Les routes historiques restent traitées UNE PAR UNE (elles l'étaient par un HTTPServer
# mono-fil, et rien n'a été écrit pour la concurrence) ; seul /events tourne à côté.
_ctl_serial = threading.Lock()


# PRÊT À SERVIR (0.23.51) : les serveurs HTTP démarrent bien avant la fin du module, et leurs
# handlers appellent des noms définis plus bas (ARRIVAL_*, _inputs_timing, _wipe_prefetch…). Une
# requête servie dans la première seconde — le relais d'état se réabonne aussitôt après un
# redéploiement — tombait sur un NameError (banc 1278, 4 redéploiements sur 4 : le flux /events du
# relais coupé à sa première lecture). Elle attend désormais la fin de l'initialisation, au plus
# HTTP_READY_WAIT_S, puis reçoit 503 « starting » si le module n'est toujours pas prêt.
_http_ready = threading.Event()
HTTP_READY_WAIT_S = 5.0


def _preview_layer(path):
    """Couche dont `path` demande l'aperçu web : /preview.png (DSK 1), /dsk/<n>/preview.png,
    /keyer/<n>/preview.png. None = pas un aperçu ; -1 = aperçu d'une couche inexistante (204)."""
    if path == "/preview.png":
        return N_KEYERS if N_DSK else -1
    parts = path.strip("/").split("/")
    if len(parts) != 3 or parts[2] != "preview.png" or parts[0] not in ("dsk", "keyer"):
        return None
    try:
        n = int(parts[1]) - 1
    except ValueError:
        return -1
    if parts[0] == "dsk":
        return N_KEYERS + n if 0 <= n < N_DSK else -1
    return n if 0 <= n < N_KEYERS else -1


# Endpoints d'un DSK (0.23.52) : /dsk/<n>/<action> OU /dsk/<action> avec le numéro dans le corps
# (`dsk`, à partir de 1, ou `idx`, à partir de 0 — charge utile de l'arbre de paramètres des
# macros). Les réglages sont ceux d'un keyer (mêmes champs, mêmes bornes, sans « clean » : un DSK
# n'est jamais dans le CLEAN) ; `web` règle l'URL de sa page.
_DSK_SETTINGS = dict((ep.split("/")[-1], f) for ep, f in _KEYER_SETTINGS.items() if ep != "/keyer/clean")
_DSK_ACTIONS = ("cut", "auto", "tie", "settings") + tuple(_DSK_SETTINGS)


def _dsk_route(path, body):
    """(numéro de DSK 0-based ou None si hors plage, action) d'un chemin /dsk/… ; (None, None) si
    ce n'est pas une route DSK."""
    parts = path.strip("/").split("/")
    if not parts or parts[0] != "dsk" or len(parts) not in (2, 3):
        return None, None
    if len(parts) == 3:
        try:
            j = int(parts[1]) - 1
        except ValueError:
            return None, parts[2]
        act = parts[2]
    else:
        act = parts[1]
        try:
            if body.get("idx") is not None:
                j = int(body.get("idx"))
            elif body.get("dsk") is not None:
                j = int(body.get("dsk")) - 1
            else:
                j = 0
        except Exception:
            return None, act
    return (j if 0 <= j < N_DSK else None), act


def _overlay_apply(body, url_key, en_key):
    """/overlay et /config overlay_* (0.23.52) : l'habillage historique est le DSK 1. L'URL va à
    sa page (limite des couches web comprise), `enabled` est son ON AIR (coupe sèche, mouvement en
    cours annulé). Sans DSK (n_dsk = 0) : 409, rien n'est appliqué. Rend (code, réponse)."""
    if not N_DSK:
        return 409, {{"error": "no_dsk", "code": "no_dsk", "params": {{}}}}
    with state_lock:
        d = state["dsks"][0]
        trial = dict(d)
        if url_key in body:
            u = body.get(url_key)
            trial["url"] = str(u or "").strip()[:LAYER_URL_MAX]
        refus = _web_refusal(d, trial)
        if refus:
            return 409, refus
        d["url"] = trial["url"]
        if en_key in body:
            d["active"] = bool(body.get(en_key))
            _cancel_keyer_motion(N_KEYERS)
    return 200, {{"ok": True}}


def _web_lenient(layer, trial):
    """Collage / rappel (réglages appliqués champ par champ) : une page qui dépasserait la limite
    n'est PAS posée — type et URL gardent leur valeur, et les deux champs sont nommés écartés."""
    if _web_refusal(layer, trial):
        trial["type"] = layer.get("type")
        trial["url"] = layer.get("url", "")
        return ["type", "url"]
    return []


def _web_refusal(layer, trial):
    """Réponse 409 si appliquer `trial` à `layer` dépasserait la limite de couches web, sinon None.
    Appelant sous state_lock."""
    if _is_web_layer(trial) and not _is_web_layer(layer) \
            and _web_count(_layers(), skip=layer) + 1 > WEB_LAYERS_MAX:
        return {{"error": "web_limit", "code": "web_limit", "params": {{"max": WEB_LAYERS_MAX}},
                "message": "web layers limit reached (%d)" % WEB_LAYERS_MAX}}
    return None


class ControlHandler(BaseHTTPRequestHandler):
    def _wait_ready(self):
        if _http_ready.is_set() or _http_ready.wait(HTTP_READY_WAIT_S):
            return True
        self._reply(503, {{"error": "starting"}})
        return False

    def do_GET(self):
        if not self._wait_ready():
            return
        if self.path == "/events":
            self._events()
            return
        _ctl_quiet()
        with _ctl_serial:
            self._do_GET_serial()

    def do_POST(self):
        if not self._wait_ready():
            return
        try:
            with _ctl_serial:
                self._do_POST_serial()
        finally:
            _state_bump()

    def _outputs_post(self, body):
        """POST /outputs (0.23.53) — À CHAUD : couches, tally et base du PVW des sorties existantes.
          {{"output": id, "layer": "k1" | "d1", "on": bool}}   une couche sur une sortie (ou retirée)
          {{"output": id, "layers": ["k1", "d1"]}}             couches de la sortie (remplace la liste)
          {{"output": id, "tally": bool}}                      compte (ou non) pour le tally
          {{"pvw_base": id}}                                   sortie dont le PVW montre l'état futur
          {{"output": id, "name": "Clean"}}                    libellé seul, même flux (0.23.55)
          {{"outputs": [{{"id", "tally", "layers", "name"}}], "pvw_base": id}}   plusieurs d'un coup
        La FORME (sortie inconnue, nom qui change le flux, PVW ajouté ou retiré) ne se règle pas ici : 409
        `redeploy_required` — elle passe par le redéploiement (hook control_action « outputs »).
        Tout ou rien : un champ illisible → 400 qui le nomme, rien n'est appliqué."""
        items = []
        if isinstance(body.get("outputs"), list):
            items.extend(d for d in body["outputs"] if isinstance(d, dict))
        if "output" in body:
            one = dict(id=body.get("output"))
            for f in ("tally", "layers", "layer", "on", "name"):
                if f in body:
                    one[f] = body[f]
            items.append(one)
        if not items and "pvw_base" not in body:
            self._reply(400, {{"error": "output, outputs ou pvw_base requis"}}); return
        bad = []
        with state_lock:
            ops = []
            for d in items:
                try:
                    oid = int(d.get("id"))
                except (TypeError, ValueError):
                    bad.append("id"); continue
                o = _output(oid)
                if o is None:
                    self._reply(409, {{"error": "sortie %s inconnue : ajouter une sortie demande un redéploiement" % oid,
                                      "code": "redeploy_required"}}); return
                if "name" in d:
                    nm = str(d.get("name") or "").strip()[:OUTPUT_NAME_MAX]
                    # 0.23.55 : le LIBELLÉ seul (même flux : même slug) se change à chaud ; un nom qui
                    # changerait le flux demande toujours le redéploiement.
                    if not nm or _output_slug(nm, oid) != _output_slug(o["name"], oid):
                        self._reply(409, {{"error": "renommer une sortie demande un redéploiement",
                                          "code": "redeploy_required"}}); return
                    if nm != o["name"]:
                        ops.append(("name", o, nm))
                if "tally" in d:
                    ops.append(("tally", o, _as_bool(d["tally"])))
                if "layers" in d:
                    if not isinstance(d["layers"], list):
                        bad.append("layers"); continue
                    lis = [_layer_of_key(x) for x in d["layers"]]
                    if any(li is None for li in lis):
                        bad.append("layers"); continue
                    ops.append(("layers", o, set(lis)))
                if "layer" in d:
                    li = _layer_of_key(d.get("layer"))
                    if li is None:
                        bad.append("layer"); continue
                    ops.append(("layer", o, (li, _as_bool(d.get("on", True)))))
            pb = state["pvw_base"]
            if "pvw_base" in body:
                v = body.get("pvw_base")
                none = v in (None, "", -1, "none")
                if none == HAS_PVW:
                    self._reply(409, {{"error": "ajouter ou retirer la sortie PVW demande un redéploiement",
                                      "code": "redeploy_required"}}); return
                if not none:
                    try:
                        pb = int(v)
                    except (TypeError, ValueError):
                        pb = None
                    if pb is None or _output(pb) is None:
                        bad.append("pvw_base")
            if bad:
                self._reply(400, {{"error": "valeur refusée : " + ", ".join(bad), "rejected": bad}}); return
            for kind, o, v in ops:
                if kind == "name":
                    o["name"] = v
                elif kind == "tally":
                    o["tally"] = v
                elif kind == "layers":
                    for li in range(N_LAYERS):
                        _out_set(o, li, li in v)
                else:
                    _out_set(o, v[0], v[1])
            state["pvw_base"] = pb
            out = _outputs_pub()
        log("mixer: outputs updated (%s; PVW on %s)"
            % (", ".join("%d:%s%s" % (o["id"], "+".join(o["layers"]) or "-", "*" if o["tally"] else "")
                         for o in out), pb), "info")
        self._reply(200, {{"ok": True, "outputs": out, "pvw_base": pb}})

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
            rev, sig, last_tx, last_push = None, None, 0.0, 0.0
            while True:
                with _events_cv:
                    if _events_rev[0] == rev:
                        _events_cv.wait(EVENTS_POLL_S)
                    cur_rev = _events_rev[0]
                cur_sig = _state_signature()
                now = time.monotonic()
                in_op, timed = _sig_motion(cur_sig)
                if in_op and cur_rev == rev:
                    # Pendant une opération, ce que voit la boucle (tally, fin d'opération exceptée)
                    # est poussé au plus EVENTS_MOTION_HZ fois par seconde ; une opération minutée,
                    # à cette cadence même sans changement (cf. l'en-tête du flux).
                    due = (cur_sig != sig or timed) and now - last_push >= 1.0 / EVENTS_MOTION_HZ
                else:
                    due = cur_rev != rev or cur_sig != sig
                if due:
                    rev, sig, last_push = cur_rev, cur_sig, now
                    _ctl_quiet()       # au repos de la boucle vidéo (cf. _ctl_quiet)
                    self.wfile.write(b"data: " + _state_doc_shared()[1] + b"\n\n")
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

    def _dsk_post(self, body):
        """Routes d'un DSK (0.23.52) — cf. _dsk_route. CUT = ON AIR en coupe sèche (`active`, ou
        bascule) ; AUTO = fondu ou volet sur sa durée (`auto_ms`, ou `duration_ms`) ; TIE = lié à la
        prochaine transition du fond (`tie`, ou bascule) ; réglages = ceux d'un keyer."""
        j, act = _dsk_route(self.path, body)
        if act not in _DSK_ACTIONS:
            self._reply(404, {{"error": "not found"}}); return
        if j is None:
            self._reply(400, {{"error": "dsk hors plage (1..%d)" % N_DSK, "code": "dsk_range",
                              "params": {{"max": N_DSK}}}}); return
        L = N_KEYERS + j
        if act == "cut":
            with state_lock:
                d = state["dsks"][j]
                d["active"] = _as_bool(body.get("active")) if "active" in body else not d["active"]
                _cancel_keyer_motion(L)
                on = d["active"]
            self._reply(200, {{"ok": True, "dsk": j + 1, "active": on}})
        elif act == "tie":
            with state_lock:
                d = state["dsks"][j]
                key = "tie" if "tie" in body else ("on" if "on" in body else None)
                d["tie"] = _as_bool(body.get(key)) if key else not d.get("tie")
                tie = d["tie"]
            self._reply(200, {{"ok": True, "dsk": j + 1, "tie": tie}})
        elif act == "auto":
            with state_lock:
                d = state["dsks"][j]
                dur = body.get("duration_ms")
                try: dur = d["auto_ms"] if dur is None else max(0, min(KEYER_AUTO_MS_MAX, int(dur)))
                except Exception:
                    self._reply(400, {{"error": "duration_ms invalide"}}); return
                if L in state["key_autos"]:
                    self._reply(200, {{"ok": True, "busy": True, "dsk": j + 1}}); return
                part = _keyer_part(L)
                _cancel_keyer_motion(L)
                if dur <= 0:
                    d["active"] = part["to_active"]
                else:
                    state["key_autos"][L] = dict(part, start=time.time(), duration_ms=dur,
                                                 mode=d.get("auto_mode"), dir=d.get("auto_dir"),
                                                 rev=bool(d.get("auto_rev")))
            self._reply(200, {{"ok": True, "dsk": j + 1, "to_active": part["to_active"], "duration_ms": dur}})
        elif act == "settings":
            doc = body.get("settings")
            if not isinstance(doc, dict):
                self._reply(400, {{"error": "settings (objet) requis"}}); return
            with state_lock:
                d = state["dsks"][j]
                trial = dict(d)
                trial["mask"] = dict(d["mask"])
                skipped = _keyer_patch_lenient(trial, dict((f, v) for f, v in doc.items() if f != "clean"))
                skipped += _web_lenient(d, trial)
                trial["clean"] = False
                d.update(trial)
                out = _keyer_settings(d)
                out.pop("clean", None)
            self._reply(200, {{"ok": True, "dsk": j + 1, "settings": out, "skipped": skipped}})
        else:
            fields = _DSK_SETTINGS[act]
            if act == "mask":
                patch = {{"mask": dict((e, body[e]) for e in _MASK_EDGES if e in body)}}
                if isinstance(body.get("mask"), dict):
                    patch["mask"].update(body["mask"])
                if not patch["mask"]:
                    patch = dict()
            else:
                patch = dict((f, body[f]) for f in fields if f in body)
            if act in ("input", "web", "type") and not patch:
                self._reply(400, {{"error": "valeur requise : " + ", ".join(fields)}}); return
            with state_lock:
                d = state["dsks"][j]
                trial = dict(d)
                trial["mask"] = dict(d["mask"])
                bad = _keyer_patch(trial, patch)
                if bad:
                    self._reply(400, {{"error": "valeur refusée : " + ", ".join(bad), "rejected": bad}}); return
                refus = _web_refusal(d, trial)
                if refus:
                    self._reply(409, refus); return
                trial["clean"] = False
                d.update(trial)
                out = dict((f, d[f]) for f in _KEYER_PERSIST if f != "clean")
                out["mask"] = dict(d["mask"])
            if "auto_dir" in patch:
                _wipe_prefetch(dict(direction=out["auto_dir"]))
            self._reply(200, {{"ok": True, "dsk": j + 1, "settings": out}})

    def _hot_config(self, body):
        """POST /hot_config {{plugin_version, hostname, config}} — RECONFIGURATION À CHAUD
        (`control.hot_config`, 0.23.60). Le mélangeur ne sait prendre à chaud QUE le mode image
        manquante (`missing_frame_mode_effective`) ; il le prend TOUJOURS (version et nom d'hôte
        identiques), puis rend :
          · 200 si c'était le seul changement — l'orchestrateur ne redémarre rien ;
          · 409 `restart_required` (+ `differs`) si autre chose diffère de ce qui tourne, et 409
            `unchanged` si rien n'a changé : un REDÉPLOIEMENT reste un redémarrage, comme avant ;
            le chemin « à chaud seulement » de l'orchestrateur, lui, juge sur
            `missing_frame_mode_effective` de la réponse et ne redémarre jamais.
        Version ou nom d'hôte différents : 409 sans rien appliquer (autre code, autres flux)."""
        cur = {{"missing_frame_mode_effective": MISSING_FRAME_MODE}}
        if str(body.get("plugin_version") or "") != PLUGIN_VERSION:
            self._reply(409, dict(cur, ok=False, code="version_mismatch", running=PLUGIN_VERSION)); return
        if str(body.get("hostname") or "") != HOSTNAME:
            self._reply(409, dict(cur, ok=False, code="hostname_mismatch")); return
        cfg = body.get("config")
        if not isinstance(cfg, dict):
            self._reply(400, dict(cur, ok=False, code="config_missing")); return
        changed = _mf_set_mode(cfg.get("missing_frame_mode_effective"))
        CONFIG["missing_frame_mode_effective"] = MISSING_FRAME_MODE
        CONFIG["missing_frame_mode"] = cfg.get("missing_frame_mode", "")
        def _canon(v):
            try:
                return json.dumps(v, sort_keys=True, default=str)
            except Exception:
                return repr(v)
        differs = sorted(k for k in set(cfg) | set(CONFIG)
                         if k not in _HOT_CONFIG_KEYS and _canon(cfg.get(k)) != _canon(CONFIG.get(k)))
        rep = dict(ok=not differs and changed,
                   applied=(["missing_frame_mode"] if changed else []),
                   missing_frame_mode_effective=MISSING_FRAME_MODE)
        if differs:
            self._reply(409, dict(rep, code="restart_required", differs=differs[:20])); return
        if not changed:
            self._reply(409, dict(rep, code="unchanged")); return
        self._reply(200, rep)

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
        prev_layer = _preview_layer(self.path)
        if prev_layer is not None:
            # Renvoie la dernière capture de la page d'une couche web en PNG ; 204 si rien encore
            # chargé. /preview.png = le DSK 1 (l'habillage historique) ; /dsk/<n>/preview.png et
            # /keyer/<n>/preview.png (0.23.52) = la couche nommée.
            arr = _web_rgba(prev_layer) if prev_layer >= 0 else None
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
            self._reply(200, dict(_state_doc_shared()[0]))
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
                # Préréglage de transition (0.23.44) : même document, relu par _preset_init.
                d["preset"] = {{"mode": state["preset"]["mode"], "wipe": dict(state["preset"]["wipe"]),
                               "composite": state["preset"].get("composite")}}
                # Volets composés (0.23.45) : même document, relu par _composites_init.
                d["wipe_composites"] = dict((n, dict(v, a=dict(v["a"]), b=dict(v["b"])))
                                            for n, v in state["wipe_composites"].items())
                # DSK (0.23.52) : même document, relu par _dsks_init (état d'antenne compris).
                d["dsks"] = _dsks_doc()
                # Sorties (0.23.53) : couches, tally, base du PVW — relu par _outputs_init.
                d["outputs"] = _outputs_doc()
            self._reply(200, d)
        elif self.path == "/wipe/composites":
            # Liste vivante des volets composés (options_endpoint des macros) + leur définition.
            with state_lock:
                names = sorted(state["wipe_composites"])
                comps = dict((n, state["wipe_composites"][n]) for n in names)
                cur = state["preset"].get("composite")
            self._reply(200, {{"names": names, "composites": comps, "current": cur,
                              "ops": list(WIPE_COMPOSITE_OPS), "max": WIPE_COMPOSITES_MAX,
                              "name_max": WIPE_COMPOSITE_NAME_MAX}})
        elif self.path.split("?", 1)[0] == "/library/capture":
            # Bibliothèque COMMUNE (0.23.54, app/plugin_library.py) : l'orchestrateur capture un volet
            # composé LOCAL pour le verser dans la bibliothèque de l'installation. ?kind=&item=<nom>.
            from urllib.parse import urlsplit, parse_qs
            q = parse_qs(urlsplit(self.path).query)
            if (q.get("kind") or [""])[0] != LIBRARY_KIND:
                self._reply(400, {{"error": "bad_kind"}}); return
            name = _composite_name((q.get("item") or [""])[0])
            with state_lock:
                doc = state["wipe_composites"].get(name) if name else None
                payload = _composite_core(doc) if doc else None
                if payload:
                    payload = dict(payload, a=dict(payload["a"]), b=dict(payload["b"]))
            if not payload:
                self._reply(404, {{"error": "unknown_composite", "params": {{"name": name}}}}); return
            # Le résumé porte le volet entier (≈ 600 octets) : la liste de la bibliothèque dessine
            # ses vignettes sans relire chaque sauvegarde.
            self._reply(200, {{"kind": LIBRARY_KIND, "payload": payload, "summary": {{"composite": payload}}}})
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
        if self.path.startswith("/dsk/"):
            self._dsk_post(body)
            return
        if self.path == "/outputs":
            self._outputs_post(body)
            return
        if self.path == "/pgm":
            # Hard cut direct vers l'entrée demandée
            try: target = int(body.get("input"))
            except Exception:
                self._reply(400, {{"error": "input requis"}}); return
            if not (0 <= target < N_INPUTS):
                self._reply(400, {{"error": "input hors plage"}}); return
            # BUS CHAUDS (0.23.42, 2026-10-29) : pendant une transition, la coupe remplace la
            # source SORTANTE et la transition CONTINUE (cf. _hot_bus) ; une transition de keyers
            # seuls continue aussi, le fond est coupé dessous. Hors transition : coupe sèche.
            with state_lock:
                _hot_bus("pgm", target)
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
            #   {{"rename_group": ancien, "to": nouveau}} renomme EN PLACE (délai, entrées et référence
            #                                          suivent) ; nom pris ou inconnu → 409, rien ne bouge
            # Le document est re-borné par _norm_delays : valeur hors bornes → ignorée, jamais écrêtée
            # en silence (la réponse rend le document effectif).
            err = None
            with state_lock:
                d = _norm_delays(state["delays"])
                if "rename_group" in body:
                    d, err = _rename_delay_group(d, body.get("rename_group"), body.get("to"))
                    if err:
                        body = dict()      # refusé : le reste du corps n'est pas appliqué non plus
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
            if err:
                self._reply(409, dict(new, ok=False, error=err)); return
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
            # BUS CHAUDS (0.23.42, 2026-10-29) : pendant une transition du fond, remplace la source
            # ENTRANTE, transition continue (cf. _hot_bus).
            with state_lock:
                _hot_bus("pvw", target)
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
            # 0.23.58 : un AUTO qui joue un volet attend son champ (borné), HORS verrou ; un CUT, non.
            snap = None
            if duration > 0:
                with state_lock:
                    if state["next_targets"]["bgd"] and state["pgm"] != state["pvw"]:
                        snap = _snapshot_preset()
                if snap is not None:
                    snap = _await_wipe_fields(snap)
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
                        _layer(i)["active"] = kp["to_active"]
                    state["transition"] = None
                else:
                    state["transition"] = {{
                        "start": time.time(),
                        "duration_ms": duration,
                        "bgd":      bgd_part,
                        "keyers":   kparts,
                        "preset":   snap if snap is not None else _snapshot_preset(),
                    }}
            self._reply(200, {{"ok": True}})

        elif self.path == "/mix":
            # Crossfade T-bar. Agit selon next_targets : BGD et/ou keyers ciblés.
            try: alpha = float(body.get("alpha"))
            except Exception:
                self._reply(400, {{"error": "alpha invalide"}}); return
            alpha = max(0.0, min(1.0, alpha))
            # 0.23.58 : le DÉPART d'une T-bar (aucun mix en cours) attend le champ du volet, borné,
            # hors verrou ; les positions suivantes du même geste n'attendent rien.
            snap = None
            if 0.001 < alpha < 0.999:
                with state_lock:
                    if (state["manual_mix"] is None and state["next_targets"]["bgd"]
                            and state["pgm"] != state["pvw"]):
                        snap = _snapshot_preset()
                if snap is not None:
                    snap = _await_wipe_fields(snap)
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
                            _layer(i)["active"] = kp["to_active"]
                    else:
                        # Pas de mix actif : commit selon next_targets directement.
                        t = state["next_targets"]
                        if t["bgd"] and state["pgm"] != state["pvw"]:
                            state["pgm"], state["pvw"] = state["pvw"], state["pgm"]
                        for i in _targeted_keyers():
                            _layer(i)["active"] = not _layer(i)["active"]
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
                            "preset": snap if snap is not None else _snapshot_preset(),
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
                refus = _web_refusal(k, trial)      # couche web au-delà de la limite (0.23.52)
                if refus:
                    self._reply(409, refus); return
                k.update(trial)
                doc = dict((f, k[f]) for f in _KEYER_PERSIST)
                doc["mask"] = dict(k["mask"])
            if "auto_dir" in patch:
                _wipe_prefetch(dict(direction=doc["auto_dir"]))     # champ prêt pour l'AUTO
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
                skipped += _web_lenient(k, trial)
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
                skipped += _web_lenient(k, trial)
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

        elif self.path == "/wipe/composite/save":
            # Range un volet composé sous un nom (écrase le même nom) : {{name, op, offset, depth, a, b}}.
            # Valeur illisible → 400 qui la nomme, rien n'est rangé.
            name = _composite_name(body.get("name"))
            if not name:
                self._reply(400, {{"error": "nom requis"}}); return
            doc, bad = _composite_norm(body)
            if bad:
                self._reply(400, {{"error": "valeur refusée : " + ", ".join(bad), "rejected": bad}}); return
            with state_lock:
                lib = state["wipe_composites"]
                if name not in lib and len(lib) >= WIPE_COMPOSITES_MAX:
                    self._reply(400, {{"error": "%d volets composés au plus" % WIPE_COMPOSITES_MAX}}); return
                lib[name] = doc
                names = sorted(lib)
            _wipe_prefetch(doc["a"]); _wipe_prefetch(doc["b"])
            log("mixer: wipe composite %r saved" % name, "info")
            self._reply(200, {{"ok": True, "name": name, "names": names, "composite": doc}})

        elif self.path == "/wipe/composite/delete":
            # Supprime un volet composé ; le préréglage et les motifs qui l'utilisaient reviennent à
            # leur forme simple (rien ne pointe sur un nom disparu).
            name = _composite_name(body.get("name"))
            with state_lock:
                if not name or name not in state["wipe_composites"]:
                    self._reply(404, {{"error": "volet composé inconnu : %s" % name}}); return
                state["wipe_composites"].pop(name)
                if state["preset"].get("composite") == name:
                    state["preset"]["composite"] = None
                for k in _layers():
                    if k.get("pattern_composite") == name:
                        k["pattern_composite"] = ""
                names = sorted(state["wipe_composites"])
            log("mixer: wipe composite %r deleted" % name, "info")
            self._reply(200, {{"ok": True, "names": names}})

        elif self.path == "/library/recall":
            # Bibliothèque COMMUNE (0.23.54) : l'orchestrateur POUSSE un volet de la bibliothèque de
            # l'installation (app/plugin_library.recall) : {{kind, payload, name, library_id, next}}.
            # Il devient une COPIE LOCALE (liée par `lib`) ; `next` (défaut vrai) en fait la prochaine
            # transition. Un volet local de même nom qui n'est PAS cette copie et ne joue pas la même
            # chose → 409 : on n'écrase jamais en silence le travail d'un opérateur.
            if str(body.get("kind") or "") != LIBRARY_KIND:
                self._reply(400, {{"error": "bad_kind"}}); return
            name = _composite_name(body.get("name"))
            if not name:
                self._reply(400, {{"error": "bad_name"}}); return
            doc, bad = _composite_norm(body.get("payload"))
            if doc is None or bad:
                self._reply(400, {{"error": "bad_save", "rejected": bad}}); return
            doc.pop("lib", None)
            lid = _composite_lib(body.get("library_id"))
            if lid:
                doc["lib"] = lid
            nxt = _as_bool(body.get("next", True))
            with state_lock:
                lib = state["wipe_composites"]
                cur = lib.get(name)
                if cur is not None and cur.get("lib") != doc.get("lib") \
                        and _composite_core(cur) != _composite_core(doc):
                    self._reply(409, {{"error": "name_taken_local", "params": {{"name": name}}}}); return
                if cur is None and len(lib) >= WIPE_COMPOSITES_MAX:
                    self._reply(400, {{"error": "local_full", "params": {{"max": WIPE_COMPOSITES_MAX}}}}); return
                lib[name] = doc
                if nxt:
                    state["preset"]["mode"] = "wipe"
                    state["preset"]["composite"] = name
                names = sorted(lib)
            _wipe_prefetch(doc["a"]); _wipe_prefetch(doc["b"])
            log("mixer: library wipe composite %r (save %s) copied%s" % (name, lid, ", next transition" if nxt else ""), "info")
            self._reply(200, {{"ok": True, "name": name, "names": names,
                              "recall": {{"name": name, "library_id": lid, "next": nxt}}}})

        elif self.path == "/wipe/composite/recall":
            # Rappel par nom (macros) : le volet composé devient la PROCHAINE transition (type Wipe).
            name = _composite_name(body.get("name"))
            with state_lock:
                if not name or name not in state["wipe_composites"]:
                    self._reply(404, {{"error": "volet composé inconnu : %s" % name}}); return
                state["preset"]["mode"] = "wipe"
                state["preset"]["composite"] = name
                doc = state["wipe_composites"][name]
            # 0.23.50 : champs des deux volets préparés en tâche de fond DÈS le rappel. Sans ça, un
            # volet composé sorti du cache (6 champs) se calculait DANS la trame de départ du volet :
            # 316-332 ms de PGM figé mesurés sur le banc 1277 (cœur + étoile ×4), 2 sur 2.
            _wipe_prefetch(doc["a"]); _wipe_prefetch(doc["b"])
            self._reply(200, {{"ok": True, "name": name}})

        elif self.path == "/preset":
            # Met à jour le preset de transition (PATCH-like). Mode et GÉOMÉTRIE du volet (forme,
            # angle, centre, symétrie, répétition, sens) s'appliquent au PROCHAIN /take ou /mix ;
            # douceur et bordure agissent aussi sur le volet EN COURS (cf. _live_wipe_preset).
            # 0.23.44 : valeur ILLISIBLE ou hors vocabulaire → 400 qui la nomme, rien n'est appliqué
            # (avant : ignorée en silence). Persisté (control.persist, document /delays).
            with state_lock:
                p = state["preset"]
                bad = []
                mode = p["mode"]
                if "mode" in body:
                    mode = str(body.get("mode") or "").strip().lower()
                    if mode not in TRANSITION_MODES:
                        bad.append("mode")
                trial = dict(p["wipe"])
                comp = p.get("composite")
                if "wipe" in body:
                    if isinstance(body.get("wipe"), dict):
                        bad += _wipe_patch(trial, body["wipe"])
                        if "direction" in body["wipe"]:
                            comp = None        # une forme choisie remplace le volet composé
                    else:
                        bad.append("wipe")
                if "composite" in body:       # 0.23.45 : nom d'un volet composé, vide/None = aucun
                    comp = _composite_name(body.get("composite"))
                    if comp is not None and comp not in state["wipe_composites"]:
                        bad.append("composite")
                if not bad:
                    p["mode"] = mode
                    p["wipe"] = trial
                    p["composite"] = comp
            if bad:
                self._reply(400, {{"error": "valeur refusée : " + ", ".join(bad), "rejected": bad}}); return
            _wipe_prefetch(trial)       # champ de la nouvelle géométrie prêt avant le prochain volet
            if comp:                    # 0.23.50 : volet composé choisi ici — mêmes champs que le rappel
                with state_lock:
                    cdoc = state["wipe_composites"].get(comp)
                if cdoc:
                    _wipe_prefetch(cdoc["a"]); _wipe_prefetch(cdoc["b"])
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
            # Habillage historique = DSK 1 (0.23.52). Change URL et/ou flag enabled ; les deux sont
            # optionnels (PATCH-like). enabled = ON AIR du DSK 1 en coupe sèche.
            code, payload = _overlay_apply(body, "url", "enabled")
            self._reply(code, payload)

        elif self.path == "/log_level":
            # Verbosité À CHAUD (pas de redéploiement) : instruction d'incident. Le niveau
            # PERSISTANT reste le champ `log_level` du config_schema ; celui-ci est volatil.
            ok = set_log_level(body.get("level") or body.get("log_level"))
            self._reply(200 if ok else 400, {{"ok": ok, "log_level": LOG_LEVEL}})

        elif self.path == "/hot_config":
            self._hot_config(body)

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
            code, payload = 200, {{"ok": True}}
            if "overlay_url" in body or "overlay_enabled" in body:
                code, payload = _overlay_apply(body, "overlay_url", "overlay_enabled")
            self._reply(code, payload)

        else:
            self._reply(404, {{"error": "not found"}})

    def log_message(self, *a): pass


threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
    daemon=True).start()
threading.Thread(
    target=lambda: ThreadingHTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
    daemon=True).start()


# ─── Thread Playwright : COUCHES WEB (0.23.52) — un Chromium, un onglet par couche web ───────
# Avant 0.23.52 : UNE page (l'habillage). Désormais chaque couche (keyer ou DSK) de type web AVEC
# une URL a son onglet dans le MÊME navigateur ; la boucle les capture tour à tour, puis dort
# WEB_ROUND_SLEEP_S. Une page est rendue même hors antenne (l'aperçu reste vivant, comme avant).
# Coût mesuré PAR ONGLET (publié sur :8080 `web_layers` et dans /state, couche par couche) :
# capture (screenshot Chromium), décodage PNG, conversion YUV (seulement quand l'image change :
# un habillage statique ne coûte que la capture + une comparaison), cadence de capture effective.
# Le Chromium entier (mémoire résidente, temps CPU) : `web_engine` sur :8080.
WEB_ROUND_SLEEP_S = 0.040
_web_engine = dict(state="idle", pages=0, chromium_rss_mb=None, chromium_cpu_pct=None, error=None)


def prep_web(rgba):
    """Capture RGBA → couche web pré-convertie : fill y/u/v (cf. prep_overlay, mêmes coefficients)
    + `k`, l'alpha de la page au format d'un KEY plein échelle (0 = transparent, max = opaque) —
    la couche se compose alors exactement comme un keyer linear (cf. _key_lut)."""
    p = prep_overlay(rgba)
    a8 = rgba[:, :, 3]
    k = np.ascontiguousarray(a8).astype(_NP_DT)
    if _SCALE != 1:
        k = k * _NP_DT(_SCALE)          # 10/12 bits : 255 → 255·2^(n−8), la pleine échelle du key
    # Rectangle NON transparent de la page (lignes, colonnes ; None = page entièrement transparente) :
    # calculé UNE fois par capture qui change, il évite de composer, à chaque image, les bandes et
    # les colonnes où la page n'a rien — un bandeau de bas d'écran ne coûte plus une image entière.
    rows = np.flatnonzero(a8.any(axis=1))
    cols = np.flatnonzero(a8.any(axis=0)) if rows.size else rows
    bbox = (int(rows[0]), int(rows[-1]), int(cols[0]), int(cols[-1])) if rows.size else None
    return dict(y=p["y"], u=p["u"], v=p["v"], k=k, bbox=bbox)


def _bbox_crop(g, bbox):
    """Géométrie `g` d'une couche (cf. _sl_keyer_geom) réduite aux pixels de SORTIE dont la source
    tombe dans `bbox` (lignes r0..r1, colonnes c0..c1 de l'image source, inclus) — le reste a un
    alpha nul : rien à composer. Bornes arrondies au pas chroma. None si rien ne reste à l'image."""
    if bbox is None:
        return None
    r0, r1, c0, c1 = bbox
    iy = g["iy"]
    t0 = max(g["out_y0"] - g["y0"], int(np.searchsorted(iy, r0, "left")))
    t1 = min(g["out_y1"] - g["y0"], int(np.searchsorted(iy, r1, "right")))
    oy0 = g["y0"] + t0; oy1 = g["y0"] + t1
    oy0 -= oy0 % _CH; oy1 += (-oy1) % _CH
    oy0 = max(oy0, g["out_y0"]); oy1 = min(oy1, g["out_y1"])
    ixv = g["ixv"]
    ja = int(np.searchsorted(ixv, c0, "left")); jb = int(np.searchsorted(ixv, c1, "right"))
    ja -= ja % _CW; jb = min(ixv.size, jb + (-jb) % _CW)
    if oy1 <= oy0 or jb <= ja:
        return None
    g2 = dict(g, out_y0=oy0, out_y1=oy1, out_x0=g["out_x0"] + ja, out_x1=g["out_x0"] + jb,
              ixv=ixv[ja:jb], ix2v=g["ix2v"][ja // _CW:jb // _CW], bw=0)
    g2["xcon"] = _consecutive(g2["ixv"]); g2["x2con"] = _consecutive(g2["ix2v"])
    return g2


def _chromium_usage():
    """(RSS Mo, temps CPU s) cumulés des processus Chromium du conteneur ; None si illisible."""
    rss = 0; cpu = 0.0; seen = False
    tck = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                with open("/proc/%s/cmdline" % pid, "rb") as f:
                    cmd = f.read(256)
                if b"chrom" not in cmd:
                    continue
                with open("/proc/%s/statm" % pid) as f:
                    rss += int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
                with open("/proc/%s/stat" % pid) as f:
                    st = f.read().rsplit(")", 1)[1].split()
                cpu += (int(st[11]) + int(st[12])) / float(tck)
                seen = True
            except Exception:
                continue
    except Exception:
        return None
    return (rss / 1048576.0, cpu) if seen else None


def overlay_loop():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/playwright-browsers")
    try:
        from playwright.sync_api import sync_playwright
        from PIL import Image
    except ImportError as e:
        _web_engine.update(state="unavailable", error=str(e))
        log(f"playwright/PIL indisponible — couches web désactivées ({{e}})", "warning")
        return
    pw = None
    browser = None
    ctx = None
    pages = dict()          # couche -> [page, url, rgba précédente, stats]
    last_usage = None       # (instant, temps CPU Chromium) pour le % CPU
    while True:
        with state_lock:
            # Une page est chargée dès qu'une URL est définie sur une couche web, à l'antenne ou
            # non : l'aperçu reste vivant hors antenne (règle de l'habillage d'avant 0.23.52).
            want = dict((i, k["url"]) for i, k in enumerate(_layers()) if _is_web_layer(k))
        # Pages retirées ou URL changée : onglet fermé, image oubliée (rien de périmé n'est composé).
        for i in list(pages):
            if want.get(i) != pages[i][1]:
                try: pages[i][0].close()
                except Exception: pass
                pages.pop(i, None)
                with _web_lock:
                    _web_store.pop(i, None)
                    _web_stats.pop(i, None)
        if want and browser is None:
            try:
                pw = sync_playwright().start()
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
                _web_engine.update(state="running", error=None)
            except Exception as e:
                _web_engine.update(state="error", error=str(e)[:200])
                log(f"mixer: chromium launch failed: {{e}}", "warning")
                try:
                    if pw: pw.stop()
                except Exception: pass
                pw = browser = ctx = None
                time.sleep(5.0)
                continue
        for i, url in want.items():
            if i in pages:
                continue
            st = dict(loaded=False, url=url, error=None, captures=0, changes=0, fps=None,
                      capture_ms=None, decode_ms=None, convert_ms=None)
            page = None
            try:
                page = ctx.new_page()
                page.goto(url, timeout=10000, wait_until="domcontentloaded")
                page.evaluate(
                    "document.documentElement.style.background='transparent';"
                    "document.body && (document.body.style.background='transparent');"
                )
            except Exception as e:
                log(f"playwright goto erreur ({{url}}): {{e}}", "warning")
                st["error"] = str(e)[:200]
            pages[i] = [page, url, None, st, deque(maxlen=50)]
            with _web_lock:
                _web_stats[i] = dict(st)
        _web_engine["pages"] = sum(1 for p in pages.values() if p[0] is not None)
        for i, ent in list(pages.items()):
            page, url, prev, st, times = ent
            if page is None:
                continue
            try:
                t0 = time.perf_counter()
                png = page.screenshot(omit_background=True, type="png")
                t1 = time.perf_counter()
                img = Image.open(io.BytesIO(png)).convert("RGBA")
                if img.size != (WIDTH, HEIGHT):
                    img = img.resize((WIDTH, HEIGHT))
                rgba = np.asarray(img, dtype=np.uint8)
                t2 = time.perf_counter()
                # Conversion RGB→YUV ICI (hors du chemin critique de la compose) et seulement si
                # l'image a changé : un habillage STATIQUE ne coûte que capture + comparaison.
                if prev is None or not np.array_equal(rgba, prev):
                    prep = prep_web(rgba)
                    ent[2] = rgba
                    with _web_lock:
                        _web_store[i] = dict(rgba=rgba, prep=prep, url=url)
                    st["changes"] += 1
                    st["convert_ms"] = round((time.perf_counter() - t2) * 1e3, 2)
                st["captures"] += 1
                st["loaded"] = True
                st["error"] = None
                st["capture_ms"] = round((t1 - t0) * 1e3, 2)
                st["decode_ms"] = round((t2 - t1) * 1e3, 2)
                times.append(time.monotonic())
                if len(times) > 1 and times[-1] > times[0]:
                    st["fps"] = round((len(times) - 1) / (times[-1] - times[0]), 1)
            except Exception as e:
                st["error"] = str(e)[:200]
                log(f"playwright screenshot erreur: {{e}}", "warning")
                time.sleep(0.5)
            with _web_lock:
                _web_stats[i] = dict(st)
        if not want and browser is not None:
            # Plus aucune couche web : le navigateur est arrêté (aucun coût résiduel).
            try: browser.close()
            except Exception: pass
            try: pw.stop()
            except Exception: pass
            pw = browser = ctx = None
            _web_engine.update(state="idle", pages=0, chromium_rss_mb=None, chromium_cpu_pct=None)
        now = time.monotonic()
        if browser is not None and (last_usage is None or now - last_usage[0] >= 2.0):
            u = _chromium_usage()
            if u is not None:
                if last_usage is not None and last_usage[1] is not None:
                    _web_engine["chromium_cpu_pct"] = round(100.0 * (u[1] - last_usage[1]) / (now - last_usage[0]), 1)
                _web_engine["chromium_rss_mb"] = round(u[0], 1)
                last_usage = (now, u[1])
            else:
                last_usage = (now, None)
        time.sleep(WEB_ROUND_SLEEP_S if want else 0.2)

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
# ENTRÉE MORTE (0.23.64) : flux introuvable, ou anneau dont le producteur n'écrit plus (source
# sans signal). Avant, on rouvrait toutes les 2 s et on le JOURNALISAIT toutes les 2 s : l'entrée
# #4 du mélangeur #227 (Z8, 2026-10-29) remplissait le journal — et chaque réouverture d'un
# lecteur à nous déclenchait une collecte du domaine. Désormais : retentes espacées (2 s, 4 s,
# 8 s… plafond 30 s), remises à zéro dès que le producteur écrit de nouveau ; journal UNIQUEMENT
# aux transitions (mort / revenu). L'état reste VISIBLE : `dead_inputs` dans /state.
REOPEN_BACKOFF_MIN_S = 2.0
REOPEN_BACKOFF_MAX_S = 30.0
_in_dead = {{}}                # idx -> [since (monotonic), delay_s, next_try (monotonic), flow name]


def _input_dead(idx, wanted):
    """Note une tentative ratée sur l'entrée `idx` et programme la suivante (délai doublé,
    plafonné). Rend True à la TRANSITION vivant → mort (le seul moment où l'on journalise)."""
    now = time.monotonic()
    d = _in_dead.get(idx)
    if d is None or d[3] != wanted:
        _in_dead[idx] = [now, REOPEN_BACKOFF_MIN_S, now + REOPEN_BACKOFF_MIN_S, wanted]
        return True
    d[1] = min(d[1] * 2.0, REOPEN_BACKOFF_MAX_S)
    d[2] = now + d[1]
    return False


def _input_backoff(idx, wanted):
    """Vrai tant que l'entrée morte `idx` doit ATTENDRE avant la prochaine tentative (un
    recâblage a déjà effacé l'état dans ensure_input : un nouveau câble repart sans attente)."""
    d = _in_dead.get(idx)
    return d is not None and d[3] == wanted and time.monotonic() < d[2]


def _input_alive(idx):
    """Le producteur de l'entrée `idx` écrit : fin de l'état mort (journalisée une fois)."""
    d = _in_dead.pop(idx, None)
    if d is not None:
        log(f"input #{{idx}}: {{d[3]}} is alive again after {{time.monotonic() - d[0]:.0f}} s",
            "info")


def _dead_inputs_pub():
    """Entrées mortes, numérotées à partir de 1 (lu sans verrou depuis le fil HTTP : copie)."""
    return sorted(i + 1 for i in list(_in_dead))


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
        if vu is not None and idx in _in_dead:
            _input_alive(idx)                  # une écriture VUE avancer, pas une ouverture
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
        _in_dead.pop(idx, None)        # débranché : ni mort ni vivant, rien à annoncer
        if cur is not None:
            with _pvw_busy, _reader_lock:   # jamais sous les pieds du fil PVW (cf. _pvw_busy)
                _pvw_invalidate()
                input_handles[idx] = None
                try: cur[0].close()
                except Exception: pass
        return None
    if idx in _in_dead and _in_dead[idx][3] != wanted:
        _in_dead.pop(idx, None)        # recâblé ailleurs : le nouveau câble repart de zéro
    if cur is not None and cur[1] == wanted:
        if not _input_decroche(idx, cur[0]):
            return cur[0]
        if _input_backoff(idx, wanted):
            return cur[0]              # entrée morte, retente pas encore due : on garde le lecteur
        # Même nom, mais plus personne n'écrit : l'anneau est mort. On le DIT à la transition
        # seulement (pas à chaque retente) et on repart par le chemin de réouverture ci-dessous.
        # Journal en ANGLAIS : convention du dépôt pour tout nouveau log (2026-10-15).
        if _input_dead(idx, wanted):
            log(f"input #{{idx}}: reader STALE on {{wanted}} — producer has not written for "
                f"{{REOPEN_ORPHELIN_S:.0f}} s (no signal, or flow recreated under the same name?); "
                f"reopening with backoff {{REOPEN_BACKOFF_MIN_S:.0f}} s → {{REOPEN_BACKOFF_MAX_S:.0f}} s, "
                f"logged again only when it recovers", "warning")
    elif cur is None and _input_backoff(idx, wanted):
        return None                    # flux introuvable, retente pas encore due
    if cur is not None:
        with _pvw_busy, _reader_lock:   # jamais sous les pieds du fil PVW (cf. _pvw_busy)
            _pvw_invalidate()
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
    # Collecte BORNÉE par l'espacement des retentes (_input_backoff) : un flux réellement absent (slot câblé
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
    try:
        r = _guard_reopen(bobimxl.Reader(inst, wanted))   # lève si le flux n'existe pas encore
        input_handles[idx] = (r, wanted)
        if idx not in _in_dead:        # entrée morte : « revenue » se dit quand elle ÉCRIT
            log(f"input #{{idx}} câblé sur flux MXL {{wanted}}", "info")
        return r
    except Exception as e:
        # ★ NE PLUS ÉCHOUER EN SILENCE. Cet `except` rendait None sans un mot : une entrée noire
        # dont PERSONNE ne pouvait dire pourquoi, ni même qu'une ouverture échouait. On journalise
        # à la TRANSITION vers l'état mort (0.23.64), avec la raison ; les retentes, espacées,
        # se taisent ; le retour se dit dans _input_alive. L'état se lit dans /state (dead_inputs).
        if _input_dead(idx, wanted):
            log(f"input #{{idx}}: cannot open MXL flow {{wanted}} ({{type(e).__name__}}: {{e}}) "
                f"— retrying with backoff {{REOPEN_BACKOFF_MIN_S:.0f}} s → "
                f"{{REOPEN_BACKOFF_MAX_S:.0f}} s, logged again only when it recovers", "warning")
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


# ─── TEMPS PERDU PAR LA BOUCLE PGM (0.23.69, 2026-11-12) ─────────────────────────────────────
# Question posée : le fil PGM perd-il du temps à l'INTERPRÉTEUR (GIL disputé par les fils HTTP,
# /events, le fil PVW ; préemption) au point de justifier une boucle native ? `own_latency_ms` n'y
# répond pas : c'est une moyenne de temps MURAL, où le calcul, les attentes d'entrée et le temps
# passé à attendre le GIL se confondent. Ici, PAR TRAME, sur le fil de la boucle (fil principal) :
#   elapsed = mural de la composition (ts_cycle_start → ts_out, la même fenêtre que own) ;
#   cpu     = temps CPU du fil (time.thread_time_ns) sur la même fenêtre ;
#   wait    = attentes d'entrée MESURÉES (get_slice du bind et des bandes, repli `get` borné) ;
#   lost    = elapsed − cpu − wait : ni calcul ni attente d'entrée → GIL attendu, file d'exécution
#             du noyau (préemption), ou blocage non instrumenté ;
#   runq    = attente en file d'exécution (/proc/thread-self/schedstat, champ 2) : la part
#             préemption de `lost` (approchée : un réveil de get_slice y compte aussi) ;
#   wake    = retard du réveil sur le créneau TAI (mode tranche) — un GIL tenu par un autre fil à
#             l'échéance du sleep retarde le DÉBUT de la trame, hors fenêtre elapsed ;
#   native  = mural passé dans les noyaux C (bobimxl.mvk_*) ;
#   sections= temps PROPRE (enfants exclus, noyaux C exclus) des parties Python/numpy : keyers
#             (chroma, découpe linear/luma, motif, dve), masques de volet (rampes), volets, blend.
# Fenêtre glissante de LT_WINDOW_S secondes, percentiles calculés à la demande (au plus une fois
# par seconde) : publiés `loop_timing` sur :8080 et /state. Les replis numpy SILENCIEUX des noyaux
# (retour False, exception) sont COMPTÉS — fil PGM exact, autres fils approchés (sans verrou).
# Coût : un appel Python enveloppant par noyau et par section, une lecture procfs et un
# thread_time_ns par trame. `loop_timing: false` (paramètre caché) retire tout.
LT_ON = CONFIG.get("loop_timing", True) is not False
LT_WINDOW_S = 60
_LT_N = max(1, LT_WINDOW_S * FPS)
_LT_SECTIONS = ("chroma", "cut", "pattern", "dve", "masks", "wipe", "kwipe", "blend")
_LT_COLS = ("elapsed", "cpu", "wait", "lost", "runq", "wake", "native") + _LT_SECTIONS
_LT_TID = threading.main_thread().ident     # la boucle vidéo tourne sur le fil principal
_lt_ring = np.zeros((len(_LT_COLS), _LT_N), dtype=np.float32)
_lt_pos = [0, 0]                            # prochaine case, trames enregistrées depuis le départ
_lt_acc = dict((k, 0) for k in ("wait", "native", "wake") + _LT_SECTIONS)
_lt_stack = []                              # sections ouvertes : [début ns, ns des enfants]
_lt_over = [0, 0]                           # trames dont elapsed > période ; créneaux sautés
_lt_nat = dict()                            # noyau → [appels, False, exceptions] (fil PGM)
_lt_nat_other = dict()                      # idem, autres fils (fil PVW, outils) — approché
_lt_cache = [0.0, None]
try:
    _LT_SCHED_FD = os.open("/proc/thread-self/schedstat", os.O_RDONLY)   # ouvert PAR le fil principal
except Exception:
    _LT_SCHED_FD = None


def _lt_runq_ns():
    """Attente cumulée en file d'exécution du fil principal (ns), ou None (schedstat absent)."""
    if _LT_SCHED_FD is None:
        return None
    try:
        return int(os.pread(_LT_SCHED_FD, 96, 0).split()[1])
    except Exception:
        return None


def _lt_section(name):
    """Décorateur : temps PROPRE de la fonction sur le fil PGM, ajouté à la section `name` (les
    sections et noyaux appelés dedans sont décomptés). Autres fils, ou LT_ON faux : appel direct."""
    def deco(fn):
        if not LT_ON:
            return fn
        def wrapped(*a, **k):
            if threading.get_ident() != _LT_TID:
                return fn(*a, **k)
            fr = [time.perf_counter_ns(), 0]
            _lt_stack.append(fr)
            try:
                return fn(*a, **k)
            finally:
                _lt_stack.pop()
                tot = time.perf_counter_ns() - fr[0]
                _lt_acc[name] += tot - fr[1]
                if _lt_stack:
                    _lt_stack[-1][1] += tot
        wrapped.__name__ = fn.__name__
        wrapped.__doc__ = fn.__doc__
        wrapped.__wrapped__ = fn
        return wrapped
    return deco


def _lt_native(name, fn):
    """Enveloppe d'un noyau C : compte appels / False / exceptions ; sur le fil PGM, ajoute son mural
    à `native` et le décompte de la section appelante."""
    def wrapped(*a, **k):
        main = threading.get_ident() == _LT_TID
        c = (_lt_nat if main else _lt_nat_other).setdefault(name, [0, 0, 0])
        c[0] += 1
        t0 = time.perf_counter_ns() if main else 0
        try:
            r = fn(*a, **k)
        except Exception:
            c[2] += 1
            raise
        finally:
            if main:
                dt = time.perf_counter_ns() - t0
                _lt_acc["native"] += dt
                if _lt_stack:
                    _lt_stack[-1][1] += dt
        if r is False:
            c[1] += 1
        return r
    wrapped.__name__ = getattr(fn, "__name__", name)
    wrapped.__wrapped__ = fn
    return wrapped


def _lt_wrap_native(mod):
    """Enveloppe les noyaux mvk_* de `mod` (une fois). Rend les noms enveloppés."""
    done = []
    if not LT_ON:
        return done
    for nm in ("mvk_mixf_into", "mvk_mixmap_into", "mvk_place_into", "mvk_blend_into"):
        f = getattr(mod, nm, None)
        if f is None or getattr(f, "__wrapped__", None) is not None:
            continue
        setattr(mod, nm, _lt_native(nm, f))
        done.append(nm)
    return done


_lt_wrap_native(bobimxl)


def _lt_wait(ns):
    """Attente d'entrée mesurée (ns) — comptée pour la trame en cours si on est sur le fil PGM."""
    if ns > 0 and threading.get_ident() == _LT_TID:
        _lt_acc["wait"] += int(ns)


def _lt_begin():
    """Ouverture de la fenêtre de la trame (au même instant que ts_cycle_start). Les cumuls ne sont
    PAS remis à zéro ici mais à la fermeture : un masque calculé au latch (avant le créneau) compte
    pour le tour qui le calcule, et le retard de réveil — mesuré avant — n'est pas effacé."""
    return (time.perf_counter_ns(), time.thread_time_ns(), _lt_runq_ns())


def _lt_end(t):
    """Fermeture de la fenêtre : une ligne de plus dans l'anneau (ms)."""
    if not LT_ON or t is None:
        return None
    w1 = time.perf_counter_ns(); c1 = time.thread_time_ns(); q1 = _lt_runq_ns()
    el = w1 - t[0]; cpu = c1 - t[1]; wt = _lt_acc["wait"]
    rq = (q1 - t[2]) if (q1 is not None and t[2] is not None) else 0
    row = [el, cpu, wt, el - cpu - wt, rq, _lt_acc["wake"], _lt_acc["native"]] + [_lt_acc[s] for s in _LT_SECTIONS]
    i = _lt_pos[0]
    _lt_ring[:, i] = [v / 1e6 for v in row]
    _lt_pos[0] = (i + 1) % _LT_N
    _lt_pos[1] += 1
    for k in _lt_acc:
        _lt_acc[k] = 0
    del _lt_stack[:]
    if el > _SLOT_NS_LT:
        _lt_over[0] += 1
    return row


_SLOT_NS_LT = int(round(1e9 / FPS))


def _lt_pct(v):
    if v.size == 0:
        return None
    p = np.percentile(v, (50.0, 99.0, 99.9))
    return dict(p50=round(float(p[0]), 3), p99=round(float(p[1]), 3),
                p999=round(float(p[2]), 3), max=round(float(v.max()), 3))


def _lt_report(force=False):
    """`loop_timing` publié (:8080, /state) — recalculé au plus une fois par seconde."""
    if not LT_ON:
        return None
    now = time.monotonic()
    if not force and _lt_cache[1] is not None and now - _lt_cache[0] < 1.0:
        return _lt_cache[1]
    n = min(_lt_pos[1], _LT_N)
    d = _lt_ring[:, :n] if _lt_pos[1] < _LT_N else _lt_ring
    cols = dict((c, d[j]) for j, c in enumerate(_LT_COLS))
    nat = dict((k, dict(calls=v[0], false=v[1], errors=v[2])) for k, v in sorted(_lt_nat.items()))
    nat_o = dict((k, dict(calls=v[0], false=v[1], errors=v[2])) for k, v in sorted(_lt_nat_other.items()))
    tot = lambda m, j: sum(v[j] for v in m.values())
    rep = dict(
        window_s=LT_WINDOW_S, window_frames=int(n), frames=int(_lt_pos[1]),
        period_ms=round(1000.0 / FPS, 3), switch_interval_ms=round(sys.getswitchinterval() * 1000.0, 3),
        runq_source=("schedstat" if _LT_SCHED_FD is not None else None),
        ms=dict((c, _lt_pct(cols[c])) for c in _LT_COLS[:7]),
        sections_ms=dict((s, _lt_pct(cols[s])) for s in _LT_SECTIONS),
        overruns_window=int(np.count_nonzero(cols["elapsed"] > 1000.0 / FPS)),
        overruns=int(_lt_over[0]), skipped_slots=int(_lt_over[1]),
        native=dict(pgm=dict(calls=tot(_lt_nat, 0), false=tot(_lt_nat, 1), errors=tot(_lt_nat, 2), by_fn=nat),
                    other=dict(calls=tot(_lt_nat_other, 0), false=tot(_lt_nat_other, 1),
                               errors=tot(_lt_nat_other, 2), by_fn=nat_o)))
    _lt_cache[:] = [now, rep]
    return rep


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

# ─── VOLETS : champs de distance, rampes, couches (0.23.44) — en-tête : cf. WIPE_SHAPES ────────
def _rot_cs(ang):
    """(cos, sin) de l'angle du volet — angles droits EXACTS (cos 90° ≠ 0 en flottant)."""
    q = ang % 360.0
    exact = {{0.0: (1.0, 0.0), 90.0: (0.0, 1.0), 180.0: (-1.0, 0.0), 270.0: (0.0, -1.0)}}
    return exact.get(q, (float(np.cos(np.radians(q))), float(np.sin(np.radians(q)))))


def _rot_coords(cx, cy, c, s, r0, r1):
    """Coordonnées (u, v) des centres de pixel des lignes [r0, r1), relatives à (cx, cy) en pixels,
    tournées de l'angle (c, s) (sens horaire à l'écran, y vers le bas) : u le long de l'axe du
    volet, v en travers."""
    xs = (np.arange(WIDTH, dtype=np.float32) + np.float32(0.5 - cx))[None, :]
    ys = (np.arange(r0, r1, dtype=np.float32) + np.float32(0.5 - cy))[:, None]
    u = xs * np.float32(c) + ys * np.float32(s)
    v = ys * np.float32(c) - xs * np.float32(s)
    return u, v


_heart_tab = []     # [(angles, rayons, rayon mini)] — table polaire du cœur, calculée une fois


def _heart_table():
    """Rayon du bord du cœur (X² + Y² − 1)³ − X²·Y³ = 0 dans chaque direction (Y vers le haut) :
    1024 directions, premier passage par la frontière le long du rayon (forme étoilée autour de
    l'origine). Quelques ms, une fois — remplace le précalcul plein cadre en tâche de fond."""
    if _heart_tab:
        return _heart_tab[0]
    n = 1024
    phi = np.linspace(0.0, 2.0 * np.pi, n, endpoint=False)
    t = np.linspace(0.0005, 2.0, 4000)[:, None]
    X = t * np.cos(phi)[None, :]
    Y = t * np.sin(phi)[None, :]
    q = X * X + Y * Y - 1.0
    f = q * q * q - X * X * Y * Y * Y
    i = np.argmax(f >= 0.0, axis=0)
    R = t[i, 0]
    # prolongée d'un tour de chaque côté : np.interp sans couture en 0 / 2π
    ph = np.concatenate([phi - 2.0 * np.pi, phi, phi + 2.0 * np.pi])
    rr = np.concatenate([R, R, R])
    _heart_tab.append((ph, rr, float(R.min())))
    return _heart_tab[0]


def _shape_dist(shape, u, v, hx, hy, sym):
    """Distance NORMALISÉE d'une forme centrée (1 = la forme couvre le rectangle hx × hy, ou le
    cercle de la demi-diagonale pour les formes rondes). Pas le linéaire (cf. appelants)."""
    rd = float(np.hypot(hx, hy))
    if shape == "box":
        return np.maximum(np.abs(u) * np.float32(1.0 / hx), np.abs(v) * np.float32(1.0 / hy))
    if shape == "diamond":
        return (np.abs(u) * np.float32(0.5 / hx) + np.abs(v) * np.float32(0.5 / hy))
    r = np.sqrt(u * u + v * v)
    if shape == "iris":
        return r * np.float32(1.0 / rd)
    if shape == "clock":
        # Balayage angulaire à partir du HAUT, sens horaire (symétrie : les deux sens à la fois).
        ph = np.arctan2(u, -v)
        if sym:
            return np.abs(ph) * np.float32(1.0 / np.pi)
        return np.mod(ph, np.float32(2.0 * np.pi)) * np.float32(0.5 / np.pi)
    if shape == "star":
        ph = np.mod(np.arctan2(u, -v), np.float32(2.0 * np.pi / 5.0))
        ph = np.minimum(ph, np.float32(2.0 * np.pi / 5.0) - ph)
        k = _STAR_K; c0 = float(np.cos(np.pi / 5.0)); s0 = float(np.sin(np.pi / 5.0))
        rho = np.float32(k * s0) / (np.float32(k * s0) * np.cos(ph) + np.float32(1.0 - k * c0) * np.sin(ph))
        return r * np.float32(k / rd) / rho
    # heart
    ph, rr, rmin = _heart_table()
    phi = np.mod(np.arctan2(-v, u), np.float32(2.0 * np.pi))
    R = np.interp(phi.ravel(), ph, rr).astype(np.float32).reshape(phi.shape)
    return r * np.float32(rmin / rd) / R


def _shape_px(shape, hx, hy):
    """Un pixel, en unités de d (douceur minimale : un bord net reste anti-crénelé)."""
    if shape in ("box", "diamond"):
        return 1.0 / min(hx, hy)
    if shape == "clock":
        return 0.002
    return 1.0 / float(np.hypot(hx, hy))


_wipe_fields = dict()
_wipe_fields_lock = threading.Lock()
_WIPE_FIELDS_MAX = 6
_FIELD_ROWS = 64        # calcul par paquets de lignes : jamais un long calcul d'un seul tenant
_field_cost_ms = dict()  # forme → durée MESURÉE du dernier calcul de champ (cf. _pattern_maps)


def _field_rows(shape, cx, cy, c, s, sym, rep, umin, r0, r1, pattern=False):
    """Distance normalisée des lignes [r0, r1) (avant recalage du minimum) et un pixel en unités de d.
    `pattern` (keyer motif) : le linéaire est une BARRE centrée sur le pivot (d = 1 à la demi-étendue)."""
    u, v = _rot_coords(cx, cy, c, s, r0, r1)
    hx = WIDTH / 2.0; hy = HEIGHT / 2.0
    if shape == "linear":
        lh = hx * abs(c) + hy * abs(s)              # demi-étendue de la projection, image centrée
        P = 2.0 * lh / rep
        if sym or pattern:
            # Porte coulissante : B part de l'axe (pivot = centre) vers les deux bords ; en
            # répétition, chaque bande s'ouvre depuis son milieu.
            if rep > 1:
                # bandes calées sur le cadre quand le centre est au milieu (N pair : demi-période)
                ph = (P / 2.0) if rep % 2 == 0 else 0.0
                u = np.mod(u - np.float32(ph) + np.float32(P / 2.0), np.float32(P)) - np.float32(P / 2.0)
            d = np.abs(u)
        else:
            d = u - np.float32(umin)
            if rep > 1:
                d = np.mod(d, np.float32(P))        # stores : N bandes balayées ensemble
        k = (rep if pattern else 1) / lh            # motif : la taille 1 remplit chaque bande
        return d * np.float32(k), k
    if rep > 1:
        # Damier : N × N cellules au format de l'image, grille tournée avec la forme et déplacée
        # avec le centre ; centre au milieu → cellules calées sur le cadre (N pair : demi-cellule).
        cw = WIDTH / float(rep); chh = HEIGHT / float(rep)
        pu_ = (cw / 2.0) if rep % 2 == 0 else 0.0
        pv_ = (chh / 2.0) if rep % 2 == 0 else 0.0
        u = np.mod(u - np.float32(pu_) + np.float32(cw / 2.0), np.float32(cw)) - np.float32(cw / 2.0)
        v = np.mod(v - np.float32(pv_) + np.float32(chh / 2.0), np.float32(chh)) - np.float32(chh / 2.0)
        hx = cw / 2.0; hy = chh / 2.0
    return _shape_dist(shape, u, v, hx, hy, sym), _shape_px(shape, hx, hy)


def _wipe_field(geom, pattern=False):
    """(d float32 plein cadre, M, un pixel) d'une géométrie (forme, angle, centre, symétrie,
    répétition) — mis en cache ; le SENS n'en fait pas partie (cf. _ramp_into), N/R ne recalcule
    rien. Calcul par paquets de lignes : en tâche de fond, il rend la main entre deux paquets.
    `pattern` (keyer motif) : distance au centre telle quelle (la TAILLE s'y mesure), pas recalée
    sur le cadre."""
    ck = (geom, bool(pattern))
    with _wipe_fields_lock:
        got = _wipe_fields.get(ck)
    if got is not None:
        return got
    shape, ang, cx, cy, sym, rep = geom
    c, s = _rot_cs(ang)
    cxp = cx * WIDTH; cyp = cy * HEIGHT
    # minimum de la projection : à un coin (u est affine en x, y)
    umin = min((x + 0.5 - cxp) * c + (y + 0.5 - cyp) * s for x in (0, WIDTH - 1) for y in (0, HEIGHT - 1))
    t0 = time.monotonic()
    d = np.empty((HEIGHT, WIDTH), dtype=np.float32)
    px = 1.0
    for r0 in range(0, HEIGHT, _FIELD_ROWS):
        r1 = min(HEIGHT, r0 + _FIELD_ROWS)
        blk, px = _field_rows(shape, cxp, cyp, c, s, sym, rep, umin, r0, r1, pattern)
        d[r0:r1] = blk
    if not pattern:
        d -= np.float32(d.min())                # centre hors cadre : la course commence au cadre
    got = (d, max(float(d.max()), 1e-6), px, (None if pattern else _field_tiles(d)))
    _field_cost_ms[shape] = (time.monotonic() - t0) * 1000.0
    with _wipe_fields_lock:
        if len(_wipe_fields) >= _WIPE_FIELDS_MAX:
            _wipe_fields.pop(next(iter(_wipe_fields)))
        _wipe_fields[ck] = got
    return got


_wipe_inflight = set()      # géométries en cours de calcul en tâche de fond (un seul fil par géométrie)


def _wipe_prefetch_run(geom):
    try:
        _wipe_field(geom)
    finally:
        with _wipe_fields_lock:
            _wipe_inflight.discard(geom)


def _wipe_prefetch(w):
    """Précalcule en tâche de fond la géométrie d'un réglage (appelé quand un réglage de volet
    change) : le départ du volet trouve son champ prêt au lieu de le calculer dans la trame.
    0.23.58 : un seul fil par géométrie (un angle tourné vite n'empile plus les calculs)."""
    geom = _wipe_geom(w)
    with _wipe_fields_lock:
        if (geom, False) in _wipe_fields or geom in _wipe_inflight:
            return
        _wipe_inflight.add(geom)
    threading.Thread(target=_wipe_prefetch_run, args=(geom,), daemon=True).start()


# ─── DÉPART D'UNE TRANSITION RETARDÉ JUSQU'AU CHAMP PRÊT (0.23.58, décision de Cyril du 30/10) ──
# Mesuré la nuit du 29 au 30/10 (banc 1278) : une forme de volet changée puis jouée dans les 0,2 s
# (pupitre, macro « préréglage + AUTO ») gelait le PGM de 75 à 210 ms — le préchauffage n'avait pas
# fini et la trame de départ calculait le champ elle-même. Règle retenue : le DÉPART attend que le
# champ soit prêt, au plus WIPE_READY_WAIT_S. L'attente se fait dans le fil de la requête, SANS
# verrou d'état : la boucle vidéo continue de sortir le PGM tel quel (ni gel, ni répétition).
# Borne atteinte sans champ : la transition part quand même, EN FONDU, et l'incident est compté.
# Vaut pour l'AUTO (/take) et le départ d'une T-bar (/mix) ; un CUT (durée 0) n'a pas de champ à
# attendre. Compteurs publiés dans /state et sur :8080 (`wipe_waits`) : le décalage réel se lit.
WIPE_READY_WAIT_S = 0.3
WIPE_READY_POLL_S = 0.005
_wipe_waits = dict(count=0, max_ms=0.0, last_ms=0.0, fallbacks=0)
_wipe_waits_lock = threading.Lock()


def _preset_geoms(snap):
    """Géométries de champ qu'exige un préréglage figé pour la transition du FOND : aucune en fondu
    ou en additif ; les deux volets d'un composé ; sinon celle du volet simple."""
    if not snap or snap.get("mode") != "wipe":
        return []
    comp = snap.get("composite_doc")
    if comp:
        return [_wipe_geom(comp["a"]), _wipe_geom(comp["b"])]
    return [_wipe_geom(snap.get("wipe") or dict())]


def _fields_ready(geoms):
    with _wipe_fields_lock:
        return all((g, False) in _wipe_fields for g in geoms)


def _await_wipe_fields(snap):
    """Attend (au plus WIPE_READY_WAIT_S) que les champs de `snap` soient prêts. À appeler HORS de
    state_lock. Rend le préréglage à jouer : `snap` tel quel si prêt, sinon le même en FONDU."""
    geoms = _preset_geoms(snap)
    if not geoms or _fields_ready(geoms):
        return snap
    comp = snap.get("composite_doc")
    for w in ([comp["a"], comp["b"]] if comp else [snap.get("wipe") or dict()]):
        _wipe_prefetch(w)                       # déjà en cours : ne relance rien
    t0 = time.monotonic()
    deadline = t0 + WIPE_READY_WAIT_S
    ready = False
    while time.monotonic() < deadline:
        time.sleep(WIPE_READY_POLL_S)
        if _fields_ready(geoms):
            ready = True
            break
    ms = round((time.monotonic() - t0) * 1000.0, 1)
    with _wipe_waits_lock:
        _wipe_waits["count"] += 1
        _wipe_waits["last_ms"] = ms
        _wipe_waits["max_ms"] = max(_wipe_waits["max_ms"], ms)
        if not ready:
            _wipe_waits["fallbacks"] += 1
        pub = dict(_wipe_waits)
    with metrics_lock:
        metrics["wipe_waits"] = pub
    if ready:
        log("mixer: transition start delayed %.1f ms until the wipe field was ready" % ms, "info")
        return snap
    log("mixer: wipe field not ready after %.0f ms — transition starts as a DISSOLVE" % ms, "warning")
    out = dict(snap, mode="dissolve")
    out.pop("composite", None)
    out.pop("composite_doc", None)
    return out


# TUILES (mesuré le 2026-10-29 sur dell-1, 1080p : une rampe numpy plein cadre = 3,8 ms, dont 1,6 ms
# pour le seul clip ; le mélange C qui suit coûte 1 ms). Hors de la bande du bord, la rampe vaut 0
# ou 1 : par géométrie, on garde le min et le max de d par tuile de 8 × 64 pixels ; à chaque trame,
# une tuile entièrement d'un côté du bord est REMPLIE (0 ou 1), seules celles que le bord traverse
# sont calculées. Classement CONSERVATEUR (marge d'un millième de douceur) : une tuile remplie vaut
# exactement ce que donnerait le calcul — mêmes octets que le plein cadre.
_TILE_H = 8
_TILE_W = 64
# Classe de chaque tuile au dernier remplissage d'un tampon (id du tampon → int8 : 0, 1, 2 = calculée) :
# une tuile restée 0 ou 1 n'est pas réécrite — le bord ne traverse que quelques tuiles par trame.
_tile_cls = dict()


def _field_tiles(d):
    """(d vu en tuiles, min, max) par tuile, ou None si l'image ne se découpe pas en tuiles."""
    if HEIGHT % _TILE_H or WIDTH % _TILE_W:
        return None
    d4 = d.reshape(HEIGHT // _TILE_H, _TILE_H, WIDTH // _TILE_W, _TILE_W)
    return (d4, d4.min(axis=(1, 3)), d4.max(axis=(1, 3)))


def _ramp_calc(out, d, k, s, rev):
    if rev:
        np.subtract(d, np.float32(k), out=out)
    else:
        np.subtract(np.float32(k), d, out=out)
    np.multiply(out, np.float32(1.0 / s), out=out)
    np.clip(out, 0.0, 1.0, out=out)
    return out


def _ramp_cls(tmin, tmax, k, s, rev):
    """Classe de chaque tuile d'une rampe de seuil k, largeur s (cf. _ramp_into) : int8 0 ou 1 = la
    tuile entière vaut cette constante (classement CONSERVATEUR, marge d'un millième de douceur :
    le calcul y donnerait exactement la même valeur), 2 = le bord la traverse, à calculer."""
    g = 1e-3 * s
    if rev:     # part de B = clip((d − k) / s) : 1 si d ≥ k + s, 0 si d ≤ k
        one = tmin >= k + s + g
        zero = tmax <= k - g
    else:       # part de B = clip((k − d) / s) : 1 si d ≤ k − s, 0 si d ≥ k
        one = tmax <= k - s - g
        zero = tmin >= k + g
    return np.where(one, 1, np.where(zero, 0, 2)).astype(np.int8)


def _tile_fill(out, cls, keep):
    """Remplit les tuiles CONSTANTES (classe 0 ou 1) de `out` ; les tuiles de classe 2 sont laissées
    à l'appelant. `keep` (tampon PERSISTANT) : seules les tuiles dont la classe a changé depuis le
    dernier remplissage de CE tampon sont réécrites. Beaucoup de changements (début d'un AUTO : la
    trame précédente du tampon était la fin du dernier, toute à 1) → une écriture CONTIGUË du cadre
    entier plutôt qu'une dispersion tuile par tuile (mesuré le 2026-10-29 sur dell-1, 1080p : la
    dispersion de toutes les tuiles coûte plusieurs millisecondes, l'écriture contiguë moins d'une)."""
    o4 = out.reshape(HEIGHT // _TILE_H, _TILE_H, WIDTH // _TILE_W, _TILE_W)
    prev = _tile_cls.get(id(out)) if keep else None
    if prev is None or prev.shape != cls.shape:
        o4[...] = (cls == 1).astype(np.float32)[:, None, :, None]
    else:
        ch = (cls != prev) & (cls != 2)
        if 4 * int(np.count_nonzero(ch)) > ch.size:
            o4[...] = (cls == 1).astype(np.float32)[:, None, :, None]
        else:
            ic, jc = np.nonzero((cls != prev) & (cls != 2))
            if ic.size:
                o4[ic, :, jc, :] = cls[ic, jc].astype(np.float32)[:, None, None]
    if keep:
        _tile_cls[id(out)] = cls
    else:
        _tile_cls.pop(id(out), None)
    return o4


def _ramp_k(e, s, M, rev):
    """Seuil de la rampe de bord e, largeur s (cf. _ramp_into) — REV : mesuré sur M − d."""
    return (M - e - 0.5 * s) if rev else (e + 0.5 * s)


def _ramp_into(out, d, e, s, M, rev, tiles=None, keep=False):
    """out ← part de B, EN PLACE : rampe de largeur s centrée sur le bord e, 1 côté d < e − s/2,
    0 côté d > e + s/2. REV : la même sur M − d (le volet part de l'autre bout, une forme se
    referme), sans toucher au champ en cache. `tiles` (cf. _field_tiles) : seules les tuiles que
    le bord traverse sont calculées ; `keep` (tampon PERSISTANT, cf. _wipe_buf) : les tuiles restées
    à 0 ou 1 depuis son dernier remplissage ne sont pas réécrites. Jamais sur un tampon neuf : son
    id peut être celui d'un tableau libéré."""
    k = _ramp_k(e, s, M, rev)
    if tiles is None:
        _tile_cls.pop(id(out), None)
        return _ramp_calc(out, d, k, s, rev)
    d4, tmin, tmax = tiles
    cls = _ramp_cls(tmin, tmax, k, s, rev)
    o4 = _tile_fill(out, cls, keep)
    ia, ja = np.nonzero(cls == 2)
    if ia.size:
        sub = d4[ia, :, ja, :]
        o4[ia, :, ja, :] = _ramp_calc(sub, sub, k, s, rev)
    return out


# Tampons de masques par PROPRIÉTAIRE (fond, keyer i), deux en alternance : le masque d'une trame
# reste intact pendant que la suivante calcule le sien. Sans propriétaire : allocation neuve.
_wipe_bufs = dict()


def _wipe_buf(owner, slot):
    if owner is None:
        return np.empty((HEIGHT, WIDTH), dtype=np.float32)
    key = (owner, slot)
    b = _wipe_bufs.get(key)
    if b is None:
        b = _wipe_bufs[key] = [0, np.empty((HEIGHT, WIDTH), dtype=np.float32),
                               np.empty((HEIGHT, WIDTH), dtype=np.float32)]
    b[0] ^= 1
    return b[1 + b[0]]


_color_planes_cache = dict()


def _hex_yuv_code(hexstr):
    """#RRGGBB → codes (Y, U, V) en plage légale, à la profondeur du conteneur, matrice déclarée
    (comme la couleur de clé chroma, cf. KEY_COLORIMETRY)."""
    r, g, b = _hex_rgb01(hexstr)
    kr_, kb_ = _KEY_KRKB[KEY_COLORIMETRY]
    y = kr_ * r + (1.0 - kr_ - kb_) * g + kb_ * b
    u = (b - y) / (2.0 * (1.0 - kb_)); v = (r - y) / (2.0 * (1.0 - kr_))
    sc = float(1 << (BIT_DEPTH - 8))
    return (int(round((16.0 + 219.0 * y) * sc)), int(round((128.0 + 224.0 * u) * sc)),
            int(round((128.0 + 224.0 * v) * sc)))


def _color_planes(hexstr):
    """Plans (y, u, v) pleins d'une couleur unie (bordure du volet / du motif), mis en cache."""
    h = _norm_hex(hexstr) or "#ffffff"
    got = _color_planes_cache.get(h)
    if got is None:
        Y, U, V = _hex_yuv_code(h)
        got = dict(y=np.full((HEIGHT, WIDTH), Y, dtype=_NP_DT), u=np.full((UV_H, UV_W), U, dtype=_NP_DT),
                   v=np.full((UV_H, UV_W), V, dtype=_NP_DT))
        if len(_color_planes_cache) >= 4:
            _color_planes_cache.clear()
        _color_planes_cache[h] = got
    return got


@_lt_section("masks")
def _wipe_masks(alpha, w, owner=None):
    """Masques d'UNE trame de volet. Sans bordure : (m, m_chroma), m = part de B. Avec bordure :
    (mC, mC_chroma, mB, mB_chroma, plans couleur) — deux COUCHES, comme un mélangeur : la bordure
    (couleur unie) recouvre A selon mC, puis B recouvre le tout selon mB (cf. _wipe_compose).
    Bord de B en e, bord extérieur de la bordure en e + largeur ; la douceur INTERNE (côté A, la
    source qui part) adoucit le bord extérieur, l'EXTERNE (côté B, la source qui arrive) celui de B.
    La course de e va de min(−sB/2, −sA/2 − bw) à max(M + sB/2, M + sA/2 − bw), plus une marge
    d'un centième de douceur : à α = 0 les deux masques sont nuls partout, à α = 1 ils valent 1
    partout — tout A, puis tout B, quelle que soit la géométrie. Vues chroma = masque plein
    sous-échantillonné (mêmes conventions que l'overlay)."""
    d, M, tl, rev, rp = _wipe_ramps(alpha, w)
    keep = owner is not None
    if len(rp) == 1:
        m = _ramp_into(_wipe_buf(owner, 0), d, rp[0][0], rp[0][1], M, rev, tl, keep)
        return (m, m[::_CH, ::_CW])
    mC = _ramp_into(_wipe_buf(owner, 0), d, rp[0][0], rp[0][1], M, rev, tl, keep)
    mB = _ramp_into(_wipe_buf(owner, 1), d, rp[1][0], rp[1][1], M, rev, tl, keep)
    return (mC, mC[::_CH, ::_CW], mB, mB[::_CH, ::_CW], _color_planes(w.get("border_color")))


def _wipe_ramps(alpha, w):
    """Bords d'UNE trame de volet (cf. _wipe_masks) : (d, M, tuiles, rev, rampes). Rampes = ((e, s),)
    sans bordure (part couverte = part de B), ((e + bw, sA), (e, sB)) avec : bord extérieur de la
    bordure (part couverte), puis bord de B."""
    d, M, px, tl = _wipe_field(_wipe_geom(w))
    rev = bool(w.get("reverse"))
    a = max(0.0, min(1.0, float(alpha)))
    bw = float(w.get("border_width", 0.0) or 0.0) * M
    if bw <= 0.0:
        s = max(px, float(w.get("softness", 0.10) or 0.0) * M)
        mg = 0.01 * s
        e = (-0.5 * s - mg) + a * (M + s + 2.0 * mg)
        return d, M, tl, rev, ((e, s),)
    sA = max(px, float(w.get("border_soft_in", 0.0) or 0.0) * M)
    sB = max(px, float(w.get("border_soft_out", 0.0) or 0.0) * M)
    mg = 0.01 * max(sA, sB)
    e0 = min(-0.5 * sB, -0.5 * sA - bw) - mg
    e1 = max(M + 0.5 * sB, M + 0.5 * sA - bw) + mg
    e = e0 + a * (e1 - e0)
    return d, M, tl, rev, ((e + bw, sA), (e, sB))


def _full_combine(out, x, y, op):
    """out ← max(x, y) | min(x, y) | clamp(x − y, 0, 1) selon `op` ("max", "min", "sub"), plein
    cadre : RÉFÉRENCE du volet composé (sans propriétaire : motif, aperçu, outils) et repli d'une
    image qui ne se découpe pas en tuiles."""
    _tile_cls.pop(id(out), None)
    if op == "max":
        np.maximum(x, y, out=out)
    elif op == "min":
        np.minimum(x, y, out=out)
    else:
        np.subtract(x, y, out=out)
        np.clip(out, 0.0, 1.0, out=out)
    return out


# VOLET COMPOSÉ TUILÉ (0.23.48, 2026-10-29). Mesuré le 2026-10-29 sur 1277 (dell-1, 1080p50, keyer à
# l'antenne) : un AUTO composé (union iris + linéaire à 45°) commitait au p95 en 18,4 ms (max 22,9)
# contre 11,5 ms en volet linéaire — les deux rampes étaient écrites dans leurs propres tampons puis
# relues pour être combinées : trois dispersions tuile par tuile, et toutes les tuiles réécrites à la
# première image. Désormais le masque combiné est calculé DIRECTEMENT : la classe de chaque tuile de
# sortie se déduit des classes des deux rampes (union : 1 si l'une vaut 1, 0 si les deux valent 0 ;
# intersection : l'inverse ; décalage : 0 si x = 0 ou y = 1, 1 si x = 1 et y = 0 — exact, une rampe
# est dans [0, 1]) ; seules les tuiles restées indécises sont calculées, sans tampon intermédiaire,
# et une seule rampe là où l'autre est constante. Mêmes opérations sur les mêmes flottants que le
# plein cadre (_composite_masks sans propriétaire) : mêmes octets (1 428 images 1080p comparées à la
# 0.23.47). Mesuré le 2026-10-29 sur 1277, même charge : p95 / max 20,5 / 26,4 → 12,9 / 18,7 ms, soit
# le coût d'un volet simple ; masque seul (1080p, union) 9 → 3 ms au p95.
_CONST_MASKS = dict()


def _const_mask(v):
    """Masque plein constant (0 ou 1), en lecture seule et partagé : une trame entièrement A ou
    entièrement B (α = 0, α = 1) ne réécrit pas son tampon."""
    m = _CONST_MASKS.get(v)
    if m is None or m.shape != (HEIGHT, WIDTH):
        m = np.full((HEIGHT, WIDTH), float(v), dtype=np.float32)
        m.flags.writeable = False
        _CONST_MASKS[v] = m
    return m


def _tiled_combine(out, X, Y, fn, keep):
    """out ← combinaison `fn` des rampes X, Y = (d en tuiles, min, max, k, s, rev), tuile par tuile ;
    une trame entièrement constante rend le masque constant partagé (cf. _const_mask)."""
    cx = _ramp_cls(X[1], X[2], X[3], X[4], X[5])
    cy = _ramp_cls(Y[1], Y[2], Y[3], Y[4], Y[5])
    if fn == "max":
        cls = np.where((cx == 1) | (cy == 1), 1, np.where((cx == 0) & (cy == 0), 0, 2))
    elif fn == "min":
        cls = np.where((cx == 0) | (cy == 0), 0, np.where((cx == 1) & (cy == 1), 1, 2))
    else:
        cls = np.where((cx == 0) | (cy == 1), 0, np.where((cx == 1) & (cy == 0), 1, 2))
    cls = cls.astype(np.int8)
    ia = np.flatnonzero(cls == 2)
    if not ia.size and (cls == cls.flat[0]).all():
        return _const_mask(int(cls.flat[0]))
    o4 = _tile_fill(out, cls, keep)
    if not ia.size:
        return out
    # Tuile indécise dont UNE seule rampe est indécise : l'autre est la constante neutre de
    # l'opération (union : 0, intersection : 1, décalage : y = 0 → x tel quel ; x = 1 → 1 − y) —
    # seule la rampe indécise est calculée. Les deux indécises : les deux, puis l'opération.
    und = cls == 2
    gx = und & (cx == 2)
    ix, jx = np.nonzero(gx)
    if ix.size:
        xs = X[0][ix, :, jx, :]
        o4[ix, :, jx, :] = _ramp_calc(xs, xs, X[3], X[4], X[5])
    iy, jy = np.nonzero(und & (cy == 2) & ~gx)
    if iy.size:
        ys = Y[0][iy, :, jy, :]
        _ramp_calc(ys, ys, Y[3], Y[4], Y[5])
        if fn == "sub":
            np.subtract(np.float32(1.0), ys, out=ys)
            np.clip(ys, 0.0, 1.0, out=ys)
        o4[iy, :, jy, :] = ys
    ib, jb = np.nonzero(gx & (cy == 2))
    if ib.size:
        xs = o4[ib, :, jb, :]
        ys = Y[0][ib, :, jb, :]
        _ramp_calc(ys, ys, Y[3], Y[4], Y[5])
        if fn == "max":
            np.maximum(xs, ys, out=xs)
        elif fn == "min":
            np.minimum(xs, ys, out=xs)
        else:
            np.subtract(xs, ys, out=xs)
            np.clip(xs, 0.0, 1.0, out=xs)
        o4[ib, :, jb, :] = xs
    return out


def _composite_tiled(a, a2, wa, wb, fn, owner):
    """Masques d'une trame de volet composé par tuiles (cf. l'en-tête), None si l'image ne se
    découpe pas en tuiles."""
    dA, MA, tA, rA, pA = _wipe_ramps(a, wa)
    dB, MB, tB, rB, pB = _wipe_ramps(a2, wb)
    if tA is None or tB is None:
        return None

    def ramp(t, M, rev, es):
        return (t[0], t[1], t[2], _ramp_k(es[0], es[1], M, rev), es[1], rev)
    cA, bA = ramp(tA, MA, rA, pA[0]), ramp(tA, MA, rA, pA[-1])
    cB, bB = ramp(tB, MB, rB, pB[0]), ramp(tB, MB, rB, pB[-1])
    oc = (owner, "c")
    ob = _tiled_combine(_wipe_buf(oc, 1), bA, (bB if fn != "sub" else cB), fn, True)
    if len(pA) == 1 and len(pB) == 1:
        return (ob, ob[::_CH, ::_CW])
    occ = _tiled_combine(_wipe_buf(oc, 0), cA, (cB if fn != "sub" else bB), fn, True)
    col = (wa if float(wa.get("border_width", 0.0) or 0.0) > 0.0 else wb).get("border_color")
    return (occ, occ[::_CH, ::_CW], ob, ob[::_CH, ::_CW], _color_planes(col))


def _wm_pair(wm):
    """(part couverte, part de B) d'un jeu de masques : sans bordure, ce sont les mêmes."""
    return (wm[0], wm[0]) if len(wm) == 2 else (wm[0], wm[2])


@_lt_section("masks")
def _composite_masks(alpha, comp, owner=None, flip=False):
    """Masques d'une trame de volet COMPOSÉ (cf. l'en-tête VOLETS COMPOSÉS), même forme que
    _wipe_masks. Chaque volet donne (part couverte c, part de B b) ; c = b sans bordure.
      union        : b = max(bA, bB), c = max(cA, cB) ;
      intersection : b = min(bA, bB), c = min(cA, cB) ;
      décalage     : second volet à a2 = depth · (1 − |2u − 1|), u = clamp((α − offset) / (1 − offset)) ;
                     b = clamp(bA − cB(a2)) (ce que le second recouvre, bordure comprise, redevient A),
                     c = clamp(cA − bB(a2)) (la bordure du second se voit sur ce que le premier a découvert).
    Fin garantie : α = 1 ⇒ u = 1 ⇒ a2 = 0 ⇒ bB = cB = 0 ⇒ b = bA(1) = 1, c = cA(1) = 1 — tout B.
    Début : α = 0 ⇒ a2 = 0 et bA = cA = 0 ⇒ tout A. Union et intersection conservent 0 et 1.
    `flip` (REV / N/R du préréglage) inverse le sens des DEUX volets. Couleur de bordure : celle du
    premier volet s'il en a une, sinon celle du second."""
    a = max(0.0, min(1.0, float(alpha)))
    wa = dict(comp["a"]); wb = dict(comp["b"])
    if flip:
        wa["reverse"] = not wa.get("reverse"); wb["reverse"] = not wb.get("reverse")
    op = comp.get("op", "union")
    if op == "offset":
        dl = max(0.0, min(0.9, float(comp.get("offset", 0.3))))
        dp = max(0.0, min(1.0, float(comp.get("depth", 0.5))))
        u = 0.0 if a <= dl else min(1.0, (a - dl) / (1.0 - dl))
        a2 = dp * (1.0 - abs(2.0 * u - 1.0))
    else:
        a2 = a
    fn = {{"union": "max", "intersection": "min"}}.get(op, "sub")
    if owner is not None:
        # Boucle vidéo (tampons persistants) : tuiles, sans tampon intermédiaire (0.23.48).
        got = _composite_tiled(a, a2, wa, wb, fn, owner)
        if got is not None:
            return got
    # Sans propriétaire (motif, aperçu, outils) : plein cadre — la RÉFÉRENCE du chemin tuilé.
    ma = _wipe_masks(a, wa)
    mb = _wipe_masks(a2, wb)
    (cA, bA), (cB, bB) = _wm_pair(ma), _wm_pair(mb)
    ob = _full_combine(_wipe_buf(None, 1), bA, (bB if fn != "sub" else cB), fn)
    if len(ma) == 2 and len(mb) == 2:
        return (ob, ob[::_CH, ::_CW])
    occ = _full_combine(_wipe_buf(None, 0), cA, (cB if fn != "sub" else bB), fn)
    col = (wa if float(wa.get("border_width", 0.0) or 0.0) > 0.0 else wb).get("border_color")
    return (occ, occ[::_CH, ::_CW], ob, ob[::_CH, ::_CW], _color_planes(col))


def _preset_masks(alpha, preset, owner=None):
    """Masques d'une trame de la transition du fond : volet composé figé au départ s'il y en a un,
    sinon le volet simple du préréglage."""
    w = (preset or dict()).get("wipe") or dict()
    comp = (preset or dict()).get("composite_doc")
    if comp:
        return _composite_masks(alpha, comp, owner, flip=bool(w.get("reverse")))
    return _wipe_masks(alpha, w, owner)


# Au démarrage : les géométries du préréglage et des AUTO de keyer, calculées en tâche de fond.
_wipe_prefetch(state["preset"]["wipe"])
for _k0 in _layers():
    _wipe_prefetch(dict(direction=_k0.get("auto_dir")))
for _c0 in state["wipe_composites"].values():
    _wipe_prefetch(_c0["a"]); _wipe_prefetch(_c0["b"])


def _wipe_alpha_map(alpha, direction, reverse, softness_frac):
    """Masque plein cadre d'un volet SANS bordure (AUTO de keyer, outils) : 0 = A, 1 = B."""
    return _wipe_masks(alpha, dict(direction=direction, reverse=reverse, softness=softness_frac))[0]

def _mixmap(dst, a, b, m, colstep=1):
    """dst ← trunc(a·(1 − m) + b·m), masque float32 (ligne-stridé OK, `colstep` = pas de colonne
    SUPPLÉMENTAIRE dans le masque) : noyau C mvk_mixmap_into, repli numpy aux mêmes octets. `dst`
    peut être `a` (élément par élément, lu avant d'être écrit)."""
    if _MVK:
        try:
            if bobimxl.mvk_mixmap_into(dst, a, b, m, m_colstep=colstep):
                return
        except Exception:
            pass
    mm = m[:, ::colstep] if colstep > 1 else m
    dst[...] = (a.astype(np.float32) * (1.0 - mm) + b.astype(np.float32) * mm).astype(_NP_DT)


@_lt_section("wipe")
def _wipe_compose(oy, ou, ov, A, B, wm, a, b):
    """Volet sur les lignes [a, b) (chroma [a/_CH, b/_CH)) des plans de sortie : A, B = dict de
    plans pleins (y, u, v), wm = _wipe_masks. Sans bordure : une passe. Avec : deux COUCHES — la
    couleur sur A selon mC, puis B sur le tout selon mB. Même code pour la bande et l'image entière
    (a = 0, b = HEIGHT) : bit-exact de l'une à l'autre."""
    ca = a // _CH; cb = b // _CH
    if len(wm) == 2:
        _mixmap(oy[a:b], A["y"][a:b], B["y"][a:b], wm[0][a:b])
        _mixmap(ou[ca:cb], A["u"][ca:cb], B["u"][ca:cb], wm[1][ca:cb])
        _mixmap(ov[ca:cb], A["v"][ca:cb], B["v"][ca:cb], wm[1][ca:cb])
        return
    C = wm[4]
    for o, pa, pb, pc, mc, mb in ((oy[a:b], A["y"][a:b], B["y"][a:b], C["y"][a:b], wm[0][a:b], wm[2][a:b]),
                                  (ou[ca:cb], A["u"][ca:cb], B["u"][ca:cb], C["u"][ca:cb], wm[1][ca:cb], wm[3][ca:cb]),
                                  (ov[ca:cb], A["v"][ca:cb], B["v"][ca:cb], C["v"][ca:cb], wm[1][ca:cb], wm[3][ca:cb])):
        _mixmap(o, pa, pc, mc)
        _mixmap(o, o, pb, mb)


def _yuv_planes_ro(buf):
    """Vues (y, u, v) d'une trame yuv (bytes ou tableau), sans copie."""
    return dict(y=np.frombuffer(buf, dtype=_NP_DT, count=Y_SIZE // _BPS).reshape(HEIGHT, WIDTH),
                u=np.frombuffer(buf, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE).reshape(UV_H, UV_W),
                v=np.frombuffer(buf, dtype=_NP_DT, count=UV_SIZE // _BPS, offset=Y_SIZE + UV_SIZE).reshape(UV_H, UV_W))


def blend_yuv_wipe_w(yuv_a, yuv_b, alpha, w, owner=None):
    """Volet image entière, réglage complet `w` (forme, angle, centre, bordure…)."""
    return _blend_wm(yuv_a, yuv_b, _wipe_masks(alpha, w, owner))


def _blend_wm(yuv_a, yuv_b, wm):
    """Volet image entière, masques déjà calculés (volet simple ou composé)."""
    out_y = np.empty((HEIGHT, WIDTH), dtype=_NP_DT)
    out_u = np.empty((UV_H, UV_W), dtype=_NP_DT)
    out_v = np.empty((UV_H, UV_W), dtype=_NP_DT)
    _wipe_compose(out_y, out_u, out_v, _yuv_planes_ro(yuv_a), _yuv_planes_ro(yuv_b), wm, 0, HEIGHT)
    return out_y.tobytes() + out_u.tobytes() + out_v.tobytes()


def blend_yuv_wipe(yuv_a, yuv_b, alpha, direction, reverse, softness):
    """Ancienne signature (outils, comparaisons) : volet sans bordure."""
    return blend_yuv_wipe_w(yuv_a, yuv_b, alpha, dict(direction=direction, reverse=reverse, softness=softness))

def blend_yuv_preset(yuv_a, yuv_b, alpha, preset):
    """Dispatch sur le mode du preset snapshot."""
    if preset is None:
        return blend_yuv(yuv_a, yuv_b, alpha)
    mode = preset.get("mode", "dissolve")
    if mode == "additif":
        return blend_yuv_additive(yuv_a, yuv_b, alpha)
    if mode == "wipe":
        return _blend_wm(yuv_a, yuv_b, _preset_masks(alpha, preset, owner="bgd"))
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
            if _last_grid and g > _last_grid + 1:
                _lt_over[1] += g - _last_grid - 1      # créneaux sans trame (tour trop long)
            _last_grid = g
            # retard du réveil sur le début du créneau (cf. TEMPS PERDU PAR LA BOUCLE PGM)
            _lt_acc["wake"] = (int(bobimxl.now_tai()) - _grid_phase) % _SLOT_NS
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
            _w0 = time.perf_counter_ns()
            got = r.get(fi - 1, timeout_ns=2_000_000) or r.get_latest()
            _lt_wait(time.perf_counter_ns() - _w0)
            static = True
        else:
            # 1ʳᵉ tranche du grain ciblé (réveil au commit partiel) ; pas encore là / flux
            # whole-frame sans le patch slices → dernier grain complet (dégénéré, 0 attente).
            _left = max(1, int((deadl - time.monotonic()) * 1e9))
            _w0 = time.perf_counter_ns()
            got = r.get_slice(fi, 1, timeout_ns=_left)
            _lt_wait(time.perf_counter_ns() - _w0)
            if got is None:
                got = r.get_latest(); static = True
                bind_late = on_grid
        repeated = False
        if got is None:
            # IMAGE MANQUANTE (0.23.60) : ni le grain visé ni le dernier grain lisible — un TROU
            # du producteur (tête en cours d'écriture, case précédente marquée bouche-trou).
            # Mode repeat : la dernière image valide de CETTE entrée, relue par son index
            # (cf. _mf_repeat_slice) ; mode black ou rien de sûr à relire : None → noir, comme avant.
            got = _mf_repeat_slice(slot, r)
            repeated = got is not None
            if got is None and _il_order(r) is not None:
                fr = _mf_repeat_whole(slot, r)       # source entrelacée : dernière trame tissée
                if fr is None:
                    return None
                pl = np.frombuffer(fr, dtype=np.uint8)
                return dict(slot=slot, r=r, fi=int(h), total=1, valid=1,
                            islh=HEIGHT, budget=int(1e9 / FPS), late=False, repeated=True,
                            y=pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                            u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                            v=pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))
        if got is not None and got[2].size == V_FRAME_SIZE // 2 and _il_order(r) is not None:
            # Source ENTRELACÉE (grain = champ) : PAS de mode tranche possible (le producteur
            # committe par CHAMPS) → repli whole-frame TISSÉ, comme lire_frame_yuv.
            fr, ts = lire_frame_yuv(r)
            if fr is None:
                fr = _mf_repeat_whole(slot, r)       # tissage impossible (champ manquant)
                if fr is None:
                    return None
                ts = None
            else:
                _mf_note_whole(slot, r, fr)
            pl = np.frombuffer(fr, dtype=np.uint8)
            if ts:
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
        if repeated:
            static = True; bind_late = False    # grain complet, déjà vérifié (cf. _mf_repeat_slice)
        else:
            _mf_note_slice(slot, r, got[0], total)   # un index, jamais une vue (cf. en-tête)
        st = dict(slot=slot, r=r, fi=int(got[0]), total=total,
                  valid=(total if static else max(1, int(gi.validSlices or 1))),
                  islh=max(1, HEIGHT // total),
                  budget=int(1e9 / FPS),   # budget d'attente PAR ENTRÉE (motif multiview)
                  late=bind_late, repeated=repeated,
                  y=pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH),
                  u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                  v=pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W))
        if dbg[1] < 0:
            dbg[1] = st["valid"]
        ts_in_per_slot[slot] = r.last_write_time()
        return st
    except Exception:
        return None


def _sl_wait(st, need_row, deadl, dbg, safe=False):
    """Attend (get_slice, réveil au commit partiel) que l'entrée couvre la ligne image
    `need_row` — budget PAR ENTRÉE, borné par le budget de trame DUR `deadl` (garde-fou n°1).
    Dépassement → REPLI IMMÉDIAT sur le dernier grain complet (tearing d'UNE image sur CETTE
    entrée seulement, les bandes déjà posées restent celles du grain fi) + comptage backoff
    (2 timeouts même grain → dormante). Renvoie les ns réellement attendues (exclues de own).
    `safe` (fil PVW, 0.23.40) : repli par `get_slice` — jamais `get`, dont la garde de génération
    peut rouvrir le lecteur pendant que le fil principal lit ses grains."""
    need_k = min(st["total"], need_row // st["islh"] + 1)
    if need_k <= st["valid"]:
        return 0
    dbg[2] += 1
    _left = min(deadl - time.monotonic(), st["budget"] / 1e9)
    _w0 = time.monotonic()
    g = st["r"].get_slice(st["fi"], need_k, timeout_ns=max(1, int(_left * 1e9))) if _left > 0 else None
    dw = int((time.monotonic() - _w0) * 1e9)
    st["budget"] -= dw
    _lt_wait(dw)
    if g is not None:
        st["valid"] = max(need_k, int(g[1].validSlices or need_k))
        return dw
    dbg[3] += 1
    st["late"] = True
    _bk = _sl_backoff.get(st["slot"])
    _sl_backoff[st["slot"]] = [st["fi"], (_bk[1] + 1 if _bk and _bk[0] == st["fi"] else 1)]
    _w1 = time.perf_counter_ns()
    try:
        gp = (st["r"].get_slice(st["fi"] - 1, st["total"], timeout_ns=2_000_000) if safe
              else st["r"].get(st["fi"] - 1, timeout_ns=2_000_000))
    except Exception:
        gp = None
    _lt_wait(time.perf_counter_ns() - _w1)
    if gp is not None and gp[2].size == V_FRAME_SIZE:
        pl = gp[2]
        st["y"] = pl[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH)
        st["u"] = pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
        st["v"] = pl[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
    st["valid"] = st["total"]     # plus aucune attente pour cette entrée cette trame
    return dw


@_lt_section("blend")
def _sl_blend_band(oy, ou, ov, A, B, alpha, mode, wm, a, b):
    """Blend BGD de la bande [a,b) — MÊMES maths float32 que blend_yuv / blend_yuv_additive /
    blend_yuv_wipe (opérations per-pixel, ligne-locales) appliquées au DELTA de lignes →
    équivalence octet-à-octet bande ≡ plein (array_equal, cf. tests offline). Le masque wipe
    `wm` (cf. _wipe_masks) est PRÉCALCULÉ par (alpha, préréglage) puis TRANCHÉ par bande ; volet
    (bordure comprise, 0.23.44) : _wipe_compose, le même code que l'image entière."""
    if mode == "wipe" and wm is not None:
        _wipe_compose(oy, ou, ov, A, B, wm, a, b)
        return
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
            if mode == "additif":
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
    if mode == "additif":
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
    if t in ("linear", "web"):      # web (0.23.52) : le key est l'alpha de la page, plein échelle
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


@_lt_section("chroma")
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


# ─── Keyer MOTIF (0.23.22 ; formes, angle, répétition et bordure 0.23.44) ──────────────────────
# Alpha = une FORME en coordonnées ÉCRAN (le motif ne bouge pas avec le fly : il découpe ce que le
# fill y montre), carte uint8 plein cadre. Mêmes formes et même champ de distance que le volet
# (_wipe_field, variante `pattern` : distance au centre, linéaire = barre) : d = 1 couvre l'écran
# pour un motif centré. Rayon R = taille × (1 + douceur) : taille 1 couvre l'écran, taille 0 n'est
# rien, la douceur est une rampe INTÉRIEURE (alpha = clamp((R − d) / douceur, 0, 1)).
# BORDURE (0.23.44) : une couche de couleur unie SOUS le fill, la forme élargie de la largeur de
# bordure — même règle de couches que le volet. Douceur INTERNE = côté de ce que le motif recouvre
# (le fond, la source qui « part ») : le bord extérieur de la bordure ; EXTERNE = côté de ce qui
# arrive (le fill) : le bord du fill, qui remplace alors la douceur du motif.
# Alpha d'antenne : table de 256 appliquée par bande, sur le seul rectangle visible.
# Calcul d'une carte : champ de distance (en cache par géométrie) puis profil. Quand le champ manque
# et que son calcul MESURÉ dépasse une demi-période (cœur, étoile en 1080p : 0,2 à 0,4 s le
# 2026-10-29), la carte est calculée en TÂCHE DE FOND et le keyer garde sa carte précédente en
# attendant (tourner l'angle d'un cœur ne fait pas tomber d'images) ; la toute première, et tout
# ce qui est rapide, se calcule tout de suite.
_pattern_cache = dict()
_pattern_last = dict()          # keyer → dernière carte servie
_pattern_busy = set()
_pattern_lock = threading.Lock()


def _pattern_profile(d, R, sw):
    return np.rint(np.clip((R - d) / sw, 0.0, 1.0) * 255.0).astype(np.uint8)


def _pattern_key(ks):
    comp = _wipe_composites.get(ks.get("pattern_composite") or "")
    if comp:
        # Volet composé comme motif (0.23.45) : sa part de B à « α = taille », bordure comprise ; la
        # géométrie est celle du composé (position, angle, répétition et douceur du motif ignorés).
        return ("composite", json.dumps(comp, sort_keys=True), float(ks.get("pattern_size", 0.5)),
                bool(ks.get("invert")))
    name = str(ks.get("pattern", "iris") or "iris").strip().lower()
    if name not in WIPE_NAMES:
        name = "iris"
    w = dict(direction=name, angle=ks.get("pattern_angle", 0.0), center_x=ks.get("pattern_x", 0.5),
             center_y=ks.get("pattern_y", 0.5), repeat=ks.get("pattern_repeat", 1))
    bw = float(ks.get("pattern_border", 0.0) or 0.0)
    return (_wipe_geom(w), float(ks.get("pattern_size", 0.5)), float(ks.get("pattern_soft", 0.1)),
            bool(ks.get("invert")), bw,
            (_norm_hex(ks.get("pattern_border_color")) or "#ffffff") if bw > 0 else None,
            float(ks.get("pattern_border_soft_in", 0.0) or 0.0) if bw > 0 else 0.0,
            float(ks.get("pattern_border_soft_out", 0.0) or 0.0) if bw > 0 else 0.0)


def _pattern_build_composite(key):
    _tag, doc, size, inv = key
    wm = _composite_masks(size, json.loads(doc))
    c, b = _wm_pair(wm)
    m8 = np.rint(b * 255.0).astype(np.uint8)
    c8 = np.rint(c * 255.0).astype(np.uint8) if len(wm) > 2 else None
    if inv:
        m = 255 - (c8 if c8 is not None else m8)
        bm = (255 - m8) if c8 is not None else None
    else:
        m = m8; bm = c8
    return dict(map=m, bmap=bm, bcol=(wm[4] if len(wm) > 2 else None))


def _pattern_build(key):
    if key[0] == "composite":
        got = _pattern_build_composite(key)
        with _pattern_lock:
            if len(_pattern_cache) > 8:
                _pattern_cache.clear()
            _pattern_cache[key] = got
            _pattern_busy.discard(key)
        return got
    geom, size, soft, inv, bw, bcol, bsi, bso = key
    d, _M, px, _tl = _wipe_field(geom, pattern=True)
    if bw > 0:
        sw = max(bso, px)                       # bord du fill : douceur EXTERNE (côté fill)
        sA = max(bsi, px)                       # bord extérieur de la bordure : douceur INTERNE
    else:
        sw = max(soft, px); sA = sw
    R = size * (1.0 + sw)
    if inv:
        m = 255 - _pattern_profile(d, R, sw)
        bm = (255 - _pattern_profile(d, R - bw, sA)) if bw > 0 else None
    else:
        m = _pattern_profile(d, R, sw)
        bm = _pattern_profile(d, R + bw, sA) if bw > 0 else None
    got = dict(map=m, bmap=bm, bcol=(_color_planes(bcol) if bw > 0 else None))
    with _pattern_lock:
        if len(_pattern_cache) > 8:
            _pattern_cache.clear()
        _pattern_cache[key] = got
        _pattern_busy.discard(key)
    return got


def _pattern_slow(key):
    """Le calcul de cette carte manque-t-il d'un champ dont le calcul MESURÉ dépasse une demi-période ?"""
    if key[0] == "composite":
        doc = json.loads(key[1])
        geoms = [(_wipe_geom(doc[p]), False) for p in ("a", "b")]
    else:
        geoms = [(key[0], True)]
    return any(g not in _wipe_fields and _field_cost_ms.get(g[0][0], 0.0) > 500.0 / FPS for g in geoms)


def _pattern_maps(ks):
    key = _pattern_key(ks)
    who = ks.get("_i")
    with _pattern_lock:
        got = _pattern_cache.get(key)
        prev = _pattern_last.get(who)
    if got is None:
        # Carte à calculer. En tâche de fond seulement si elle a une carte à servir en attendant ET
        # si son champ manque et coûte (mesuré) plus d'une demi-période : un profil sur un champ en
        # cache, ou une petite image, se calcule tout de suite.
        slow = _pattern_slow(key)
        if prev is not None and who is not None and slow:
            with _pattern_lock:
                start = key not in _pattern_busy
                _pattern_busy.add(key)
            if start:
                threading.Thread(target=_pattern_build, args=(key,), daemon=True).start()
            return prev
        got = _pattern_build(key)
    with _pattern_lock:
        _pattern_last[who] = got
    return got


def _pattern_map(ks):
    """Carte alpha uint8 du motif (sans bordure) — compatibilité des outils et des tests."""
    return _pattern_maps(ks)["map"]


def _pattern_cut(ks, ka):
    q = int(round(float(ka) * 255.0))
    kt = None if q >= 255 else np.rint(np.arange(256, dtype=np.float32) * (q / 255.0)).astype(np.uint8)
    pm = _pattern_maps(ks)
    return dict(kind="pattern", map=pm["map"], ka=kt, bmap=pm["bmap"], bcol=pm["bcol"])


@_lt_section("pattern")
def _sl_pattern_band(py, pu, pv, F, g, pc, a, b):
    """Keyer MOTIF sur la bande [a,b) : alpha = la carte du motif sous le rectangle visible (vue, sans
    copie), alpha d'antenne par table ; chroma : alpha au pixel luma PAIR (convention du linear)."""
    a2 = max(a, g["out_y0"]); b2 = min(b, g["out_y1"])
    M = pc["map"]; kt = pc["ka"]
    BM = pc.get("bmap"); BC = pc.get("bcol")
    x0o = g["out_x0"]; x1o = g["out_x1"]
    if a2 < b2:
        if BM is not None:          # bordure (0.23.44) : couleur unie SOUS le fill, même rectangle
            bm = BM[a2:b2, x0o:x1o]
            _blend8(py[a2:b2, x0o:x1o], BC["y"][a2:b2, x0o:x1o], bm if kt is None else np.take(kt, bm))
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
    if BM is not None:
        bc = BM[ca2 * _CH:cb2 * _CH:_CH, x0o:x1o:_CW][:, :cx1 - cx0]
        bc = np.ascontiguousarray(bc if kt is None else np.take(kt, bc))
        for P, S in ((pu, BC["u"]), (pv, BC["v"])):
            _blend8(P[ca2:cb2, cx0:cx1], S[ca2:cb2, cx0:cx1], bc)
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


@_lt_section("cut")
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
    _w0 = time.perf_counter_ns()
    try:
        gk = rk.get(int(got[0]), timeout_ns=2_000_000)
    except Exception:
        gk = None
    _lt_wait(time.perf_counter_ns() - _w0)
    if gk is not None and len(gk[2]) == V_FRAME_SIZE:
        return fill, bytes(gk[2]), ts
    k, _ts = lire_frame_yuv(rk)
    return fill, k, ts


def _sl_key_one(kl, y, u, v, a, b, deadl, dbg, safe=False):
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
        w += _sl_wait(F, nr, deadl, dbg, safe)
        if KK is not F:
            w += _sl_wait(KK, nr, deadl, dbg, safe)
    base = None
    if kl.get("wm") is not None:
        ca = a // _CH; cb = b // _CH
        base = (y[a:b].copy(), u[ca:cb].copy(), v[ca:cb].copy())
    _sl_keyer_band(y, u, v, F, kl["g"], kl["ka"], a, b, KK, kl["lut"])
    if base is not None:
        _sl_wipe_band(y, u, v, base, kl["wm"], a, b)
    return w


@_lt_section("kwipe")
def _sl_wipe_band(py, pu, pv, base, m, a, b):
    """AUTO de keyer en VOLET : bande [a,b) = base (avant le keyer) · (1 − m) + keyée · m, avec m le
    masque plein trame du volet (_wipe_masks). Même noyau que l'overlay (mvk_mixmap_into),
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
        _whole_key_one(P, ks, ts_in_per_slot)
    return buf.tobytes()


def _whole_layer_src(ks, ka, g, ts_in_per_slot):
    """(F, KK, g) d'une couche pour la trame entière — ses images lues (ou la page figée), la
    géométrie rognée d'une page web — ou None (rien à composer)."""
    fi = int(ks["input"])
    if ks.get("type") == "web":
        # Couche web (0.23.52) : la page figée au latch, composée comme un linear.
        F, KK = _web_srcs(ks.get("_web"))
        if F is None:
            return None
        if int(_key_lut(ks, ka)[0]) == 0:
            g = _bbox_crop(g, ks["_web"].get("bbox"))
            if g is None:
                return None
        return F, KK, g
    if ks.get("type") == "linear":
        rf = ensure_input(fi)
        rk = ensure_input(int(ks.get("key_input", fi)))
        if rf is None or rk is None:
            return None
        f, k, ts = _read_pair(rf, rk)
        # (0.23.60) fill ou key manquant : la dernière image valide de CETTE entrée (mode repeat).
        # La paire n'est plus appariée au grain près pendant cette trame-là — mieux qu'une couche
        # qui disparaît, et la recopie est comptée.
        ki = int(ks.get("key_input", fi))
        if f:
            _mf_note_whole(fi, rf, f)
        else:
            f = _mf_repeat_whole(fi, rf); ts = None
        if rk is rf:
            k = f
        elif k:
            _mf_note_whole(ki, rk, k)
        else:
            k = _mf_repeat_whole(ki, rk)
    else:
        f, ts = _read_whole(fi)
        k = f
    if ts:
        ts_in_per_slot[fi] = ts
    if not f or not k:
        return None
    F = _planes_of(f)
    return F, (F if k is f else _planes_of(k)), g


def _whole_key_one(P, ks, ts_in_per_slot, cache=None, ck=None):
    """Une couche composée EN PLACE sur la trame entière P (plans). `cache` (dict, clé `ck`) : ses
    images lues une seule fois par trame quand plusieurs sorties la composent (0.23.53)."""
    ka = float(ks.get("alpha", 0.0))
    if ka <= 0.001:
        return
    g = _sl_keyer_geom(ks)
    if g is None:
        return
    if cache is not None and ck in cache:
        src_ = cache[ck]
    else:
        src_ = _whole_layer_src(ks, ka, g, ts_in_per_slot)
        if cache is not None:
            cache[ck] = src_
    if src_ is None:
        return
    F, KK, g = src_
    wmk = ks.get("wmask")
    base = (P["y"].copy(), P["u"].copy(), P["v"].copy()) if wmk is not None else None
    _sl_keyer_band(P["y"], P["u"], P["v"], F, g, ka, 0, HEIGHT, KK, _key_lut(ks, ka))
    if base is not None:
        _sl_wipe_band(P["y"], P["u"], P["v"], base, wmk, 0, HEIGHT)

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


@_lt_section("dve")
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


def _pvw_submit(job):
    """Fil principal : confie la trame PVW au fil PVW — ne bloque JAMAIS (cf. POLITIQUE DE
    DÉPASSEMENT, déclarations de _pvw_cv). Démarre le fil au premier appel."""
    global _pvw_pending, _pvw_thread
    if _pvw_thread is None:
        _pvw_thread = threading.Thread(target=_pvw_loop, name="mixer-pvw", daemon=True)
        _pvw_thread.start()
    with _pvw_cv:
        if _pvw_pending is not None:
            _pvw_stats["dropped"] += 1      # la plus ancienne en attente n'aura pas de grain
            job["light"] = True
        elif _pvw_running:
            job["light"] = True             # en retard d'une trame : rattrape sans keyers
        if job["light"]:
            _pvw_stats["queued"] += 1
        _pvw_pending = job
        _pvw_cv.notify_all()


def _pvw_drain(timeout=2.0):
    """Attend que le fil PVW n'ait plus rien à faire (tests hors ligne, arrêt). Vrai si c'est le cas."""
    end = time.monotonic() + timeout
    with _pvw_cv:
        while _pvw_pending is not None or _pvw_running:
            left = end - time.monotonic()
            if left <= 0:
                return False
            _pvw_cv.wait(left)
    return True


def _pvw_loop():
    global _pvw_pending, _pvw_running
    try:
        os.setpriority(os.PRIO_PROCESS, threading.get_native_id(), PVW_NICE)
    except Exception as e:
        log("mixer: PVW thread priority not lowered (%s) — PGM-first not enforced by the scheduler"
            % e, "warning")
    while True:
        with _pvw_cv:
            while _pvw_pending is None:
                _pvw_cv.wait()
            job = _pvw_pending
            _pvw_pending = None
            _pvw_running = True
        try:
            with _pvw_busy:
                if job["epoch"] != _pvw_epoch:
                    _pvw_stats["stale"] += 1    # lecteur/sortie fermé depuis : rien de sûr à lire
                else:
                    _pvw_frame(job)
        except Exception as e:
            _pvw_stats["errors"] += 1
            if _pvw_stats["errors"] in (1, 10, 100) or _pvw_stats["errors"] % 1000 == 0:
                log("mixer: PVW frame failed (%d so far): %s" % (_pvw_stats["errors"], e), "warning")
        finally:
            with _pvw_cv:
                _pvw_running = False
                _pvw_cv.notify_all()


def _pvw_after(job):
    """Mode « after » : la trame PVW sur le fil principal, PGM et CLEAN déjà publiés. Garde de
    budget (même règle que l'image entière) : sans keyers si elle ne tient plus dans la période."""
    global _pvw_after_cost_ms
    t = time.monotonic()
    if job["kls"] and (t - job["t0"]) * 1e3 + _pvw_after_cost_ms > PVW_BUDGET_FRAC * 1000.0 / FPS:
        job["light"] = True
        _pvw_stats["queued"] += 1
    try:
        with _pvw_busy:
            _pvw_frame(job)
    except Exception as e:
        _pvw_stats["errors"] += 1
        if _pvw_stats["errors"] in (1, 10, 100) or _pvw_stats["errors"] % 1000 == 0:
            log("mixer: PVW frame failed (%d so far): %s" % (_pvw_stats["errors"], e), "warning")
    if job["kls"] and not job["light"]:
        _pvw_after_cost_ms = (time.monotonic() - t) * 1e3


def _pvw_frame(job):
    """Fil PVW : compose et publie UNE trame PVW bande par bande — fond futur + keyers dans leur
    état futur, sans habillage (cf. _pvw_future) — exactement les opérations que la boucle unique
    faisait avant 0.23.40, sur des copies de l'état des entrées. `light` : sans les keyers."""
    global _pvw_skips, _pvw_dbg
    V = job["V"]
    kls_pv = job["kls"]
    if job["light"]:
        if kls_pv:
            _pvw_skips += 1
        kls_pv = []
        # trame rattrapée : ses entrées sont arrivées depuis longtemps, l'échéance d'origine est
        # passée — un budget neuf évite de compter en retard ce qui ne l'est pas.
        deadl = time.monotonic() + 0.5 / FPS
    else:
        deadl = job["deadl"]
    dbg = [job["n_in"], -1, 0, 0, 0]
    t_start = time.monotonic()
    wait_ns = 0
    w = out_pvw
    _i, gi_v, vw_v = w.open_grain(index=job["idx"])
    pv_y = vw_v[:Y_SIZE].view(_NP_DT).reshape(HEIGHT, WIDTH)
    pv_u = vw_v[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
    pv_v = vw_v[Y_SIZE + UV_SIZE:V_FRAME_SIZE].view(_NP_DT).reshape(UV_H, UV_W)
    done = 0
    need = [V] + [kl["F"] for kl in kls_pv] + [kl["KK"] for kl in kls_pv]
    full = False
    n_coarse = n_fine = 0
    try:
        k = 0
        while k < _SL_NB:
            # Bande de CALCUL : SLICE_GROUP bandes publiées si tout est déjà là (cf. SLICE_GROUP).
            full = full or (SLICE_GROUP > 1 and _sl_all_full(need))
            n = min(SLICE_GROUP, _SL_NB - k) if full else 1
            a = k * SLICE_LINES; b = a + n * SLICE_LINES
            ca = a // _CH; cb = b // _CH
            if V is not None:
                wait_ns += _sl_wait(V, b - 1, deadl, dbg, True)
                pv_y[a:b] = V["y"][a:b]; pv_u[ca:cb] = V["u"][ca:cb]; pv_v[ca:cb] = V["v"][ca:cb]
            else:
                pv_y[a:b] = _BLACK_Yi; pv_u[ca:cb] = _NEUT_Ci; pv_v[ca:cb] = _NEUT_Ci
            for kl in kls_pv:
                wait_ns += _sl_key_one(kl, pv_y, pv_u, pv_v, a, b, deadl, dbg, True)
            done = b
            k += n
            if n > 1: n_coarse += 1
            else: n_fine += 1
            if k < _SL_NB:   # commit PROGRESSIF ; bande finale → finally
                w.commit(gi_v, valid_slices=k)
    finally:
        # Même contrat que le PGM : jamais de lignes indéterminées, commit final TOUJOURS.
        if done < HEIGHT:
            cd = done // _CH
            pv_y[done:] = _BLACK_Yi; pv_u[cd:] = _NEUT_Ci; pv_v[cd:] = _NEUT_Ci
        try: w.commit(gi_v, valid_slices=None)
        except Exception: pass
        t_end = time.monotonic()
        _pvw_commit_ms.append((t_end - job["t0"]) * 1e3)
        _pvw_own.append(((t_end - t_start) * 1e9 - wait_ns) / 1e6)
        _pvw_stats["frames"] += 1
        _sl_count_grouping("pvw", n_coarse, n_fine)
        _pvw_dbg = dbg
        late = [st["slot"] for st in [V] + [kl["F"] for kl in kls_pv] + [kl["KK"] for kl in kls_pv]
                if st is not None and st.get("late")]
        if late:
            with _pvw_cv:
                _pvw_late_slots.update(late)


def _web_srcs(prep):
    """(fill, key) d'une couche web pour le compose bandé : deux pseudo-entrées sur la page figée,
    complètes (aucune attente, aucun repli, jamais en retard). (None, None) si pas encore rendue."""
    if prep is None:
        return None, None
    base = dict(slot=None, r=None, fi=None, total=1, valid=1, islh=HEIGHT, budget=0, late=False)
    F = dict(base, y=prep["y"], u=prep["u"], v=prep["v"])
    K = dict(base, y=prep["k"], u=prep["u"], v=prep["v"])
    return F, K


def _slice_frame(grid, _unused, bgd_active, bgd_alpha, bgd_from_idx, bgd_to_idx, bgd_preset,
                 keyer_alpha, keyer_state, ovl, pgm, pvw, ts_in_per_slot, keyers=None,
                 pvw_keyers=None, outs=None):
    """MODE TRANCHE — compose et PUBLIE la trame BANDE PAR BANDE aux sorties programme et au PVW.

    SORTIES (0.23.53) : `outs` = instantané des sorties (cf. _latch_outputs ; None → lu ici). Chaque
    sortie reçoit le fond puis SES couches, dans l'ordre de la pile ; les couches communes sont
    composées une fois (plus long préfixe commun, cf. _out_plan). Une couche visible qui n'est sur
    aucune sortie n'est ni lue ni composée.

    PVW (0.23.29) : `pvw` = entrée du FOND du PVW (cf. _pvw_future : entrée PVW ou PGM selon la
    prochaine transition) ; `pvw_keyers` = keyers du PVW (même forme que `keyers`). Aucun keyer
    visible au PVW (ou None) → copie de l'entrée, comme avant 0.23.29 : zéro surcoût. Sinon les
    keyers visibles sont posés bande par bande avec _sl_key_one — même code, mêmes attentes bornées
    par le budget de trame, même repli sur le dernier grain qu'au PGM ; leurs entrées sont ouvertes
    même hors antenne. Jamais d'habillage au PVW.
    0.23.40 (2026-10-29) : le PVW est composé et committé par le FIL PVW (_pvw_frame), sur des
    copies de l'état des entrées liées ICI — cette fonction ne compose et ne committe plus que
    CLEAN et PGM, qui n'attendent jamais le PVW (cf. POLITIQUE DE DÉPASSEMENT).

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
    t_frame0 = time.monotonic()   # ouverture de la trame : base de pgm/pvw_commit_ms
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
    if outs is None:
        with state_lock:
            outs = _latch_outputs()

    def _visible(kset, onlyout=False):
        """Keyers VISIBLES d'une pile : latch, géométrie, table d'alpha, entrées. `onlyout` : ceux
        qui sont sur au moins une sortie (0.23.53)."""
        res = []
        for pos, ks in enumerate(kset or ()):
            li = ks.get("_i", pos)
            if onlyout and not any(_out_has(o, li, ks) for o in outs):
                continue
            ka = float(ks.get("alpha", 0.0))
            if ka <= 0.001:
                continue
            g = _sl_keyer_geom(ks)
            if g is None:
                continue
            lin = ks.get("type") == "linear"
            web = ks.get("type") == "web"
            lut = _key_lut(ks, ka)
            if web and ks.get("_web") is not None and int(lut[0]) == 0:
                # page : seul son rectangle non transparent compte (un key 0 y reste transparent)
                g = _bbox_crop(g, ks["_web"].get("bbox"))
                if g is None:
                    continue
            res.append(dict(g=g, ka=ka, lut=lut, fill=(None if web else int(ks["input"])),
                            key=(int(ks.get("key_input", ks["input"])) if lin else None),
                            wm=ks.get("wmask"), li=li, k=ks,
                            web=web, webp=(ks.get("_web") if web else None)))
        return res
    kls = _visible(keyers, True)    # sorties programme
    kls_pv = _visible(pvw_keyers) if HAS_PVW else []   # PVW (vide → copie de l'entrée, zéro surcoût)
    bgd_mode = (bgd_preset.get("mode", "dissolve") if bgd_preset else "dissolve")
    wm = None
    if bgd_active and bgd_mode == "wipe":
        wm = _preset_masks(bgd_alpha, bgd_preset, owner="bgd")
    if ovl is not None and ovl["y"].shape != (HEIGHT, WIDTH):
        ovl = None   # même garde de forme que le chemin historique (overlay PRÉ-CONVERTI)
    # ─ Ouverture des entrées actives de la trame, TOUTES au même index (ou leur tête si
    #   hors-grille). Un slot partagé par plusieurs couches (ex. keyer = PGM) n'est lu qu'une fois.
    slots = ([bgd_from_idx, bgd_to_idx] if bgd_active else [pgm])
    for kl in kls + kls_pv:
        slots.append(kl["fill"])
    slots = [s_ for s_ in slots if s_ is not None]     # couche web : aucune entrée à lire
    if HAS_PVW:                                        # sans sortie PVW (0.23.53) : rien à lire pour elle
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
        if kl["web"]:
            # Couche web (0.23.52) : fill et key = la page figée au latch, toujours complète.
            kl["F"], kl["KK"] = _web_srcs(kl["webp"])
            continue
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
            # Même règle que les entrées (cf. `_need` ci-dessus), au délai RÉELLEMENT demandé au key
            # (grain de son fill) : un key connu pour arriver après lui n'est pas attendu à chaque image.
            _need = _lag_of(kl["key"])
            _dl = time.monotonic() if (_need is not None and _need > grid - want) else bind_deadl
            keysrcs[ck] = _sl_bind(kl["key"], grid, want, _dl, ts_in_per_slot, dbg)
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
    # Un grain par sortie programme (0.23.53), toutes au même numéro.
    ows = [_out_w(o) for o in outs]
    ogr = [w_.open_grain(index=idx_out) for w_ in ows]
    # PVW → fil PVW, lancé TOUT DE SUITE (il compose pendant que ce fil fait CLEAN et PGM). Il
    # reçoit une COPIE de l'état de chaque entrée qu'il lit : ses attentes, ses replis et son
    # budget ne touchent pas l'état du PGM ; deux couches qui partageaient une entrée partagent
    # la même copie, comme elles partageaient l'original.
    _cp = dict()

    def _own(st):
        if st is None:
            return None
        c = _cp.get(id(st))
        if c is None:
            c = _cp[id(st)] = dict(st)
        return c
    pvw_job = dict(idx=idx_out, V=_own(V),
                   kls=[dict(kl, F=_own(kl["F"]), KK=_own(kl["KK"])) for kl in kls_pv],
                   deadl=time.monotonic() + 1.0 / FPS, t0=t_frame0, epoch=_pvw_epoch,
                   n_in=dbg[0], light=False)
    if HAS_PVW and PVW_MODE == "thread":
        _pvw_submit(pvw_job)
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
    OP = [_planes(g_[2]) for g_ in ogr]
    pg_t = next((t for t, o in enumerate(outs) if o["id"] == OUTPUT_ID_PGM), 0)
    # GARDE-FOU n°1 : la sortie ne peut JAMAIS être en retard — budget de trame DUR.
    deadl = time.monotonic() + 1.0 / FPS
    wait_ns = 0
    done = 0
    # Plan de la trame (0.23.53) : couches de chaque sortie, préfixes communs composés une fois.
    plan = _out_plan([[j for j, kl in enumerate(kls) if _out_has(o, kl["li"], kl["k"])] for o in outs])
    need = (([A, B] if blend_on else [S]) + [kl["F"] for kl in kls] + [kl["KK"] for kl in kls])
    full = False
    n_coarse = n_fine = 0
    t_first = None
    try:
        k = 0
        while k < _SL_NB:
            # Bande de CALCUL : SLICE_GROUP bandes publiées si tout est déjà là (cf. SLICE_GROUP) ;
            # sinon une seule, pour publier au plus près de ce qui arrive.
            full = full or (SLICE_GROUP > 1 and _sl_all_full(need))
            n = min(SLICE_GROUP, _SL_NB - k) if full else 1
            a = k * SLICE_LINES; b = a + n * SLICE_LINES
            ca = a // _CH; cb = b // _CH
            # ─ Plan de la trame (cf. _out_plan) : le fond (transition comprise) dans une sortie,
            #   puis chaque couche posée une fois et recopiée là où les sorties se séparent ─
            for op in plan:
                if op[0] == "key":
                    _py, _pu, _pv = OP[op[2]]
                    wait_ns += _sl_key_one(kls[op[1]], _py, _pu, _pv, a, b, deadl, dbg)
                elif op[0] == "copy":
                    (_sy, _su, _sv), (_py, _pu, _pv) = OP[op[1]], OP[op[2]]
                    _py[a:b] = _sy[a:b]; _pu[ca:cb] = _su[ca:cb]; _pv[ca:cb] = _sv[ca:cb]
                else:
                    _py, _pu, _pv = OP[op[1]]
                    if blend_on:
                        wait_ns += _sl_wait(A, b - 1, deadl, dbg)
                        wait_ns += _sl_wait(B, b - 1, deadl, dbg)
                        _sl_blend_band(_py, _pu, _pv, A, B, bgd_alpha, bgd_mode, wm, a, b)
                    elif S is not None:
                        wait_ns += _sl_wait(S, b - 1, deadl, dbg)
                        _py[a:b] = S["y"][a:b]; _pu[ca:cb] = S["u"][ca:cb]; _pv[ca:cb] = S["v"][ca:cb]
                    else:
                        _py[a:b] = _BLACK_Yi; _pu[ca:cb] = _NEUT_Ci; _pv[ca:cb] = _NEUT_Ci
            if ovl is not None:
                _sl_overlay_band(OP[pg_t][0], OP[pg_t][1], OP[pg_t][2], ovl, a, b)
            # (PVW : fil PVW, cf. _pvw_frame — plus rien à faire ici pour lui)
            done = b
            k += n
            if n > 1: n_coarse += 1
            else: n_fine += 1
            if k < _SL_NB:   # commit PROGRESSIF (réveille l'aval) ; bande finale → finally
                for w_, g_ in zip(ows, ogr):
                    w_.commit(g_[1], valid_slices=k)
                if t_first is None:
                    t_first = time.monotonic()
    finally:
        # Complément : bandes jamais posées (exception à mi-trame) → noir, pour ne JAMAIS
        # publier de lignes indéterminées ; puis commit FINAL TOUJOURS (un grain laissé
        # partiel ne serait jamais lisible par un consommateur whole-frame).
        if done < HEIGHT:
            cd = done // _CH
            for _py, _pu2, _pv2 in OP:
                _py[done:] = _BLACK_Yi; _pu2[cd:] = _NEUT_Ci; _pv2[cd:] = _NEUT_Ci
        for _w, _g in zip(ows, ogr):
            try: _w.commit(_g[1], valid_slices=None)
            except Exception: pass
        _vid_idle.set()     # sorties programme publiées : le plan de contrôle peut passer
        _t_end = time.monotonic()
        _pgm_commit_ms.append((_t_end - t_frame0) * 1e3)
        _pgm_first_ms.append(((t_first or _t_end) - t_frame0) * 1e3)
        _sl_count_grouping("pgm", n_coarse, n_fine)
    if HAS_PVW and PVW_MODE == "after":
        _pvw_after(pvw_job)
    late = dict()
    # Entrées vues en retard par le FIL PVW (trames précédentes) : reversées ici au garde-fou n°2,
    # comme quand le PVW tournait dans cette boucle.
    with _pvw_cv:
        pvw_late = set(_pvw_late_slots)
        _pvw_late_slots.clear()
    # Un key lu à part (grain de son fill) compte pour son slot — une fois par trame.
    for s, st in list(srcs.items()) + [(ck[0], st) for ck, st in keysrcs.items()
                                       if srcs.get(ck[0]) is None]:
        if st is not None and s not in late:
            _lt = bool(st["late"]) or s in pvw_late
            late[s] = late.get(s, False) or _lt
            _in_frames[s] = _in_frames.get(s, 0) + 1
            if _lt:
                _in_late[s] = _in_late.get(s, 0) + 1
    _open_wired_inputs()
    _sl_dbg = dbg
    return wait_ns, late


def _reopen_outputs():
    """Nouvelle génération de toutes les sorties (saut d'horloge) : Writer.regenerate purge chaque
    flux puis le recrée — les lecteurs aval le retrouvent par leur garde de génération (écriture
    figée → réouverture). Pas de garbage_collect : il est GLOBAL au domaine et a déjà tué des flux
    vivants. Binding trop ancien (sans regenerate) : fermer et rouvrir, faute de mieux."""
    log("mixer: new generation of the outputs after a clock jump", "warning")
    with _pvw_busy:     # le fil PVW écrit dans out_pvw : jamais pendant qu'on la régénère
        _pvw_invalidate()
        _reopen_outputs_locked()


def _reopen_outputs_locked():
    ws = _all_writers()
    if all(hasattr(w, "regenerate") for w in ws):
        for w in ws:
            w.regenerate()
        return
    for w in ws:
        try: w.close()
        except Exception: pass
    _open_all_outputs()


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
    """Writer MXL de sortie (planar). Index TOUJOURS explicite à l'écriture (out_index, commun à
    toutes les sorties → programme(s) et PVW synchronisés), calculé par bobimxl.Writer.next_index — la grille TAI,
    qui ne dépend d'aucune entrée. MODE TRANCHE : le flowDef
    porte slice_height → libmxl publie le grain en N tranches égales (commit progressif) ; sans
    slice : inchangé."""
    return bobimxl.Writer(inst, name, WIDTH, HEIGHT, CHROMA, BIT_DEPTH, FPS, 1,
                          **(dict(slice_height=SLICE_LINES) if SLICE_ON else dict()))

def _all_writers():
    """Writers ouverts : les sorties programme, puis le PVW s'il existe (0.23.53)."""
    return [w for w in [_out_w(o) for o in OUTPUTS] + [globals().get("out_pvw")] if w is not None]


def _open_all_outputs():
    """Ouvre un writer par sortie programme (out_pgm, out_clean, out_o<id>) et le PVW s'il existe."""
    g = globals()
    for o in OUTPUTS:
        g[o["var"]] = _ouvrir_sortie(o["shm"])
    g["out_pvw"] = _ouvrir_sortie(SHM_OUT_PVW) if HAS_PVW else None


out_pgm = out_clean = out_pvw = None     # sortie 1 ou PVW absents (0.23.53) : None, jamais un fantôme
_open_all_outputs()
log("mixer: outputs " + ", ".join("%d=%s%s" % (o["id"], o["shm"], " (tally)" if o["tally"] else "")
                                  for o in OUTPUTS)
    + ("; PVW on output %d" % _PVW_BASE_INIT if HAS_PVW else "; no PVW output"), "info")
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

# Cœurs RÉELLEMENT utilisables (`_CPUS`, cf. PVW_MODE) publiés sur :8080 (`cpus`) :
# `containers.cores` dit ce qui a été demandé, pas ce qui est appliqué.
if SLICE_ON:
    if _CPUS <= 1 and PVW_MODE == "thread":
        log("mixer: ONE CPU available and pvw_mode=thread forced — the PVW thread cannot run in "
            f"parallel; it runs at nice +{{PVW_NICE}} but still competes for the GIL", "warning")
    else:
        log(f"mixer: {{_CPUS}} CPU(s) available — PVW mode {{PVW_MODE}}"
            + (f" (own thread, nice +{{PVW_NICE}})" if PVW_MODE == "thread"
               else " (main thread, after the PGM is published)"), "info")

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

_http_ready.set()      # module entièrement défini : les handlers HTTP peuvent servir (cf. _http_ready)
while True:
    _vid_idle.set()
    if bus_error.is_set():
        bus_error.clear()
        # Fil PVW (0.23.40) : rien n'est fermé pendant qu'il lit une entrée ou écrit out_pvw.
        _pvw_busy.acquire()
        _pvw_invalidate()
        try:
            for i in range(N_INPUTS):
                h = input_handles[i]
                if h is not None:
                    with _reader_lock:
                        input_handles[i] = None
                        try: h[0].close()
                        except Exception: pass
            for w in _all_writers():
                try: w.close()
                except Exception: pass
            # GC entre close et reopen (même parade que ensure_input : sans GC, la réouverture
            # peut retomber sur un flux orphelin résolvable par nom).
            try: inst.garbage_collect()
            except Exception: pass
            time.sleep(2)
            _open_all_outputs()
        finally:
            _pvw_busy.release()
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
                    _layer(_ki)["active"] = _kp["to_active"]
                state["transition"] = None
                pgm = state["pgm"]; pvw = state["pvw"]
                trans = None
        _finish_keyer_autos()
        keyer_state  = dict(state["keyer"])
        # COUCHES : keyers puis DSK (0.23.52 : l'habillage est le DSK 1), réglages + alpha + image
        # de page des couches web FIGÉS pour toute la trame (latch par grain). Les DSK sont en fin
        # de liste — composés après tous les keyers — et jamais sur le CLEAN (clean forcé à faux).
        keyers_now   = _latch_keyers(manual, trans)
        # PVW (0.23.29) : fond et keyers de l'état FUTUR, figés au même latch (cf. _pvw_future).
        pvw_bgd, pvw_keyers = _pvw_future(pgm, pvw, manual, trans, keyers_now)
        # SORTIES (0.23.53) : leurs couches et leur tally, figés au même latch.
        outs_now = _latch_outputs()
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
                               keyers_now, pvw_bgd, pvw_keyers, outs_now)

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
                    if _kn["alpha"] > 0.001 and _kn["type"] != "web":   # web : aucune entrée
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
    _vid_idle.clear()                 # composition : le plan de contrôle attend (cf. _ctl_quiet)
    ts_cycle_start = time.time_ns()   # début du compositing (réf détectée + alignée) → base OWN
    _lt_t = _lt_begin() if LT_ON else None   # fenêtre elapsed/cpu/wait/lost (cf. TEMPS PERDU)
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
    _mf_frame_start()                  # image manquante : une recopie comptée une fois par trame

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
        ovl = None          # (0.23.52) plus d'habillage à part : c'est le DSK 1, dans keyers_now
        _sl_late = dict()
        try:
            _sl_waited, _sl_late = _slice_frame(int(grid_now), None,
                                                bgd_active, bgd_alpha,
                                                bgd_from_idx, bgd_to_idx, bgd_preset,
                                                keyer_alpha_now, keyer_state, ovl,
                                                pgm, pvw_bgd, ts_in_per_slot, keyers=keyers_now,
                                                pvw_keyers=pvw_keyers, outs=outs_now)
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
            # (0.23.60) image manquante → dernière image valide de l'entrée en mode repeat
            f_from, ts_from = _read_whole(bgd_from_idx)
            f_to,   ts_to   = _read_whole(bgd_to_idx)
            if ts_from: ts_in_per_slot[bgd_from_idx] = ts_from
            if ts_to:   ts_in_per_slot[bgd_to_idx]   = ts_to
            if f_from and f_to:
                yuv_bgd = blend_yuv_preset(f_from, f_to, bgd_alpha, bgd_preset)
            else:
                yuv_bgd = f_to or f_from or empty_frame
        else:
            yuv_pgm_src, ts_pgm = _read_whole(pgm)
            if ts_pgm: ts_in_per_slot[pgm] = ts_pgm
            yuv_bgd = yuv_pgm_src or empty_frame

        # SORTIES (0.23.53) : chaque sortie = le fond + SES couches (keyers puis DSK, ordre de la
        # pile) ; même plan que le mode tranche (cf. _out_plan) — couches communes composées une
        # fois, chaque couche LUE une fois par trame. Copie à l'écriture : une sortie sans couche
        # publie le fond tel quel, sans copie (comme le CLEAN d'avant).
        _kw = [(_pos, _kn) for _pos, _kn in enumerate(keyers_now)
               if _kn["alpha"] > 0.001 and _sl_keyer_geom(_kn) is not None
               and any(_out_has(_o, _kn.get("_i", _pos), _kn) for _o in outs_now)]
        _plan = _out_plan([[_j for _j, (_pos, _kn) in enumerate(_kw) if _out_has(_o, _kn.get("_i", _pos), _kn)]
                           for _o in outs_now])
        _bufs = dict(); _own = set(); _kcache = dict()
        for _op in _plan:
            if _op[0] == "bg":
                _bufs[_op[1]] = yuv_bgd
            elif _op[0] == "copy":
                _bufs[_op[2]] = _bufs[_op[1]]
                _own.discard(_op[1])           # partagé désormais : la prochaine couche copie d'abord
            else:
                _t = _op[2]
                if _t not in _own:
                    _bufs[_t] = np.frombuffer(_bufs[_t], dtype=np.uint8).copy()
                    _own.add(_t)
                _whole_key_one(_planes_of(_bufs[_t]), _kw[_op[1]][1], ts_in_per_slot, _kcache, _op[1])

        # ─ Écriture des 3 shm ─
        # ⚠ CHEMIN WHOLE-FRAME (slice off, inéligible, ou RÉFÉRENCE PERDUE) : pas de délai
        # d'étage mesurable — `lire_frame_yuv` ne rend que (octets, ts), sans index de grain.
        # On observe quand même AVEC UNE LISTE VIDE : ça n'invente aucune mesure, mais ça COMPTE
        # le refus, donc `:8080` publie `non_mesurable` au lieu de rester muet. Sans ça, un mixer
        # qui produit sans mesurer était indiscernable d'un mixer à l'arrêt — constaté sur
        # `mixer-test` le 2026-08-19, qui tournait sur CE chemin (sync_ref_lost) pendant que je
        # croyais avoir instrumenté la seule sortie du plugin.
        delai_etage.observe(out_pgm, out_index, [])
        for _t, _o in enumerate(outs_now):
            _ecrire_sortie(_out_w(_o), _bufs.get(_t, yuv_bgd), out_index)
        _vid_idle.set()     # sorties programme publiées : le plan de contrôle peut passer
        _pgm_commit_ms.append((time.time_ns() - ts_cycle_start) / 1e6)
        # PGM et CLEAN sont PUBLIÉS : les keyers du PVW ne retardent plus le programme (0.23.40,
        # 2026-10-29 — avant, les trois sorties partaient ensemble après le PVW). Ils prennent
        # encore sur la période de la boucle : la garde PVW_BUDGET_FRAC reste donc en place.
        # ─ Bus PVW = fond futur + couches de SA sortie de base dans leur état futur (cf.
        #   _pvw_future). Aucune visible → la copie de l'entrée, comme avant 0.23.29. Sans PVW
        #   (0.23.53) : rien à lire, rien à écrire.
        if HAS_PVW:
            yuv_pvw_src, ts_pvw = _read_whole(pvw_bgd)
            if ts_pvw: ts_in_per_slot[pvw_bgd] = ts_pvw
            yuv_pvw = yuv_pvw_src or empty_frame
            if any(_kn["alpha"] > 0.001 for _kn in pvw_keyers):
                _t_pv = time.time_ns()
                if (_t_pv - ts_cycle_start) / 1e6 + _pvw_cost_ms > PVW_BUDGET_FRAC * 1000.0 / FPS:
                    _pvw_skips += 1          # PGM d'abord : le PVW sort sans ses keyers (cf. PVW_BUDGET_FRAC)
                else:
                    yuv_pvw = _compose_keyers_whole(yuv_pvw, pvw_keyers, ts_in_per_slot)
                    _pvw_cost_ms = (time.time_ns() - _t_pv) / 1e6
            _ecrire_sortie(out_pvw,   yuv_pvw,   out_index)
    ts_out = time.time_ns()
    _lt_end(_lt_t)

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
            # 0.23.40 : instant du commit FINAL du PGM depuis l'ouverture de la trame (ce que voit
            # le SDI), et le fil PVW du mode tranche (cf. POLITIQUE DE DÉPASSEMENT).
            _pc = list(_pgm_commit_ms)
            metrics["pgm_commit_ms"] = ({{"avg": round(sum(_pc) / len(_pc), 2), "max": round(max(_pc), 2)}}
                                        if _pc else None)
            metrics["cpus"] = _CPUS
            # Couches web (0.23.52) : coût mesuré par onglet + le Chromium entier (cf. overlay_loop).
            metrics["web_layers"] = _web_metrics()
            metrics["web_engine"] = dict(_web_engine)
            if SLICE_ON:
                # 0.23.41 : bandes de calcul regroupées — G, trames par chemin, et l'instant du
                # commit de la PREMIÈRE tranche PGM (ce qu'attend l'aval en mode tranche).
                _pf = list(_pgm_first_ms)
                metrics["slice_group"] = dict(g=SLICE_GROUP, lines=SLICE_GROUP * SLICE_LINES,
                                              pgm=dict(_sl_grouping["pgm"]),
                                              pvw=dict(_sl_grouping["pvw"]))
                metrics["pgm_first_slice_ms"] = ({{"avg": round(sum(_pf) / len(_pf), 2),
                                                   "max": round(max(_pf), 2)}} if _pf else None)
                _vc = list(_pvw_commit_ms); _vo = list(_pvw_own); _vd = _pvw_dbg
                metrics["pvw_thread"] = dict(_pvw_stats, mode=PVW_MODE,
                                      own_ms=(round(sum(_vo) / len(_vo), 2) if _vo else None),
                                      commit_ms=({{"avg": round(sum(_vc) / len(_vc), 2),
                                                  "max": round(max(_vc), 2)}} if _vc else None),
                                      slice=({{"waits": _vd[2], "fallbacks": _vd[3], "dormant": _vd[4]}}
                                             if _vd else None))
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
                # Sources non synchrones (0.23.57) : une seconde de plus, sur la mesure déjà faite.
                _us, _unsync_cnt = _unsync_step(metrics["inputs_timing"], _unsync_now, _unsync_cnt)
                if _us != _unsync_now:
                    log("mixer: inputs late on the default delay (not in a delay group): %s"
                        % (", ".join("#%d" % n for n in _us) or "none"), "info")
                    _unsync_now = _us
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
            # IMAGE MANQUANTE (0.23.60) : compteurs de recopie + avis au-delà du seuil (cf.
            # _mf_publish). Deux avis peuvent coexister avec celui du mode tranche : l'orchestrateur
            # accepte une LISTE (app/metrics.py), un seul champ aurait obligé à en taire un.
            _mf_pub, _mf_adv = _mf_publish()
            metrics.update(_mf_pub)
            _advs = [a for a in ((metrics.get("advisory") if SLICE_ON else None), _mf_adv) if a]
            metrics["advisory"] = (_advs[0] if len(_advs) == 1 else (_advs or None))
