import mmap, struct, time, threading, json, os, signal, io
from collections import deque
import numpy as np
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

lat_in = {{}}  # {{shm_name: RollingMs}}

# ─── Config injectée (contrat plugin) ───────────────────────
CONFIG         = {config}
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

N_INPUTS      = max(1, min(8, int(CONFIG.get("n_inputs") or 2)))   # 1..8
SHM_OUT_PGM   = CONFIG.get("shm_out_pgm")   or (HOSTNAME + "_pgm")    # PGM : vidéo + overlay
SHM_OUT_CLEAN = CONFIG.get("shm_out_clean") or (HOSTNAME + "_clean")  # PGM clean (sans overlay)
SHM_OUT_PVW   = CONFIG.get("shm_out_pvw")   or (HOSTNAME + "_pvw")    # PVW (preview)
WIDTH       = int(CONFIG.get("width") or 1280);  WIDTH  -= WIDTH % 2
HEIGHT      = int(CONFIG.get("height") or 720);  HEIGHT -= HEIGHT % 2
FPS         = int(round(float(CONFIG.get("fps") or 25))) or 25
DEFAULT_DISSOLVE_MS = int(CONFIG.get("dissolve_ms") or 500)
INITIAL_OVERLAY_URL     = CONFIG.get("overlay_url") or None
INITIAL_OVERLAY_ENABLED = bool(CONFIG.get("overlay_enabled"))

# ─── Format yuv420p/422p/444p (selon CONFIG[chroma], défaut 4:2:2) ──
CHROMA = str(CONFIG.get("chroma") or "422")
_CW = {{"420": 2, "422": 2, "444": 1}}.get(CHROMA, 2)   # diviseur largeur chroma
_CH = {{"420": 2, "422": 1, "444": 1}}.get(CHROMA, 1)   # diviseur hauteur chroma
PIX_FMT = {{"420": "yuv420p", "422": "yuv422p", "444": "yuv444p"}}.get(CHROMA, "yuv422p")
UV_W = WIDTH // _CW
UV_H = HEIGHT // _CH
V_HEADER_SIZE = 64
V_RING_SIZE   = 10
Y_SIZE        = WIDTH * HEIGHT
UV_SIZE       = UV_W * UV_H
V_FRAME_SIZE  = Y_SIZE + 2 * UV_SIZE
V_TOTAL_SIZE  = V_HEADER_SIZE + V_RING_SIZE * V_FRAME_SIZE

# ─── État runtime ───────────────────────────────────────────
state_lock = threading.Lock()
state = {{
    "pgm": 0,
    "pvw": 1 if N_INPUTS > 1 else 0,
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
    "inputs":          [str(CONFIG.get("input_%d" % i) or "") for i in range(N_INPUTS)],
    "overlay_url":     INITIAL_OVERLAY_URL,
    "overlay_enabled": bool(INITIAL_OVERLAY_ENABLED),
}}

def _snapshot_preset():
    """Copie défensive du preset courant — capté au moment de créer un mix."""
    p = state["preset"]
    return {{"mode": p["mode"], "wipe": dict(p["wipe"])}}

# Overlay : (H, W, 4) uint8 RGBA, mis à jour par overlay_loop
overlay_lock = threading.Lock()
overlay_rgba = None

# Métriques
metrics_lock = threading.Lock()
metrics = {{"fps": 0.0, "frame_index": 0, "pgm": 0, "pvw": 1 if N_INPUTS > 1 else 0, "inputs_latency_ms": {{}}}}

# SIGBUS handling
bus_error = threading.Event()
def _handle_sigbus(signum, frame):
    print("SIGBUS reçu — réouverture des mmap")
    bus_error.set()
signal.signal(signal.SIGBUS, _handle_sigbus)


# ─── HTTP : /metrics (8080) et control (8082) ───────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with metrics_lock: payload = dict(metrics)
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
                    "transition":      bool(tr),
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
                    "overlay_url":     state["overlay_url"],
                    "overlay_enabled": state["overlay_enabled"],
                    "default_dissolve_ms": DEFAULT_DISSOLVE_MS,
                    "plugin_version": PLUGIN_VERSION,
                }}
                # state_field par slot (topologie / câblage générique)
                for _i in range(N_INPUTS):
                    s["input_%d" % _i] = state["inputs"][_i]
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
            if duration is None: duration = DEFAULT_DISSOLVE_MS
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
    global overlay_rgba
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/playwright-browsers")
    try:
        from playwright.sync_api import sync_playwright
        from PIL import Image
    except ImportError as e:
        print(f"playwright/PIL indisponible — overlay HTML désactivé ({{e}})")
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
                if want_url:
                    try:
                        page = ctx.new_page()
                        page.goto(want_url, timeout=10000, wait_until="domcontentloaded")
                        page.evaluate(
                            "document.documentElement.style.background='transparent';"
                            "document.body && (document.body.style.background='transparent');"
                        )
                    except Exception as e:
                        print(f"playwright goto erreur ({{want_url}}): {{e}}")
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
                    with overlay_lock:
                        overlay_rgba = np.asarray(img, dtype=np.uint8)
                except Exception as e:
                    print(f"playwright screenshot erreur: {{e}}")
                    time.sleep(0.5)
            time.sleep(0.040)
    finally:
        pw.stop()

threading.Thread(target=overlay_loop, daemon=True).start()


# ─── Wiring dynamique des inputs ────────────────────────────
# Chaque slot peut être (re)câblé à chaud via POST /input.
# input_handles[idx] = (file, mmap, name_actuellement_ouvert)
input_handles = [None] * N_INPUTS

def ensure_input(idx):
    """Renvoie le mmap pour l'input idx, en (ré-)ouvrant si le shm name a changé.
    Renvoie None si le slot n'est pas câblé ou le shm est indisponible."""
    with state_lock:
        wanted = state["inputs"][idx]
    cur = input_handles[idx]
    if not wanted:
        if cur is not None:
            try: cur[1].close(); cur[0].close()
            except Exception: pass
            input_handles[idx] = None
        return None
    if cur is not None and cur[2] == wanted:
        return cur[1]
    # Changement de câblage → fermer puis ouvrir
    if cur is not None:
        try: cur[1].close(); cur[0].close()
        except Exception: pass
        input_handles[idx] = None
    path = f"/dev/shm/{{wanted}}"
    try:
        if not os.path.exists(path): return None
        if os.path.getsize(path) < V_TOTAL_SIZE: return None
        f = open(path, "r+b")
        shm = mmap.mmap(f.fileno(), V_TOTAL_SIZE, prot=mmap.PROT_READ)
        _ = shm[0:16]
        input_handles[idx] = (f, shm, wanted)
        print(f"input #{{idx}} câblé sur {{path}}")
        return shm
    except Exception as e:
        print(f"input #{{idx}} ({{wanted}}) indisponible : {{e}}")
        return None


def lire_frame_yuv(shm):
    """Lit la frame la plus récente. Renvoie (bytes, ts_in_ns) ou (None, None)."""
    if shm is None: return None, None
    try:
        frame_index, ts_in = struct.unpack("QQ", shm[0:16])
        if frame_index == 0: return None, None
        slot = frame_index % V_RING_SIZE
        offset = V_HEADER_SIZE + slot * V_FRAME_SIZE
        return bytes(shm[offset:offset + V_FRAME_SIZE]), ts_in
    except Exception:
        return None, None


# ─── Compositing helpers ────────────────────────────────────
def blend_yuv(yuv_a, yuv_b, alpha):
    """Dissolve classique (planes Y/U/V en moyenne pondérée). Total des contributions = 1."""
    a = np.frombuffer(yuv_a, dtype=np.uint8).astype(np.float32)
    b = np.frombuffer(yuv_b, dtype=np.uint8).astype(np.float32)
    out = a * (1.0 - alpha) + b * alpha
    return out.astype(np.uint8).tobytes()

def blend_yuv_additive(yuv_a, yuv_b, alpha):
    """Mix additif : à α=0.5 les deux luma s'ajoutent (clip 255). Chroma reste en blend
    pondéré pour ne pas saturer la couleur. Coef A descend de 1 à 0 entre α=0.5 et α=1 ;
    Coef B monte de 0 à 1 entre α=0 et α=0.5, puis reste à 1."""
    coef_a = min(1.0, 2.0 * (1.0 - alpha))
    coef_b = min(1.0, 2.0 * alpha)
    Ya = np.frombuffer(yuv_a[:Y_SIZE], dtype=np.uint8).astype(np.float32)
    Yb = np.frombuffer(yuv_b[:Y_SIZE], dtype=np.uint8).astype(np.float32)
    out_y = np.clip(Ya * coef_a + Yb * coef_b, 0, 255).astype(np.uint8).tobytes()
    Ua = np.frombuffer(yuv_a[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).astype(np.float32)
    Ub = np.frombuffer(yuv_b[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).astype(np.float32)
    Va = np.frombuffer(yuv_a[Y_SIZE+UV_SIZE:], dtype=np.uint8).astype(np.float32)
    Vb = np.frombuffer(yuv_b[Y_SIZE+UV_SIZE:], dtype=np.uint8).astype(np.float32)
    out_u = (Ua * (1.0 - alpha) + Ub * alpha).astype(np.uint8).tobytes()
    out_v = (Va * (1.0 - alpha) + Vb * alpha).astype(np.uint8).tobytes()
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
    print("Précalcul du wipe Cœur...")
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
    print(f"Wipe Cœur prêt en {{time.time() - t0:.1f}}s")

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
    a_half = a_full[::_CH, ::_CW]
    Ya = np.frombuffer(yuv_a[:Y_SIZE], dtype=np.uint8).reshape(HEIGHT, WIDTH).astype(np.float32)
    Yb = np.frombuffer(yuv_b[:Y_SIZE], dtype=np.uint8).reshape(HEIGHT, WIDTH).astype(np.float32)
    out_y = (Ya * (1.0 - a_full) + Yb * a_full).astype(np.uint8).tobytes()
    Ua = np.frombuffer(yuv_a[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).reshape(UV_H, UV_W).astype(np.float32)
    Ub = np.frombuffer(yuv_b[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).reshape(UV_H, UV_W).astype(np.float32)
    out_u = (Ua * (1.0 - a_half) + Ub * a_half).astype(np.uint8).tobytes()
    Va = np.frombuffer(yuv_a[Y_SIZE+UV_SIZE:], dtype=np.uint8).reshape(UV_H, UV_W).astype(np.float32)
    Vb = np.frombuffer(yuv_b[Y_SIZE+UV_SIZE:], dtype=np.uint8).reshape(UV_H, UV_W).astype(np.float32)
    out_v = (Va * (1.0 - a_half) + Vb * a_half).astype(np.uint8).tobytes()
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


def yuv_to_rgb_arr(yuv_bytes):
    y = np.frombuffer(yuv_bytes[:Y_SIZE], dtype=np.uint8).reshape(HEIGHT, WIDTH).astype(np.int32)
    u = np.frombuffer(yuv_bytes[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).reshape(UV_H, UV_W)
    v = np.frombuffer(yuv_bytes[Y_SIZE+UV_SIZE:],       dtype=np.uint8).reshape(UV_H, UV_W)
    u = u.repeat(_CH, axis=0).repeat(_CW, axis=1).astype(np.int32)
    v = v.repeat(_CH, axis=0).repeat(_CW, axis=1).astype(np.int32)
    c = y - 16; d = u - 128; e = v - 128
    r = np.clip((298*c + 409*e + 128) >> 8, 0, 255).astype(np.uint8)
    g = np.clip((298*c - 100*d - 208*e + 128) >> 8, 0, 255).astype(np.uint8)
    b = np.clip((298*c + 516*d + 128) >> 8, 0, 255).astype(np.uint8)
    return np.stack([r, g, b], axis=2)


def rgb_to_yuv_bytes(rgb):
    r = rgb[:,:,0].astype(np.int32); g = rgb[:,:,1].astype(np.int32); b = rgb[:,:,2].astype(np.int32)
    y_p = np.clip(((66*r + 129*g + 25*b + 128) >> 8) + 16, 0, 255).astype(np.uint8)
    u_p = np.clip(((-38*r - 74*g + 112*b + 128) >> 8) + 128, 0, 255).astype(np.uint8)
    v_p = np.clip(((112*r - 94*g - 18*b + 128) >> 8) + 128, 0, 255).astype(np.uint8)
    return y_p.tobytes() + u_p[::_CH,::_CW].tobytes() + v_p[::_CH,::_CW].tobytes()


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
    src_y = np.frombuffer(keyer_yuv[:Y_SIZE], dtype=np.uint8).reshape(HEIGHT, WIDTH)
    src_u = np.frombuffer(keyer_yuv[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).reshape(UV_H, UV_W)
    src_v = np.frombuffer(keyer_yuv[Y_SIZE+UV_SIZE:], dtype=np.uint8).reshape(UV_H, UV_W)
    ix  = (np.arange(dst_w) * WIDTH  // dst_w)
    iy  = (np.arange(dst_h) * HEIGHT // dst_h)
    k_y_full = src_y[iy[:, None], ix[None, :]]
    ix2 = (np.arange(dst_w // _CW) * UV_W // (dst_w // _CW))
    iy2 = (np.arange(dst_h // _CH) * UV_H // (dst_h // _CH))
    k_u_full = src_u[iy2[:, None], ix2[None, :]]
    k_v_full = src_v[iy2[:, None], ix2[None, :]]

    # Crop à la zone visible
    k_y = k_y_full[src_y_off:src_y_off+vis_h, src_x_off:src_x_off+vis_w]
    k_u = k_u_full[src_y_off//_CH:(src_y_off+vis_h)//_CH, src_x_off//_CW:(src_x_off+vis_w)//_CW]
    k_v = k_v_full[src_y_off//_CH:(src_y_off+vis_h)//_CH, src_x_off//_CW:(src_x_off+vis_w)//_CW]

    # Composite dans bgd
    bgd_y = np.frombuffer(bgd_yuv[:Y_SIZE], dtype=np.uint8).reshape(HEIGHT, WIDTH).copy()
    bgd_u = np.frombuffer(bgd_yuv[Y_SIZE:Y_SIZE+UV_SIZE], dtype=np.uint8).reshape(UV_H, UV_W).copy()
    bgd_v = np.frombuffer(bgd_yuv[Y_SIZE+UV_SIZE:], dtype=np.uint8).reshape(UV_H, UV_W).copy()

    reg_y = bgd_y[out_y0:out_y1, out_x0:out_x1].astype(np.float32)
    bgd_y[out_y0:out_y1, out_x0:out_x1] = (reg_y * (1.0 - keyer_alpha) +
                                            k_y.astype(np.float32) * keyer_alpha).astype(np.uint8)
    reg_u = bgd_u[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW].astype(np.float32)
    bgd_u[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW] = (reg_u * (1.0 - keyer_alpha) +
                                                       k_u.astype(np.float32) * keyer_alpha).astype(np.uint8)
    reg_v = bgd_v[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW].astype(np.float32)
    bgd_v[out_y0//_CH:out_y1//_CH, out_x0//_CW:out_x1//_CW] = (reg_v * (1.0 - keyer_alpha) +
                                                       k_v.astype(np.float32) * keyer_alpha).astype(np.uint8)

    # Bordure blanche (BT.601 : Y=235, U=V=128). Tracée avec le même alpha.
    if border_w > 0:
        bw = int(border_w)
        def _stripe(y0_, y1_, x0_, x1_):
            if y1_ <= y0_ or x1_ <= x0_: return
            reg = bgd_y[y0_:y1_, x0_:x1_].astype(np.float32)
            bgd_y[y0_:y1_, x0_:x1_] = (reg * (1.0 - keyer_alpha) + 235.0 * keyer_alpha).astype(np.uint8)
            reg = bgd_u[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW].astype(np.float32)
            bgd_u[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW] = (reg * (1.0 - keyer_alpha) + 128.0 * keyer_alpha).astype(np.uint8)
            reg = bgd_v[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW].astype(np.float32)
            bgd_v[y0_//_CH:y1_//_CH, x0_//_CW:x1_//_CW] = (reg * (1.0 - keyer_alpha) + 128.0 * keyer_alpha).astype(np.uint8)
        bx0 = max(0, x0); bx1 = min(WIDTH, x0 + dst_w)
        by0 = max(0, y0); by1 = min(HEIGHT, y0 + dst_h)
        _stripe(max(0, y0),                   min(HEIGHT, y0 + bw),        bx0, bx1)  # haut
        _stripe(max(0, y0 + dst_h - bw),      min(HEIGHT, y0 + dst_h),     bx0, bx1)  # bas
        _stripe(by0, by1, max(0, x0),                 min(WIDTH,  x0 + bw))             # gauche
        _stripe(by0, by1, max(0, x0 + dst_w - bw),    min(WIDTH,  x0 + dst_w))          # droite

    return bgd_y.tobytes() + bgd_u.tobytes() + bgd_v.tobytes()


def composite_overlay(yuv_bytes, ovl_rgba):
    """Compose un overlay RGBA sur une frame yuv (selon chroma) (roundtrip RGB)."""
    rgb = yuv_to_rgb_arr(yuv_bytes).astype(np.float32)
    a = ovl_rgba[:, :, 3:4].astype(np.float32) / 255.0
    rgb_ovl = ovl_rgba[:, :, :3].astype(np.float32)
    out = rgb * (1.0 - a) + rgb_ovl * a
    return rgb_to_yuv_bytes(np.clip(out, 0, 255).astype(np.uint8))


# ─── Boucle principale ──────────────────────────────────────
def _ouvrir_sortie(name):
    path = f"/dev/shm/{{name}}"
    with open(path, "wb") as f:
        f.write(b"\x00" * V_TOTAL_SIZE)
    f = open(path, "r+b")
    return f, mmap.mmap(f.fileno(), V_TOTAL_SIZE)

out_pgm_f,   out_pgm   = _ouvrir_sortie(SHM_OUT_PGM)
out_clean_f, out_clean = _ouvrir_sortie(SHM_OUT_CLEAN)
out_pvw_f,   out_pvw   = _ouvrir_sortie(SHM_OUT_PVW)

def _ecrire_sortie(shm, yuv, frame_index):
    slot = frame_index % V_RING_SIZE
    offset = V_HEADER_SIZE + slot * V_FRAME_SIZE
    shm[offset:offset + V_FRAME_SIZE] = yuv
    shm[0:16] = struct.pack("QQ", frame_index, time.time_ns())

frame_index = 0
start = time.time()
next_t = start
interval = 1.0 / FPS
empty_frame = b"\x10" * Y_SIZE + b"\x80" * (2 * UV_SIZE)   # noir yuv

while True:
    if bus_error.is_set():
        bus_error.clear()
        for i in range(N_INPUTS):
            h = input_handles[i]
            if h is not None:
                try: h[1].close(); h[0].close()
                except Exception: pass
                input_handles[i] = None
        for fh, sh in ((out_pgm_f, out_pgm), (out_clean_f, out_clean), (out_pvw_f, out_pvw)):
            try: sh.close(); fh.close()
            except Exception: pass
        time.sleep(2)
        out_pgm_f,   out_pgm   = _ouvrir_sortie(SHM_OUT_PGM)
        out_clean_f, out_clean = _ouvrir_sortie(SHM_OUT_CLEAN)
        out_pvw_f,   out_pvw   = _ouvrir_sortie(SHM_OUT_PVW)
        continue

    now = time.time()
    wait = next_t - now
    if wait > 0: time.sleep(wait)

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
            ovl = overlay_rgba
        if ovl is not None and ovl.shape == (HEIGHT, WIDTH, 4):
            yuv_pgm = composite_overlay(yuv_pgm, ovl)

    # ─ Bus PVW (entrée en preview, sans overlay) ─
    yuv_pvw_src, ts_pvw = lire_frame_yuv(ensure_input(pvw))
    if ts_pvw: ts_in_per_slot[pvw] = ts_pvw
    yuv_pvw = yuv_pvw_src or empty_frame

    # ─ Écriture des 3 shm ─
    _ecrire_sortie(out_pgm,   yuv_pgm,   frame_index)
    _ecrire_sortie(out_clean, yuv_clean, frame_index)
    _ecrire_sortie(out_pvw,   yuv_pvw,   frame_index)
    ts_out = time.time_ns()

    # Latence par slot consommé : Δ ts_out - ts_in, indexé par shm_name câblé
    with state_lock:
        slot_to_shm = list(state["inputs"])
    for slot_idx, ts_in in ts_in_per_slot.items():
        shm_name = slot_to_shm[slot_idx] if slot_idx < len(slot_to_shm) else ""
        if not shm_name:
            continue
        if shm_name not in lat_in:
            lat_in[shm_name] = RollingMs()
        lat_in[shm_name].push((ts_out - ts_in) / 1e6)

    frame_index += 1
    next_t = start + frame_index * interval

    if frame_index % FPS == 0:
        elapsed = time.time() - start
        with metrics_lock:
            metrics["fps"] = round(frame_index / elapsed, 1)
            metrics["frame_index"] = frame_index
            metrics["pgm"] = pgm
            metrics["pvw"] = pvw
            metrics["inputs_latency_ms"] = {{k: v.avg() for k, v in lat_in.items()}}
