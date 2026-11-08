"""Équivalence octet du compositing du mixer — DEUX propriétés, sur les 4 chemins.

(1) mvk (kernel C fusionné) ≡ repli numpy, pour chaque fonction de compose ;
(2) MODE TRANCHE ≡ PLEIN CADRE : la compose bande par bande (_sl_blend_band / _sl_keyer_band /
    _sl_overlay_band) doit produire EXACTEMENT les octets du chemin whole-frame (blend_yuv_preset
    / composite_keyer_yuv / composite_overlay) — propriété revendiquée par le mode tranche,
    désormais TESTÉE (elle ne l'était pas quand le chemin bandé était en numpy pur).

Méthode : rendre le script du plugin (str.format), en extraire par AST les fonctions de
compositing (exec nœud par nœud, garde-fous contre serveurs/threads/boucles), le charger DEUX
FOIS dans deux namespaces — une fois tel quel (_MVK=True → chemin C), une fois avec le kernel
neutralisé (bobimxl._mvk_lib=False → mvk_available() False → _MVK=False → numpy pur) — puis
comparer les octets sur une grille de cas (dissolve, additif, wipe × directions/softness, keyer
partiellement hors champ, overlay) en 8/10 bits et 420/422/444. Exit 0 = tout identique.

⚠ À exécuter DANS l'image runtime (libbobi_mvk présente), le script rendu étant fourni par
l'orchestrateur :
    ./venv/bin/python -c "from app import plugins; \
        open('/tmp/mx.py','w').write(plugins.render_script('mixer', CFG, 'equiv'))"
    docker run --rm -v /tmp/mx.py:/tmp/mx.py -v tools/equiv_mixer.py:/tmp/eq.py \
        --entrypoint python3 bobi-compute:0.13 /tmp/eq.py /tmp/mx.py
Sans argument, le script se rend lui-même via app.plugins (utile sur un contrôleur disposant
de la lib) pour les 3 profils ci-dessous.
"""
import ast, hashlib, itertools, json, sys

import numpy as np

NEEDED = {"blend_yuv", "blend_yuv_additive", "blend_yuv_wipe", "blend_yuv_preset",
          "composite_keyer_yuv", "_wipe_alpha_map", "_wipe_masks", "_wipe_compose",
          "prep_overlay", "composite_overlay",
          "_sl_blend_band", "_sl_keyer_band", "_sl_overlay_band", "_sl_keyer_geom",
          "_sl_stripe_band"}
BANNED = ("HTTPServer", "serve_forever", ".start()", "Instance(", "Writer(", "Reader(",
          "socket.", "while True", "signal.signal")


def load_ns(src, use_mvk):
    """Exec du script rendu, nœud par nœud, dans un namespace neuf. use_mvk=False → le kernel
    est neutralisé AVANT l'exec (bobimxl._mvk_lib = False : _mvk_load() renvoie None sans
    tenter le chargement), donc _MVK est calculé à False → chemins numpy purs."""
    import bobimxl
    bobimxl._mvk_lib = None if use_mvk else False
    tree = ast.parse(src)
    ns = {"__name__": "mixer_equiv"}
    for node in tree.body:
        seg = ast.unparse(node)
        if any(b in seg for b in BANNED):
            continue
        try:
            exec(compile(ast.Module([node], []), "<mixer>", "exec"), ns)
        except Exception:
            pass
    missing = NEEDED - set(ns)
    if missing:
        raise SystemExit("fonctions manquantes dans le rendu : %s" % sorted(missing))
    if bool(ns["_MVK"]) != use_mvk:
        raise SystemExit("_MVK=%s alors qu'on demandait use_mvk=%s (lib absente ?)"
                         % (ns["_MVK"], use_mvk))
    return ns


def cases(ns, band_lines):
    """Renvoie {clé: sha256(octets)} pour tous les cas de compose, chemins PLEIN et TRANCHE."""
    W, H = ns["WIDTH"], ns["HEIGHT"]
    UV_W, UV_H = ns["UV_W"], ns["UV_H"]
    Y_SIZE, UV_SIZE = ns["Y_SIZE"], ns["UV_SIZE"]
    _NP_DT, _MAXV, _CH, _CW = ns["_NP_DT"], ns["_MAXV"], ns["_CH"], ns["_CW"]
    rng = np.random.default_rng(42)

    def rand_frame():
        y = rng.integers(16, _MAXV - 15, size=(H, W), dtype=np.int64).astype(_NP_DT)
        u = rng.integers(0, _MAXV, size=(UV_H, UV_W), dtype=np.int64).astype(_NP_DT)
        v = rng.integers(0, _MAXV, size=(UV_H, UV_W), dtype=np.int64).astype(_NP_DT)
        return y.tobytes() + u.tobytes() + v.tobytes()

    def as_st(buf):
        """Vue plans d'une frame — forme de l'état par-entrée du compose bandé (_sl_*)."""
        pl = np.frombuffer(buf, dtype=np.uint8)
        return dict(y=pl[:Y_SIZE].view(_NP_DT).reshape(H, W),
                    u=pl[Y_SIZE:Y_SIZE + UV_SIZE].view(_NP_DT).reshape(UV_H, UV_W),
                    v=pl[Y_SIZE + UV_SIZE:].view(_NP_DT).reshape(UV_H, UV_W))

    fa, fb = rand_frame(), rand_frame()
    keyer_src, bgd = rand_frame(), rand_frame()
    ovl_rgba = rng.integers(0, 256, size=(H, W, 4), dtype=np.int64).astype(np.uint8)
    ovl = ns["prep_overlay"](ovl_rgba)
    A, B, Ksrc = as_st(fa), as_st(fb), as_st(keyer_src)
    bands = [(k * band_lines, (k + 1) * band_lines) for k in range(H // band_lines)]
    out = {}

    def blank():
        return (np.empty((H, W), _NP_DT), np.empty((UV_H, UV_W), _NP_DT),
                np.empty((UV_H, UV_W), _NP_DT))

    def pack(y, u, v):
        return y.tobytes() + u.tobytes() + v.tobytes()

    # ─ transitions : plein cadre vs bandé ─
    presets = [("dissolve", None), ("additif", None)]
    for direction, soft in itertools.product(("horizontal", "vertical", "diagonal"), (0.0, 0.1)):
        for reverse in (False, True):
            presets.append(("wipe", dict(direction=direction, reverse=reverse, softness=soft)))
    # 0.23.44 : formes, angle, centre, répétition, bordure (troisième couche).
    for direction in ("iris", "star", "heart", "clock", "box", "diamond"):
        presets.append(("wipe", dict(direction=direction, reverse=False, softness=0.1, angle=30.0,
                                     center_x=0.4, repeat=2)))
        presets.append(("wipe", dict(direction=direction, reverse=True, softness=0.0,
                                     border_width=0.1, border_color="#ff8000",
                                     border_soft_in=0.2, border_soft_out=0.05)))
    for mode, wcfg in presets:
        for alpha in (0.0, 0.13, 0.5, 0.777, 1.0):
            tag = "%s_%s_%s" % (mode, alpha, wcfg and json.dumps(wcfg, sort_keys=True))
            preset = dict(mode=mode, wipe=(wcfg or dict(direction="horizontal",
                                                        reverse=False, softness=0.10)))
            out["full_" + tag] = hashlib.sha256(
                ns["blend_yuv_preset"](fa, fb, alpha, preset)).hexdigest()
            wm = None
            if mode == "wipe":
                wm = ns["_wipe_masks"](alpha, wcfg)
            y, u, v = blank()
            for a, b in bands:
                ns["_sl_blend_band"](y, u, v, A, B, alpha, mode, wm, a, b)
            out["band_" + tag] = hashlib.sha256(pack(y, u, v)).hexdigest()

    # ─ keyer : plein cadre vs bandé (rect partiellement hors champ, bordure) ─
    for i, (kx, ky, kscale, bw, ka) in enumerate([
            (0.5, 0.5, 0.3, 0, 1.0), (0.02, 0.02, 0.25, 4, 0.6), (0.98, 0.98, 0.25, 6, 1.0),
            (0.5, 0.02, 0.5, 2, 0.42), (0.02, 0.5, 0.6, 0, 0.9), (0.5, 0.5, 1.4, 0, 1.0)]):
        out["full_keyer_%d" % i] = hashlib.sha256(
            ns["composite_keyer_yuv"](bgd, keyer_src, ka, kx, ky, kscale, bw)).hexdigest()
        ks = dict(x=kx, y=ky, scale=kscale, border_width=bw, active=True, input=0)
        g = ns["_sl_keyer_geom"](ks)
        st = as_st(bgd)
        y, u, v = blank()
        y[:] = st["y"]; u[:] = st["u"]; v[:] = st["v"]
        if g is not None:
            for a, b in bands:
                ns["_sl_keyer_band"](y, u, v, Ksrc, g, ka, a, b)
        out["band_keyer_%d" % i] = hashlib.sha256(pack(y, u, v)).hexdigest()

    # ─ overlay : plein cadre vs bandé ─
    out["full_overlay"] = hashlib.sha256(ns["composite_overlay"](bgd, ovl)).hexdigest()
    st = as_st(bgd)
    y, u, v = blank()
    y[:] = st["y"]; u[:] = st["u"]; v[:] = st["v"]
    for a, b in bands:
        ns["_sl_overlay_band"](y, u, v, ovl, a, b)
    out["band_overlay"] = hashlib.sha256(pack(y, u, v)).hexdigest()

    # ─ IDENTITÉ de l'overlay transparent : alpha=0 ⇒ la source doit ressortir INTACTE
    #   (le roundtrip YUV→RGB→YUV d'avant 0.13.0 échouait ici : clip+requantif de TOUTE l'image) ─
    ovl0 = ns["prep_overlay"](np.zeros((H, W, 4), dtype=np.uint8))
    out["overlay_transparent_is_identity"] = hashlib.sha256(
        ns["composite_overlay"](bgd, ovl0)).hexdigest()
    out["_ref_bgd"] = hashlib.sha256(bgd).hexdigest()
    return out


def render(cfg):
    sys.path.insert(0, "/opt/bobistudio")
    from app import plugins
    return plugins.render_script("mixer", cfg, "equivtest")


PROFILS = (
    ("8 bits 422", dict(width=64, height=32, chroma="422", bit_depth=8)),
    ("10 bits 420", dict(width=64, height=32, chroma="420", bit_depth=10)),
    ("8 bits 444", dict(width=64, height=32, chroma="444", bit_depth=8)),
)

total = 0
srcs = ([("script fourni", open(sys.argv[1]).read())] if len(sys.argv) > 1
        else [(lbl, render(cfg)) for lbl, cfg in PROFILS])

for label, src in srcs:
    print("== profil", label)
    with_mvk = cases(load_ns(src, True), band_lines=4)
    without = cases(load_ns(src, False), band_lines=4)

    fails = [k for k in with_mvk if with_mvk[k] != without[k]]
    if fails:
        print("  FAIL mvk ≠ numpy :", fails); total += len(fails)
    else:
        print("  OK  mvk ≡ numpy — %d cas" % len(with_mvk))

    for tbl, nm in ((with_mvk, "mvk"), (without, "numpy")):
        bad = [k[5:] for k in tbl if k.startswith("full_")
               and tbl[k] != tbl.get("band_" + k[5:])]
        if bad:
            print("  FAIL tranche ≠ plein cadre (%s) :" % nm, bad); total += len(bad)
        else:
            print("  OK  tranche ≡ plein cadre (%s)" % nm)
        if tbl["overlay_transparent_is_identity"] != tbl["_ref_bgd"]:
            print("  FAIL overlay transparent ≠ identité (%s)" % nm); total += 1
        else:
            print("  OK  overlay transparent = identité (%s)" % nm)

print("Échecs :", total)
sys.exit(1 if total else 0)
