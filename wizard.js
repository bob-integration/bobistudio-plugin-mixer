// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// Mélangeur — ASSISTANT DE CONFIGURATION (0.23.56, décisions de Cyril du 29/10, maquette v3).
// Chargé à la demande par control.js (manifeste ui.wizard_js) : à la PREMIÈRE OUVERTURE d'un mélangeur
// neuf (marqueur `setup_pending`, posé par le formulaire de création) pour qui a le droit de déployer,
// puis par ⚙ → Assistant. Le SCHÉMA est l'interface : fond en ligne basse (entrées → BGD A/B →
// transition → sorties), couches empilées au-dessus (keyers sur le fond, DSK après), sources des
// couches prises aux ENTRÉES (faisceau : rail vertical commun, key en tirets) ; un fil guidé la
// première fois. Rien n'est appliqué avant « Appliquer » :
//   · POST /api/containers/<vmid>/control/setup (hook `setup`) : structure, couches, sorties — le
//     serveur dit s'il faut redémarrer (↻, confirmation « l'antenne sera coupée quelques secondes »,
//     sauf à la première configuration) ;
//   · à chaud, les endpoints HABITUELS : /plugin/keyer/type|input|web, /plugin/dsk/<n>/…,
//     /plugin/latency (délai par défaut), /plugin_config (tally), /api/atem/assign (pupitre).
// 0.23.57 (retours de Cyril, 30/10) : la TRANSITION n'est plus réglée ici — « c'est de l'exploitation
// de dire si on veut un mix ou un wipe » ; son bloc reste DESSINÉ (chemin du signal) mais renvoie à la
// rubrique Transitions. Les SOURCES SYNCHRONES ont leur encart dans le panneau des entrées : ce que
// c'est, l'état mesuré (entrées en retard sur le délai par défaut, /state.unsync_inputs), le délai
// par défaut réglable à chaud, et le renvoi vers l'onglet Entrées & délais pour les groupes.
// L'assistant n'ouvre aucun chemin de configuration nouveau : tout ce qu'il règle reste pilotable
// par ces endpoints et par les macros. Le câblage des sources reste dans la page Câbles.
'use strict';
window.MXLMixerWizard = (function () {
    const MAX_K = 4, MAX_D = 2, MAX_O = 4, MAX_IN = 20, NAME_MAX = 24;
    const KTYPES = ['dve', 'linear', 'luma', 'chroma', 'pattern', 'web'];
    let UIDN = 0;
    const uid = (p) => p + '_' + (++UIDN);
    const clone = (o) => JSON.parse(JSON.stringify(o));
    const range = (n) => Array.from({ length: Math.max(0, n) }, (_, i) => i);
    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

    /* ═════════ Modèle (fonctions pures, exportées pour la garde) ═════════ */

    // Suffixe du flux d'une sortie : MÊME règle que hooks.py:output_slug (le serveur fait foi).
    function slug(name, id) {
        let s = String(name || '').normalize('NFKD').replace(/[^\x00-\x7f]/g, '').toLowerCase().replace(/[^a-z0-9_]/g, '_');
        while (s.includes('__')) s = s.replace(/__/g, '_');
        s = s.replace(/^_+|_+$/g, '').slice(0, NAME_MAX).replace(/^_+|_+$/g, '');
        return s || ('out' + id);
    }
    const asInt = (v, d) => { const n = parseInt(v, 10); return Number.isFinite(n) ? n : d; };

    // Configuration de l'assistant depuis l'état vivant (/state) et les params persistés.
    function fromLive(st, p) {
        st = st || {}; p = p || {};
        const n = Math.max(1, Math.min(MAX_IN, asInt(st.n_inputs, asInt(p.n_inputs, 2))));
        const layer = (k, i, fam) => ({ uid: fam + (i + 1), from: i, t: KTYPES.includes(k && k.type) ? k.type : (fam === 'd' ? 'web' : 'dve'),
            fill: Math.min(n - 1, Math.max(0, asInt(k && k.input, 0))),
            key: Math.min(n - 1, Math.max(0, asInt(k && k.key_input, asInt(k && k.input, 0)))), url: (k && k.url) || '' });
        let ks = Array.isArray(st.keyers) && st.keyers.length ? st.keyers : range(asInt(p.n_keyers, 1)).map(() => ({}));
        let ds = Array.isArray(st.dsks) ? st.dsks : range(p.n_dsk == null ? 1 : asInt(p.n_dsk, 1)).map(() => ({ type: 'web' }));
        const keyers = ks.slice(0, MAX_K).map((k, i) => layer(k, i, 'k'));
        const dsks = ds.slice(0, MAX_D).map((k, i) => layer(k, i, 'd'));
        const keyUid = (key) => { const f = key[0], i = asInt(key.slice(1), 0) - 1; const l = (f === 'k' ? keyers : dsks)[i]; return l ? l.uid : null; };
        let outs;
        if (Array.isArray(st.outputs) && st.outputs.length) {
            outs = st.outputs.map((o) => ({ uid: 'o' + o.id, id: o.id, name: String(o.name || ''), shm: o.shm || '', tally: !!o.tally,
                layers: (o.layers || []).map(keyUid).filter(Boolean) }));
        } else {     // moteur d'avant les sorties : PGM (toutes les couches) + CLEAN (keyers « clean »)
            outs = [{ uid: 'o0', id: 0, name: 'PGM', shm: '', tally: true, layers: keyers.concat(dsks).map((l) => l.uid) },
                    { uid: 'o1', id: 1, name: 'CLEAN', shm: '', tally: false,
                      layers: (st.keyers || []).map((k, i) => (k && k.clean ? keyers[i] && keyers[i].uid : null)).filter(Boolean) }];
        }
        const pb = st.pvw_base === undefined ? 0 : st.pvw_base;
        const base = outs.find((o) => o.id === pb);
        const fps = Number(p.fps || 25) || 25;
        return {
            n, fmt: { w: asInt(p.width, 1280), h: asInt(p.height, 720), fps, scan: p.scan === 'i' ? 'i' : 'p',
                      chroma: p.chroma || null, bit_depth: p.bit_depth || null, colorimetry: p.colorimetry || null },
            genlock: p.genlock === undefined ? true : !!p.genlock && p.genlock !== 'false',
            align: Math.max(0, Math.min(4, asInt(p.align_budget, 1))),
            keyers, dsks, outs,
            pvw: { on: pb !== null && pb !== -1, base: base ? base.uid : outs[0].uid },
            // Délai par défaut (« sources synchrones ») : réglage d'exploitation à chaud.
            sync: { mode: (st.latency_mode || p.latency_mode) === 'fastest' ? 'fastest' : 'fixed_1' },
            tally: { emit: !!p.tally_emit, pgm: p.tally_pgm_color || 'red', pvw: p.tally_pvw_color || 'green' },
            panel: { choice: null, ip: '', row: 0, shift: null },
        };
    }
    // Pile des couches, du bas vers le haut : keyers puis DSK — clé ENGINE (k1…, d1…) = rang dans sa famille.
    function stack(c) {
        return c.keyers.map((l, i) => Object.assign({}, l, { fam: 'k', idx: i, key: 'k' + (i + 1), lab: 'K' + (i + 1) }))
            .concat(c.dsks.map((l, j) => Object.assign({}, l, { fam: 'd', idx: j, key: 'd' + (j + 1), lab: 'D' + (j + 1) })));
    }
    const outByUid = (c, u) => c.outs.find((o) => o.uid === u);
    // Flux d'une sortie : celui d'avant tant que le slug du nom ne change pas (règle du hook).
    function outShm(host, o, a) {
        const prev = a && o.id != null ? a.outs.find((x) => x.id === o.id) : null;
        if (prev && prev.shm && (prev.name === o.name || slug(prev.name, o.id) === slug(o.name, o.id))) return prev.shm;
        return host + '_' + slug(o.name, o.id == null ? 9 : o.id);
    }
    // Sorties nouvelles : id ≥ 3 libre (ni pris ici, ni repris d'une sortie retirée → autre UUID NMOS).
    function assignIds(c, a) {
        const used = new Set(c.outs.map((o) => o.id).filter((x) => x != null).concat(a ? a.outs.map((o) => o.id) : []));
        let nxt = 3;
        c.outs.forEach((o) => { if (o.id == null) { while (used.has(nxt)) nxt++; o.id = nxt; used.add(nxt); } });
        return c;
    }
    const fmtKey = (f) => [f.w, f.h, Number(f.fps), f.scan].join('/');
    // Changements qui REDÉMARRENT le script (↻) : [{k, from, to}]. Le serveur tranche ; ceci annonce.
    function restartChanges(a, c, host) {
        if (!a) return [];
        const out = [];
        const push = (k, x, y) => { if (String(x) !== String(y)) out.push({ k, from: x, to: y }); };
        push('n_inputs', a.n, c.n);
        if (fmtKey(a.fmt) !== fmtKey(c.fmt)) out.push({ k: 'format', from: fmtLabel(a.fmt), to: fmtLabel(c.fmt) });
        push('genlock', a.genlock, c.genlock);
        push('align_budget', a.align, c.align);
        push('n_keyers', a.keyers.length, c.keyers.length);
        push('n_dsk', a.dsks.length, c.dsks.length);
        c.outs.forEach((o) => {
            const prev = o.id != null ? a.outs.find((x) => x.id === o.id) : null;
            if (!prev) out.push({ k: 'out_added', from: '', to: o.name });
            else if (outShm(host, o, a) !== (prev.shm || outShm(host, prev, null))) out.push({ k: 'out_renamed', from: prev.name, to: o.name });
        });
        a.outs.forEach((p) => { if (!c.outs.some((o) => o.id === p.id)) out.push({ k: 'out_removed', from: p.name, to: '' }); });
        if (a.pvw.on !== c.pvw.on) out.push({ k: 'pvw', from: a.pvw.on, to: c.pvw.on });
        return out;
    }
    // Couches comparées RANG par rang (à chaud, la structure ne bouge pas) : ce qu'il faut pousser.
    function layerPushes(a, c) {
        const ops = [];
        [['k', a.keyers, c.keyers], ['d', a.dsks, c.dsks]].forEach(([fam, la, lc]) => {
            lc.forEach((l, i) => {
                const o = la[i] || {};
                const ep = (what) => (fam === 'k' ? `keyer/${what}` : `dsk/${i + 1}/${what}`);
                const idx = fam === 'k' ? { idx: i } : {};
                if (l.t !== o.t) ops.push({ path: ep('type'), body: Object.assign({ type: l.t }, idx) });
                if (l.t !== 'web' && (l.fill !== o.fill || l.key !== o.key)) ops.push({ path: ep('input'), body: Object.assign({ input: l.fill, key_input: l.key }, idx) });
                if (l.t === 'web' && (l.url || '') !== (o.url || '')) ops.push({ path: ep('web'), body: Object.assign({ url: l.url || '' }, idx) });
            });
        });
        return ops;
    }
    // Corps de POST /control/setup.
    function buildSetup(c, a, confirm) {
        const st = stack(c);
        const lay = (l) => ({ from: l.from == null ? null : l.from, type: l.t, input: l.fill, key_input: l.key, url: l.url || '' });
        const structure = { n_inputs: c.n, width: c.fmt.w, height: c.fmt.h, fps: c.fmt.fps, scan: c.fmt.scan,
                            genlock: !!c.genlock, align_budget: c.align, n_keyers: c.keyers.length, n_dsk: c.dsks.length };
        ['chroma', 'bit_depth', 'colorimetry'].forEach((k) => { if (c.fmt[k] != null && (!a || c.fmt[k] !== a.fmt[k])) structure[k] = c.fmt[k]; });
        const outs = c.outs.map((o) => Object.assign({ name: o.name.trim(), tally: !!o.tally,
            layers: st.filter((l) => o.layers.includes(l.uid)).map((l) => l.key) }, o.id != null ? { id: o.id } : {}));
        const base = outByUid(c, c.pvw.base) || c.outs[0];
        const body = { structure, layers: { keyers: c.keyers.map(lay), dsks: c.dsks.map(lay) }, outputs: outs,
                       pvw_base: c.pvw.on ? base.id : null };
        // Délai par défaut : persisté par le hook (relu au démarrage), poussé à chaud par la page.
        if (!a || c.sync.mode !== a.sync.mode) body.latency_mode = c.sync.mode;
        if (confirm) body.confirm = true;
        return body;
    }
    // Sources synchrones : ce que le moteur MESURE (/state) — tranche ou non, groupes, entrées en
    // retard sur le délai par défaut (hystérésis côté moteur), entrées câblées.
    function syncSummary(st) {
        st = st || {};
        const d = st.delays || {}, groups = d.groups || {}, members = d.inputs || {};
        const byGroup = {};
        Object.keys(members).forEach((k) => { const g = members[k]; if (g in groups) (byGroup[g] = byGroup[g] || []).push(Number(k)); });
        Object.keys(byGroup).forEach((g) => byGroup[g].sort((x, y) => x - y));
        return {
            slice: !!st.slice_mode,
            measured: Array.isArray(st.unsync_inputs),
            mode: st.latency_mode === 'fastest' ? 'fastest' : 'fixed_1',
            groups: Object.keys(groups).sort().map((g) => ({ name: g, frames: groups[g], inputs: byGroup[g] || [] })),
            unsync: Array.isArray(st.unsync_inputs) ? st.unsync_inputs.map(Number).filter(Number.isFinite).sort((x, y) => x - y) : [],
            wired: (st.inputs || []).filter(Boolean).length,
        };
    }
    function fmtLabel(f) {
        if (!f) return '—';
        try { if (window.videoFormatLabel) return window.videoFormatLabel({ w: f.w, h: f.h, fps: f.fps, scan: f.scan }); } catch (_) {}
        return `${f.w}×${f.h}${f.scan}${f.fps}`;
    }
    // Couche nouvelle : seulement sur la sortie de base du PVW (décision du 29/10).
    function addLayer(c, fam) {
        const arr = fam === 'k' ? c.keyers : c.dsks;
        if (arr.length >= (fam === 'k' ? MAX_K : MAX_D)) return null;
        const l = { uid: uid(fam), from: null, t: fam === 'k' ? 'dve' : (c.dsks.length ? 'linear' : 'web'),
                    fill: Math.max(0, c.n - (fam === 'k' ? 1 : 2)), key: c.n - 1, url: '' };
        arr.push(l);
        const o = outByUid(c, c.pvw.on ? c.pvw.base : c.outs[0].uid) || c.outs[0];
        o.layers = stack(c).map((x) => x.uid).filter((x) => x === l.uid || o.layers.includes(x));
        return l;
    }
    function removeLayer(c, fam, i) {
        const arr = fam === 'k' ? c.keyers : c.dsks;
        if (fam === 'k' && arr.length <= 1) return;
        const [l] = arr.splice(i, 1);
        if (l) c.outs.forEach((o) => { o.layers = o.layers.filter((x) => x !== l.uid); });
    }
    function setInputs(c, n) {
        c.n = Math.max(1, Math.min(MAX_IN, n));
        c.keyers.concat(c.dsks).forEach((l) => { l.fill = Math.min(l.fill, c.n - 1); l.key = Math.min(l.key, c.n - 1); });
    }
    function addOutput(c, T) {
        if (c.outs.length >= MAX_O) return null;
        const nm = (n) => T('plugin.mixer.out_new_name', 'AUX {n}').replace('{n}', n);
        const used = c.outs.map((o) => o.name.trim()); let n = 1; while (used.includes(nm(n))) n++;
        const o = { uid: uid('o'), id: null, name: nm(n), shm: '', tally: false, layers: [] };
        c.outs.push(o);
        return o;
    }
    function removeOutput(c, u) {
        const o = outByUid(c, u);
        if (!o || o.id === 0) return;
        c.outs = c.outs.filter((x) => x.uid !== u);
        if (!outByUid(c, c.pvw.base)) c.pvw.base = c.outs[0].uid;
    }

    /* ═════════ Interface ═════════ */
    let W = null;      // instance ouverte : {root, o, M}

    function T(k, fb) { return (W && W.o.T) ? W.o.T(k, fb) : (fb !== undefined ? fb : k); }
    const t = (k, fb) => T('plugin.mixer.wiz.' + k, fb);

    function open(root, o) {
        close();
        const live = fromLive(o.state, o.params);
        W = { root, o, M: { cfg: clone(live), applied: clone(live), first: !!o.first, sel: null, tour: o.first ? 0 : -1, confirm: null,
                             busy: false, err: '', formats: [], panels: null, consumers: null, live: syncSummary(o.state) } };
        root.classList.add('mxw-host');
        root.addEventListener('click', onClick);
        root.addEventListener('change', onChange);
        root.addEventListener('input', onInput);
        root.addEventListener('keydown', onKey);
        render();
        if (window.loadVideoFormats) window.loadVideoFormats().then((f) => { if (W && W.root === root) { W.M.formats = f || []; render(); } }).catch(() => {});
        if (o.canSettings) loadPanels();
        return api();
    }
    function close() {
        if (!W) return;
        const r = W.root;
        r.removeEventListener('click', onClick); r.removeEventListener('change', onChange);
        r.removeEventListener('input', onInput); r.removeEventListener('keydown', onKey);
        r.classList.remove('mxw-host'); r.innerHTML = '';
        W = null;
    }
    function api() { return { close, act, live, get model() { return W && W.M; } }; }
    // État vivant poussé par la page (control.js) : on ne redessine que si la synchro a changé —
    // un redessin à chaque /state ferait perdre la saisie en cours.
    function live(st) {
        if (!W) return;
        const n = syncSummary(st);
        if (JSON.stringify(n) === JSON.stringify(W.M.live)) return;
        W.M.live = n;
        if (!W.M.cfg.sync || W.M.cfg.sync.mode === W.M.applied.sync.mode) { W.M.cfg.sync = { mode: n.mode }; }
        W.M.applied.sync = { mode: n.mode };
        render();
    }

    async function loadPanels() {
        try {
            const r = await fetch('/api/atem/sessions', { cache: 'no-store' });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const d = await r.json();
            if (!W) return;
            W.M.panels = (d.panels || []).map((p) => ({ ip: p.ip, label: p.label || p.ip, me: p.me_vmids || [], shift_key: p.shift_key }));
            const mine = W.M.panels.find((p) => p.me.includes(W.o.vmid));
            if (mine) Object.assign(W.M.cfg.panel, { choice: 'now', ip: mine.ip, row: mine.me.indexOf(W.o.vmid), shift: mine.shift_key == null ? null : mine.shift_key });
            W.M.applied.panel = clone(W.M.cfg.panel);
            render();
        } catch (_) { if (W) { W.M.panels = []; render(); } }
    }

    function rb(k) {
        const M = W.M;
        if (M.first) return '';
        const on = restartChanges(M.applied, M.cfg, W.o.hostname).some((c) => c.k === k || (k === 'outs' && /^out_/.test(c.k)));
        return `<span class="mxw-rb${on ? ' on' : ''}" title="${esc(t('rb_title', 'Changer ce réglage redémarre le mélangeur'))}">↻ ${esc(on ? t('rb_changed', 'redémarre — modifié') : t('rb', 'redémarre'))}</span>`;
    }
    const hot = () => (W.M.first ? '' : `<span class="mxw-hot">${esc(t('hot', 'à chaud'))}</span>`);
    const led = (a, v, label, on, dis) => `<button type="button" class="ctl-push ctl-push--led" data-a="${a}" data-v="${esc(v)}" aria-pressed="${on ? 'true' : 'false'}"${dis ? ' disabled' : ''}><span class="ctl-led"></span>${esc(label)}</button>`;
    const push = (a, v, label, cls, extra) => `<button type="button" class="ctl-push${cls ? ' ' + cls : ''}" data-a="${a}" data-v="${esc(v)}"${extra || ''}>${label}</button>`;
    const check = (a, label, on, help) => `<label class="mxw-check"><input type="checkbox" class="ios-toggle" data-a="${a}"${on ? ' checked' : ''}><span>${label}${help ? `<span class="mxw-help">${help}</span>` : ''}</span></label>`;
    const num = (a, label, val, min, max, step, badge) => `<label class="mxw-num"><span>${esc(label)}${badge || ''}</span><input type="number" data-a="${a}" value="${val}" min="${min}" max="${max}" step="${step || 1}" inputmode="numeric"></label>`;
    const ktype = (k) => T('plugin.mixer.ktype_' + k, { dve: 'DVE', linear: 'Linéaire', luma: 'Luma', chroma: 'Chroma', pattern: 'Motif', web: 'Page web' }[k] || k);

    /* ── schéma ── */
    function schematic() {
        const M = W.M, c = M.cfg, host = W.o.hostname, Wd = 1120, top = 34, rail = 192;
        const ih = c.n > 10 ? 20 : 26, igap = c.n > 10 ? 4 : 6;
        const kN = c.keyers.length + (c.keyers.length < MAX_K ? 1 : 0), dN = c.dsks.length + (c.dsks.length < MAX_D ? 1 : 0), R = kN + dN, rh = 48, bh = 40;
        const oh = 78, og = 10, oN = c.outs.length + (c.outs.length < MAX_O ? 1 : 0), outsH = oN * (oh + og) - og;
        const inH = c.n * (ih + igap) - igap + 34;
        const yF = top + 92 + (R - 1) * rh;
        const H = Math.max(yF + 120, top + 6 + inH + 80, top + 6 + outsH + 80, 440);
        const rowY = (r) => yF - 86 - r * rh;
        const cls = (id) => (M.sel === id ? ' sel' : '');
        const selL = (M.sel && /^[kd]:/.test(M.sel)) ? (M.sel[0] === 'k' ? c.keyers : c.dsks)[Number(M.sel.slice(2))] : null;
        const inY = (i) => top + 6 + 8 + i * (ih + igap) + ih / 2;
        let o = `<svg class="mxw-sch" viewBox="0 0 ${Wd} ${H}" role="group" aria-label="${esc(t('sch_aria', 'Schéma du mélangeur, touchez un élément pour le régler'))}">`;
        [[t('sch_inputs', 'ENTRÉES'), 14], [t('sch_dsk', 'DSK — au-dessus de tout'), 668], [t('sch_outputs', 'SORTIES PROGRAMME'), 880]]
            .forEach(([x, xx]) => { o += `<text x="${xx}" y="20" class="t-m">${esc(x)}</text>`; });
        const kTop = rowY(kN - 1), dTop = rowY(R - 1);
        o += `<rect x="222" y="${kTop - 8}" width="428" height="${yF + 46 - kTop + 8}" rx="8" class="zone"/><text x="230" y="${kTop - 14}" class="t-m">${esc(t('sch_keyers', 'KEYERS — superposés au fond, dans la transition'))}</text>`;
        o += `<rect x="660" y="${dTop - 8}" width="176" height="${yF + 12 - dTop + 8}" rx="8" class="zone"/>`;
        // Cadre de l'étape du guide : dessiné AVANT les blocs, sans fond opaque ni geste — il entoure,
        // il ne couvre rien.
        const step = M.tour >= 0 ? STEPS[M.tour].focus : null;
        if (step) {
            const pv = H - 40;
            const f = { inputs: [4, top, 162, inH + 12], layers: [216, dTop - 6, 626, yF - 34 - dTop],
                        outs: [864, top, 252, outsH + 12], panel: [4, pv - 28, 162, 56] }[step];
            o += `<rect x="${f[0]}" y="${f[1]}" width="${f[2]}" height="${f[3]}" rx="10" class="mxw-focus" data-focus="${step}" aria-hidden="true"/>`;
        }
        // entrées
        o += `<g data-el="inputs" tabindex="0" role="button" aria-label="${esc(t('el_inputs', 'Entrées : {n}, {f}').replace('{n}', c.n).replace('{f}', fmtLabel(c.fmt)))}" class="${cls('inputs')}"><rect x="10" y="${top + 6}" width="150" height="${inH}" rx="6" class="box"/>`;
        const srcOf = selL && selL.t !== 'web' ? Object.assign({ [selL.fill]: 'fill' }, selL.t === 'linear' ? { [selL.key]: selL.key === selL.fill ? 'fill+key' : 'key' } : {}) : {};
        for (let i = 0; i < c.n; i++) {
            const y = inY(i) - ih / 2, s = srcOf[i];
            o += `<rect x="18" y="${y}" width="134" height="${ih}" rx="3" class="${s ? 'box-src' : 'box'}"/><text x="25" y="${y + ih / 2 + 4}">${esc(t('sch_input', 'Entrée {n}').replace('{n}', i + 1))}</text>`;
            if (s) o += `<text x="110" y="${y + ih / 2 + 4}" class="t-src">${s}</text>`;
        }
        o += `<text x="18" y="${top + 6 + inH - 10}" class="t-m">${esc(fmtLabel(c.fmt))} · ${esc(c.genlock ? 'genlock' : t('free', 'libre'))}</text>`;
        const us = (M.live && M.live.unsync) || [];
        if (us.length) o += `<text x="18" y="${top + 6 + inH + 14}" class="t-warn">⚑ ${esc(t('sch_unsync', '{n} en retard').replace('{n}', us.map((n) => '#' + n).join(' ')))}</text>`;
        if (!M.first && restartChanges(M.applied, c, host).some((x) => ['n_inputs', 'format', 'genlock', 'align_budget'].includes(x.k))) o += `<text x="142" y="${top + 22}" class="rbm">↻</text>`;
        o += `</g>`;
        const yMin = Math.min(inY(0), dTop + bh / 2), yMax = Math.max(inY(c.n - 1), yF);
        for (let i = 0; i < c.n; i++) o += `<path class="ln-rail" d="M152 ${inY(i)} H ${rail}"/>`;
        o += `<path class="ln-rail" d="M${rail} ${yMin} V ${yMax}" style="stroke-width:3"/>`;
        // fond : BGD A/B → transition → sorties
        o += `<path class="ln-acc" d="M${rail} ${yF} H 232"/>`;
        o += `<rect x="232" y="${yF - 32}" width="110" height="64" rx="6" class="box-acc"/><text x="242" y="${yF - 10}" class="t-s">BGD A / B</text><text x="242" y="${yF + 8}" class="t-m">${esc(t('sch_bgd1', 'fond, choisi'))}</text><text x="242" y="${yF + 22}" class="t-m">${esc(t('sch_bgd2', 'à l\'exploitation'))}</text>`;
        o += `<path class="ln-acc" d="M342 ${yF} H 520"/>`;
        // Transition (0.23.57) : DESSINÉE, c'est le chemin du signal, mais réglée en exploitation
        // (rubrique Transitions) — ni geste ni panneau ici.
        o += `<g class="mxw-fixed" role="img" aria-label="${esc(t('tr_ops_aria', 'Transition : se règle en exploitation, rubrique Transitions'))}"><rect x="520" y="${yF - 38}" width="122" height="76" rx="6" class="box-fixed"/>
            <text x="532" y="${yF - 14}" class="t-s">${esc(t('sch_tr', 'TRANSITION'))}</text><text x="532" y="${yF + 4}" class="t-m">${esc(t('sch_tr_ops', 'en exploitation'))}</text>
            <text x="532" y="${yF + 22}" class="t-m">${esc(t('sch_tr_where', 'rubrique Transitions'))}</text></g>`;
        o += `<path class="ln-acc" d="M642 ${yF} H 856"/><text x="668" y="${yF - 8}" class="t-m">${esc(t('sch_dsk_here', 'DSK posés ici ↑'))}</text>`;
        const links = [];
        const bar = (l, i, fam, r) => {
            const x = fam === 'k' ? 232 : 668, w = fam === 'k' ? 410 : 160, y = rowY(r), id = fam + ':' + i;
            const srcTxt = l.t === 'web' ? (l.url ? l.url.replace(/^https?:\/\//, '').slice(0, fam === 'k' ? 40 : 18) : t('url_missing', 'URL à saisir'))
                : (l.t === 'linear' ? `fill : ${l.fill + 1} · key : ${l.key + 1}` : t('sch_fill', 'fill : entrée {n}').replace('{n}', l.fill + 1));
            o += `<g data-el="${id}" tabindex="0" role="button" aria-label="${esc((fam === 'k' ? 'Keyer ' : 'DSK ') + (i + 1) + ', ' + ktype(l.t) + ', ' + srcTxt)}" class="${cls(id)}"><rect x="${x}" y="${y}" width="${w}" height="${bh}" rx="5" class="${l.t === 'web' ? 'box-web' : 'box-key'}"/>
                <text x="${x + 10}" y="${y + 17}" class="t-s">${fam === 'k' ? 'K' : 'DSK '}${i + 1} · ${esc(ktype(l.t))}</text><text x="${x + 10}" y="${y + 33}" class="t-m">${esc(srcTxt)}</text></g>`;
            if (l.t !== 'web') {
                const on = M.sel === id;
                links.push({ y0: inY(l.fill), y1: y + bh / 2, x1: x, on, key: false });
                if (l.t === 'linear') links.push({ y0: inY(l.key), y1: y + bh / 2 + 8, x1: x, on, key: true });
            }
        };
        c.keyers.forEach((l, i) => bar(l, i, 'k', i));
        if (c.keyers.length < MAX_K) { const y = rowY(c.keyers.length); o += `<g data-el="addk" tabindex="0" role="button" aria-label="${esc(t('add_keyer', 'Ajouter un keyer'))}" class="${cls('addk')}"><rect x="232" y="${y}" width="410" height="${bh}" rx="5" class="box-add"/><text x="250" y="${y + 25}" class="t-m">＋ keyer (${c.keyers.length}/${MAX_K})</text></g>`; }
        c.dsks.forEach((l, i) => bar(l, i, 'd', kN + i));
        if (c.dsks.length < MAX_D) { const y = rowY(kN + c.dsks.length); o += `<g data-el="addd" tabindex="0" role="button" aria-label="${esc(t('add_dsk', 'Ajouter un DSK'))}" class="${cls('addd')}"><rect x="668" y="${y}" width="160" height="${bh}" rx="5" class="box-add"/><text x="684" y="${y + 25}" class="t-m">＋ DSK (${c.dsks.length}/${MAX_D})</text></g>`; }
        // faisceau : chaque lien part du rail commun ; la couche choisie s'allume, key en tirets
        links.sort((a, b) => a.on - b.on).forEach((L1) => {
            o += `<path class="${L1.on ? 'ln-src' : 'ln-bundle'}" d="M${rail} ${L1.y0} V ${L1.y1} H ${L1.x1}"${L1.key ? ' stroke-dasharray="5 3"' : ''}/>`;
            if (L1.on) o += `<path class="ln-src" d="M152 ${L1.y0} H ${rail}"${L1.key ? ' stroke-dasharray="5 3"' : ''}/>`;
        });
        // sorties (pastilles de couches)
        const st = stack(c), ox = 870, ow = 240, ot = top + 6;
        c.outs.forEach((out, j) => {
            const y = ot + j * (oh + og), oid = 'o:' + out.uid, isBase = c.pvw.on && c.pvw.base === out.uid;
            o += `<path class="ln-acc" d="M856 ${yF} C 864 ${yF}, 860 ${y + oh / 2}, ${ox} ${y + oh / 2}"/>`;
            o += `<g data-el="${oid}" tabindex="0" role="button" aria-label="${esc(t('el_out', 'Sortie {o}').replace('{o}', out.name))}" class="${cls(oid)}"><rect x="${ox}" y="${y}" width="${ow}" height="${oh}" rx="6" class="box${out.id === 0 ? ' box-pgm' : ''}"/>
                <text x="${ox + 10}" y="${y + 18}" class="t-s">${esc(out.name.toUpperCase())}</text><text x="${ox + 10}" y="${y + 32}" class="t-m">${esc(outShm(host, out, M.applied))}${isBase ? ' · ' + esc(t('pvw_base_short', 'base PVW')) : ''}</text>
                <circle cx="${ox + ow - 16}" cy="${y + 15}" r="6" class="${out.tally ? 'tally-on' : 'tally-off'}"/><text x="${ox + ow - 46}" y="${y + 19}" class="t-m">tally</text></g>`;
            if (!st.length) o += `<text x="${ox + 10}" y="${y + 60}" class="t-m">${esc(t('no_layer', 'aucune couche'))}</text>`;
            st.forEach((l, i) => {
                const px = ox + 8 + i * 37, py = y + 42, on = out.layers.includes(l.uid);
                o += `<g data-el="pill:${out.uid}:${l.uid}" tabindex="0" role="switch" aria-checked="${on}" aria-label="${esc(t('pill_aria', '{l} sur {o}').replace('{l}', l.lab).replace('{o}', out.name))}"><rect x="${px}" y="${py}" width="34" height="28" rx="14" class="${on ? 'pill-on' : 'pill-off'}"/><text x="${px + 9}" y="${py + 18}" class="${on ? 't-p' : 't-poff'}">${l.lab}</text></g>`;
            });
        });
        if (c.outs.length < MAX_O) { const y = ot + c.outs.length * (oh + og); o += `<g data-el="addo" tabindex="0" role="button" aria-label="${esc(t('add_out', 'Ajouter une sortie programme'))}" class="${cls('addo')}"><rect x="${ox}" y="${y}" width="${ow}" height="44" rx="6" class="box-add"/><text x="${ox + 18}" y="${y + 27}" class="t-m">＋ ${esc(t('add_out_short', 'sortie programme'))} (${c.outs.length}/${MAX_O})</text></g>`; }
        const pvY = H - 40, base = outByUid(c, c.pvw.base) || c.outs[0];
        o += `<path class="${c.pvw.on ? 'ln' : 'ln-off'}" d="M287 ${yF + 32} V ${pvY} H ${ox}"/>`;
        o += `<g data-el="pvw" tabindex="0" role="button" aria-label="${esc(t('el_pvw', 'Sortie PVW'))}" class="${cls('pvw')}"><rect x="${ox}" y="${pvY - 22}" width="${ow}" height="44" rx="6" class="${c.pvw.on ? 'box box-pvw' : 'box-off'}"/>
            <text x="${ox + 10}" y="${pvY - 3}" class="${c.pvw.on ? 't-s' : 't-x'}">PVW</text><text x="${ox + 10}" y="${pvY + 13}" class="t-m">${esc(c.pvw.on ? t('pvw_on', 'base : {o}').replace('{o}', base.name.toUpperCase()) : t('pvw_off', 'désactivée'))}</text></g>`;
        const pnl = c.panel, pl = (M.panels || []).find((p) => p.ip === pnl.ip);
        o += `<g data-el="panel" tabindex="0" role="button" aria-label="${esc(t('el_panel', 'Pupitre et tally'))}" class="${cls('panel')}"><rect x="10" y="${pvY - 22}" width="150" height="44" rx="6" class="${pnl.ip ? 'box-acc' : 'box-add'}"/>
            <text x="20" y="${pvY - 3}" class="t-s">${esc(t('panel', 'Pupitre'))}</text><text x="20" y="${pvY + 13}" class="t-m">${esc(pl ? pl.label : (pnl.choice === 'later' ? t('later_short', 'plus tard') : t('none', 'aucun')))}${c.tally.emit ? ' · tally' : ''}</text></g>`;
        return o + '</svg>';
    }

    /* ── panneau contextuel ── */
    function pInputs() {
        const c = W.M.cfg, f = W.M.formats || [];
        const cur = fmtKey(c.fmt);
        let fm = f.map((x, i) => led('fmt', i, x.label, fmtKey({ w: x.w, h: x.h, fps: x.fps, scan: x.scan }) === cur)).join('');
        if (!f.some((x) => fmtKey({ w: x.w, h: x.h, fps: x.fps, scan: x.scan }) === cur)) fm = `<span class="ctl-readout"><b>${esc(fmtLabel(c.fmt))}</b></span>` + fm;
        return `${num('n', t('inputs_n', 'Entrées (1 à 20)'), c.n, 1, MAX_IN, 1, rb('n_inputs'))}
            <div><div class="mxw-sub">${esc(t('format', 'Format'))}${rb('format')}</div><div class="mxw-row">${fm}</div>
            <span class="mxw-help">${esc(t('format_help', 'Les formats de l\'installation (Réglages → Vidéo).'))}</span></div>
            <details class="mxw-adv"><summary class="ctl-push">${esc(t('advanced', 'Avancé'))}</summary>
              ${check('genlock', esc(t('genlock', 'Cadence verrouillée sur une entrée de référence (genlock)')) + rb('genlock'), c.genlock)}
              ${num('align', t('align', 'Attente d\'alignement (images)'), c.align, 0, 4, 1, rb('align_budget'))}</details>
            ${pSync()}
            <div class="mxw-help">${esc(t('inputs_cabling', 'Les sources se câblent dans la page Câbles ; les groupes de délai, dans l\'onglet Entrées & délais.'))}</div>`;
    }
    // SOURCES SYNCHRONES (0.23.57) — retour de Cyril : « il faut être alerté dessus pour savoir que
    // ça existe et que ça se règle ». Une phrase, l'état mesuré, le délai par défaut (à chaud) et le
    // renvoi vers l'onglet Entrées & délais, où se règlent les groupes.
    function pSync() {
        const M = W.M, L = M.live || syncSummary(null), c = M.cfg;
        let h = `<section class="mxw-sync${L.unsync.length ? ' warn' : ''}" aria-labelledby="mxw-sync-t"><div class="mxw-sub" id="mxw-sync-t">${esc(t('sync_t', 'Sources synchrones ?'))}</div>
            <div class="mxw-help">${esc(t('sync_x', 'Une source synchrone (même référence PTP) arrive à temps pour le délai par défaut. Une source qui ne l\'est pas (HF, liaison, traitement extérieur) arrive plus tard : rangez-la dans un groupe de délai, sinon elle passe avec son image précédente.'))}</div>`;
        if (!L.slice) {
            h += `<div class="mxw-help">${esc(t('sync_whole', 'Ce mélangeur tourne en image entière : délais et groupes ne s\'appliquent qu\'en mode tranche (Réglages → Vidéo). En image entière, c\'est l\'attente d\'alignement (Avancé) qui joue ce rôle.'))}</div>`;
        } else {
            h += `<div class="mxw-field"><span>${esc(t('sync_default', 'Délai par défaut (entrées sans groupe)'))}${hot()}</span><div class="mxw-row">
                <button type="button" class="ctl-bus ctl-bus--solid" data-bus="pvw" data-a="lmode" data-v="fixed_1" aria-checked="${c.sync.mode !== 'fastest'}">${esc(t('sync_fixed', 'Fixe, 1 image'))}<small>${esc(t('sync_fixed_s', 'défaut'))}</small></button>
                <button type="button" class="ctl-bus ctl-bus--solid" data-bus="pvw" data-a="lmode" data-v="fastest" aria-checked="${c.sync.mode === 'fastest'}">${esc(t('sync_fastest', 'Au plus rapide'))}<small>${esc(t('sync_fastest_s', 'sans marge'))}</small></button></div></div>`;
            const gs = L.groups.length ? L.groups.map((g) => t('sync_group', '{g} ({f} img) : {i}').replace('{g}', g.name).replace('{f}', g.frames)
                .replace('{i}', g.inputs.length ? g.inputs.map((n) => '#' + n).join(' ') : t('sync_group_empty', 'vide'))).join(' · ') : t('sync_nogroup', 'aucun — toutes les entrées suivent le délai par défaut');
            h += `<div class="mxw-sync-state"><span class="ctl-readout"><span>${esc(t('sync_groups', 'Groupes de délai'))}</span><b>${esc(gs)}</b></span>`;
            if (!L.measured) h += `<span class="mxw-help">${esc(t('sync_old', 'Le moteur déployé ne mesure pas encore les retards : ils apparaîtront après le prochain redéploiement.'))}</span>`;
            else if (L.unsync.length) h += `<span class="mxw-warn-t" role="status">⚑ ${esc(t('sync_late', 'En retard sur le délai par défaut : {i}. Elles passent avec leur image précédente.').replace('{i}', L.unsync.map((n) => '#' + n).join(', ')))}</span>`;
            else if (L.wired) h += `<span class="mxw-help">${esc(t('sync_ok', 'Mesuré : aucune entrée sans groupe en retard.'))}</span>`;
            else h += `<span class="mxw-help">${esc(t('sync_unwired', 'Rien de câblé : la mesure commence au câblage.'))}</span>`;
            h += `</div>`;
        }
        return h + `<div class="mxw-row">${push('delays', '', esc(t('sync_open', 'Régler les groupes (Entrées & délais)')), L.unsync.length ? 'ctl-push--warn' : '')}</div></section>`;
    }
    function pLayer(fam, i) {
        const c = W.M.cfg, l = (fam === 'k' ? c.keyers : c.dsks)[i];
        let h = `<div class="mxw-help">${esc(fam === 'k' ? t('keyer_help', 'Keyer amont, dans le M/E : passe dans la transition.') : t('dsk_help', 'Après la transition, au-dessus de tout. Son propre CUT et son AUTO.'))}</div>
            <div class="mxw-row">${KTYPES.map((k) => led('ltype', fam + ':' + i + ':' + k, ktype(k), l.t === k)).join('')}</div>${hot()}`;
        if (l.t === 'web') {
            h += `<span class="mxw-cpu">⚑ ${esc(t('web_cpu', 'coûte du CPU : un onglet Chromium par couche « page web »'))}</span>
                <input type="url" class="mxw-input" data-a="url:${fam}:${i}" placeholder="https://…" value="${esc(l.url)}" aria-label="${esc(t('url', 'URL de la page'))}">`;
        } else {
            const row = (w, lab) => `<div><div class="mxw-sub">${esc(lab)}</div><div class="mxw-row">${range(c.n).map((n) =>
                `<button type="button" class="ctl-bus ctl-bus--solid" data-bus="dsk" data-a="lsrc" data-v="${fam}:${i}:${w}:${n}" aria-checked="${l[w] === n}">${n + 1}</button>`).join('')}</div></div>`;
            h += row('fill', l.t === 'linear' ? t('fill', 'Fill — entrée') : t('source', 'Source — entrée'));
            if (l.t === 'linear') h += row('key', t('key', 'Key — entrée'));
            h += `<div class="mxw-help">${esc(t('src_help', 'Prise directement parmi les entrées, indépendamment de BGD A / B.'))}</div>`;
        }
        h += `<div><div class="mxw-sub">${esc(t('shown_on', 'Montré sur les sorties'))}${hot()}</div><div class="mxw-list">${c.outs.map((o) =>
            check('lo:' + o.uid + ':' + l.uid, esc(o.name.toUpperCase()), o.layers.includes(l.uid))).join('')}</div></div>`;
        const canRm = fam === 'd' || c.keyers.length > 1;
        h += `<div class="mxw-foot">${canRm ? push('rm', fam + ':' + i, esc(fam === 'k' ? t('rm_keyer', 'Retirer ce keyer') : t('rm_dsk', 'Retirer ce DSK')), 'ctl-push--danger')
            : `<span class="mxw-help">${esc(t('one_keyer', 'Il reste au moins un keyer.'))}</span>`}${rb(fam === 'k' ? 'n_keyers' : 'n_dsk')}</div>`;
        return h;
    }
    function pOutput(u) {
        const c = W.M.cfg, o = outByUid(c, u); if (!o) return '';
        const st = stack(c);
        return `<label class="mxw-field"><span>${esc(t('out_name', 'Nom'))}${rb('outs')}</span><input type="text" class="mxw-input" data-a="oname:${o.uid}" value="${esc(o.name)}" maxlength="${NAME_MAX}" autocomplete="off"></label>
            <div class="mxw-help">${esc(t('out_stream', 'Flux publié : {s}').replace('{s}', outShm(W.o.hostname, o, W.M.applied)))}${W.M.first ? '' : ' — ' + esc(t('out_rename_help', 'changer la casse ou les accents se fait à chaud ; un autre nom change le flux et débranche ses abonnés'))}</div>
            <div><div class="mxw-sub">${esc(t('out_layers', 'Couches montrées, dans l\'ordre de la pile'))}${hot()}</div>
              ${st.length ? `<div class="mxw-list">${st.slice().reverse().map((l) => check('lo:' + o.uid + ':' + l.uid, esc(l.lab + ' · ' + ktype(l.t)), o.layers.includes(l.uid))).join('')}</div>`
                : `<div class="mxw-help">${esc(t('no_layer', 'aucune couche'))}</div>`}
              <div class="mxw-row">${push('oall', o.uid, esc(t('all', 'Toutes')))}${push('onone', o.uid, esc(t('none_clean', 'Aucune (clean)')))}</div>
              ${c.outs.length > 1 ? `<span class="mxw-cpu">⚑ ${esc(t('out_cpu', 'chaque sortie à la composition différente coûte du calcul'))}</span>` : ''}</div>
            ${check('otally:' + o.uid, esc(t('out_tally', 'Compte pour le tally')) + hot(), o.tally, esc(t('out_tally_help', 'Une source visible sur cette sortie allume son tally PGM.')))}
            ${c.pvw.on ? `<div class="mxw-row">${led('pvwbase', o.uid, t('pvw_base', 'Base du PVW'), c.pvw.base === o.uid)}<span class="mxw-help">${esc(t('pvw_base_help', 'Le PVW montre la prochaine image de cette sortie.'))}</span></div>` : ''}
            <div class="mxw-foot">${o.id !== 0 && c.outs.length > 1 ? push('rmo', o.uid, esc(t('rm_out', 'Retirer cette sortie')), 'ctl-push--danger')
                : `<span class="mxw-help">${esc(t('pgm_always', 'La sortie PGM reste toujours.'))}</span>`}</div>`;
    }
    function pPvw() {
        const c = W.M.cfg;
        return `${check('pvwon', esc(t('pvw_publish', 'Publier la sortie PVW')) + rb('pvw'), c.pvw.on, esc(t('pvw_help', 'La prévisualisation : la prochaine image d\'une sortie programme.')))}
            ${c.pvw.on ? `<div><div class="mxw-sub">${esc(t('pvw_base_of', 'Basée sur la sortie'))}${hot()}</div><div class="mxw-row">${c.outs.map((o) => led('pvwbase', o.uid, o.name.toUpperCase(), c.pvw.base === o.uid)).join('')}</div></div>` : ''}`;
    }
    function pPanel() {
        const M = W.M, c = M.cfg, p = c.panel;
        let h = `<div class="mxw-row">${led('pchoice', 'now', t('panel_now', 'Configurer maintenant'), p.choice === 'now')}${led('pchoice', 'later', t('panel_later', 'Plus tard'), p.choice === 'later')}</div>`;
        const where = `<div class="mxw-next"><div class="mxw-sub">${esc(t('where', 'Où les retrouver'))}</div><div class="mxw-help">• ${esc(t('where_panel', 'Pupitre : Réglages → Pupitres ATEM'))}</div><div class="mxw-help">• ${esc(t('where_tally', 'Tally : la rangée Tally du mélangeur'))}</div><div class="mxw-help">• ${esc(t('where_levels', 'Niveaux : Réglages → Labels & Tally'))}</div></div>`;
        if (p.choice !== 'now') return h + (p.choice === 'later' ? where : '');
        if (!W.o.canSettings) {
            h += `<div class="mxw-help">${esc(t('panel_no_right', 'Affecter un pupitre demande le droit de modifier les réglages : Réglages → Pupitres ATEM.'))}</div>`;
        } else if (M.panels === null) {
            h += `<div class="mxw-help">${esc(t('loading', 'Chargement…'))}</div>`;
        } else if (!M.panels.length) {
            h += `<div class="mxw-help">${esc(t('panel_none', 'Aucun pupitre ATEM connu : il apparaît dès qu\'il se connecte (Réglages → Pupitres ATEM).'))}</div>`;
        } else {
            h += `<div><div class="mxw-sub">${esc(t('panel_atem', 'Pupitre ATEM'))}${hot()}</div><div class="mxw-row">${led('pip', '', t('none', 'aucun'), !p.ip)}${M.panels.map((x) => led('pip', x.ip, x.label + ' · ' + x.ip, p.ip === x.ip)).join('')}</div></div>`;
            if (p.ip) {
                h += `<div><div class="mxw-sub">${esc(t('panel_row', 'Rangée M/E'))}</div><div class="mxw-row">${[0, 1].map((r) => led('prow', r, 'M/E ' + (r + 1), p.row === r)).join('')}</div></div>`;
                if (c.n > 8) {
                    h += `${num('shift', t('shift_key', 'Touche MACRO qui fait office de Shift (0 = aucune)'), p.shift == null ? 0 : p.shift + 1, 0, 100, 1)}
                        <div class="mxw-help">${esc(t('shift_help', 'Les 8 boutons de bus passent sur les entrées 9 à 16 tant que Shift est actif. La touche SHIFT du pupitre marche aussi si son Button Mapping met 9-16 sur sa page Shift.'))}${c.n > 16 ? ' ' + esc(t('shift_16', 'Entrées 17 et plus : depuis l\'interface seulement.')) : ''}</div>`;
                }
            }
        }
        const tallied = c.outs.filter((o) => o.tally).map((o) => o.name.toUpperCase());
        h += `<div><div class="mxw-sub">Tally${hot()}</div>${check('temit', esc(t('tally_emit', 'Émettre le tally des sources')), c.tally.emit,
            esc(t('tally_outs', 'Sorties qui comptent : {o}').replace('{o}', tallied.length ? tallied.join(', ') : t('none_f', 'aucune'))))}`;
        if (c.tally.emit) {
            const col = (a, cur) => [['red', t('red', 'Rouge'), 'pgm'], ['green', t('green', 'Vert'), 'pvw'], ['off', t('off', 'Aucun'), 'off']]
                .map(([v, l, b]) => `<button type="button" class="ctl-bus ctl-bus--solid" data-bus="${b}" data-a="${a}" data-v="${v}" aria-checked="${cur === v}">${esc(l)}</button>`).join('');
            h += `<div class="mxw-field"><span>${esc(t('at_pgm', 'Au programme'))}</span><div class="mxw-row">${col('tpgm', c.tally.pgm)}</div></div>
                <div class="mxw-field"><span>${esc(t('at_pvw', 'Au PVW'))}</span><div class="mxw-row">${col('tpvw', c.tally.pvw)}</div></div>`;
        }
        return h + '</div>' + where;
    }
    function panelHTML() {
        const M = W.M, id = M.sel;
        if (!id) return `<aside class="mxw-ctx empty"><div class="mxw-help">${esc(t('ctx_empty', 'Touchez un élément du schéma pour le régler : les entrées, une couche, une sortie, le PVW ou le pupitre. La transition se règle en exploitation. Les « ＋ » ajoutent ; les pastilles d\'une sortie incluent ou excluent une couche.'))}</div></aside>`;
        let ttl, b;
        if (id === 'inputs') { ttl = t('ttl_inputs', 'Entrées et format'); b = pInputs(); }
        else if (id === 'pvw') { ttl = t('el_pvw', 'Sortie PVW'); b = pPvw(); }
        else if (id === 'panel') { ttl = t('el_panel', 'Pupitre et tally'); b = pPanel(); }
        else if (id.startsWith('o:')) { const o = outByUid(M.cfg, id.slice(2)); if (!o) { M.sel = null; return panelHTML(); } ttl = t('el_out', 'Sortie {o}').replace('{o}', o.name.toUpperCase()); b = pOutput(o.uid); }
        else { const [f, i] = id.split(':'); if (!(f === 'k' ? M.cfg.keyers : M.cfg.dsks)[Number(i)]) { M.sel = null; return panelHTML(); } ttl = (f === 'k' ? 'Keyer ' : 'DSK ') + (Number(i) + 1); b = pLayer(f, Number(i)); }
        return `<aside class="mxw-ctx" aria-label="${esc(ttl)}"><div class="mxw-ctx-h"><span class="mxw-ctx-t">${esc(ttl)}</span>${push('close', '', '✕', 'ctl-push--small', ` aria-label="${esc(t('close_panel', 'Fermer le panneau'))}"`)}</div>${b}</aside>`;
    }

    /* ── fil guidé : bandeau d'étapes AU-DESSUS du schéma (0.23.63, option B de Cyril, 31/10) ──
       La bulle flottante posée sur le schéma masquait ce qu'il fallait voir (à l'étape Couches :
       « ＋ keyer », Keyer 1, « ＋ DSK », DSK 1). Désormais : les étapes en pastilles cliquables sur la
       ligne du titre, un bandeau d'une ligne (titre, une phrase, actions de l'étape, ‹ et Suivant), et
       le bloc concerné ENCADRÉ sur le schéma, derrière les éléments : rien ne couvre jamais le schéma. Le
       panneau de droite reste réservé à l'élément touché. */
    // focus = bloc encadré sur le schéma ; open(c) = ce que « Régler » ouvre au panneau.
    const STEPS = [
        { k: 'inputs', kk: 'Entrées', tt: 'Combien d\'entrées, quel format ? Vos sources sont-elles synchrones ?', dd: 'Touchez le bloc des entrées. Une source non synchrone (HF, liaison…) se range dans un groupe de délai. Le câblage des sources se fait ensuite dans la page Câbles.',
          focus: 'inputs', open: () => 'inputs' },
        { k: 'layers', kk: 'Couches', tt: 'Ajoutez vos keyers et vos DSK', dd: 'Ils se superposent au fond. Keyer : dans la transition. DSK : au-dessus de tout. Touchez une couche pour son type et ses sources, prises parmi les entrées.',
          focus: 'layers', open: null },
        { k: 'outs', kk: 'Sorties', tt: 'Une sortie par usage : antenne, clean, web…', dd: 'Chaque sortie choisit ses couches : touchez ses pastilles. 4 sorties au plus. Le PVW se base sur l\'une d\'elles.',
          focus: 'outs', open: (c) => 'o:' + c.outs[0].uid },
        { k: 'panel', kk: 'Pupitre', tt: 'Pupitre ATEM et tally : maintenant ou plus tard ?', dd: 'Facultatif. Tout se retrouve ensuite dans Réglages.',
          focus: 'panel', open: () => 'panel' },
        { k: 'done', kk: 'Terminé', tt: 'Vérifiez le schéma, puis « Appliquer ».', dd: 'Ensuite : câbler les entrées dans Câbles. L\'assistant se rouvre par ⚙ → Assistant.',
          focus: null, open: null },
    ];
    // Pastilles d'étapes (contrôle du catalogue `ctl-steps`) : faites ✓, courante marquée, toutes cliquables.
    function stepPills() {
        const M = W.M;
        if (M.tour < 0) return '';
        return `<nav class="ctl-steps" aria-label="${esc(t('steps_aria', 'Étapes du guide'))}">${STEPS.map((s, i) => {
            const done = i < M.tour, cur = i === M.tour;
            return `<button type="button" class="ctl-step${done ? ' done' : ''}" data-a="tourGo" data-v="${i}"${cur ? ' aria-current="step"' : ''}>`
                + `<span class="ctl-step-n" aria-hidden="true">${done ? '✓' : i + 1}</span>${esc(t('tour_' + s.k + '_k', s.kk))}</button>`;
        }).join('')}</nav><span class="mxw-sp"></span>${push('tourSkip', '', esc(t('skip', 'Passer le guide')))}`;
    }
    // Bandeau de l'étape courante : titre + une phrase, puis ses actions, ‹ et Suivant (Appliquer à la fin).
    function stepBand() {
        const M = W.M;
        if (M.tour < 0) return '';
        const s = STEPS[M.tour], c = M.cfg;
        const acts = s.k === 'layers'
            ? push('el', 'addk', esc(t('step_add_keyer', '＋ keyer')), 'ctl-push--ok', c.keyers.length >= MAX_K ? ' disabled' : '')
              + push('el', 'addd', esc(t('step_add_dsk', '＋ DSK')), 'ctl-push--ok', c.dsks.length >= MAX_D ? ' disabled' : '')
            : (s.open ? push('tourOpen', s.open(c), esc(t('tour_open', 'Régler')), 'ctl-push--ok') : '');
        const last = M.tour >= STEPS.length - 1;
        return `<div class="mxw-band" role="region" aria-label="${esc(t('guide', 'Guide'))}" data-step="${s.k}">
            <div class="mxw-band-txt"><span class="mxw-band-t">${esc(t('tour_' + s.k + '_t', s.tt))}</span><span class="mxw-help">${esc(t('tour_' + s.k + '_d', s.dd))}</span></div>
            <div class="mxw-row">${acts}${M.tour > 0 ? push('tourPrev', '', '‹', '', ` aria-label="${esc(t('prev', 'Étape précédente'))}"`) : ''}${last ? push('apply', '', esc(t('apply_short', 'Appliquer')), 'ctl-push--ok') : push('tourNext', '', esc(t('next', 'Suivant')))}</div></div>`;
    }

    /* ── page ── */
    function hotCount() {
        const M = W.M, a = M.applied, c = M.cfg;
        let n = layerPushes(a, c).length;
        const oc = (x) => JSON.stringify(x.outs.map((o) => [o.id, o.name, o.tally, stack(x).filter((l) => o.layers.includes(l.uid)).map((l) => l.key)])
            .concat([[x.pvw.on, (outByUid(x, x.pvw.base) || {}).id]]));
        if (oc(a) !== oc(c)) n++;
        if (a.sync.mode !== c.sync.mode) n++;
        if (JSON.stringify(a.tally) !== JSON.stringify(c.tally)) n++;
        if (JSON.stringify(a.panel) !== JSON.stringify(c.panel)) n++;
        return n;
    }
    function applyBar() {
        const M = W.M, c = M.cfg;
        if (M.first) {
            return `<div class="mxw-bar">${push('later', '', esc(t('later', 'Plus tard')))}<span class="mxw-sum">${esc(W.o.hostname)} · ${c.n} ${esc(t('inputs_word', 'entrées'))} · ${esc(fmtLabel(c.fmt))} · ${c.keyers.length} keyer${c.keyers.length > 1 ? 's' : ''} · ${c.dsks.length} DSK · ${c.outs.length} ${esc(t('outs_word', 'sortie(s)'))}${c.pvw.on ? ' + PVW' : ''} · ${esc(t('first_note', 'rien à l\'antenne, appliquer ne coupe rien'))}</span>
                ${M.tour < 0 ? push('tourRestart', '', esc(t('tour_restart', 'Reprendre le guide'))) : ''}${push('apply', '', esc(t('apply_cfg', 'Appliquer la configuration')), 'ctl-push--ok ctl-push--led', M.busy ? ' disabled aria-busy="true"' : '')}</div>`;
        }
        const rc = restartChanges(M.applied, c, W.o.hostname), n = rc.length + hotCount();
        return `<div class="mxw-bar">${push('discard', '', esc(t('discard', 'Annuler les changements')), '', n ? '' : ' disabled')}${push('close', '', esc(t('close', 'Fermer')))}
            <span class="mxw-sum">${n ? esc(t('n_changes', '{n} changement(s)').replace('{n}', n)) + (rc.length ? ` · <b class="mxw-warn-t">${esc(t('n_restart', '{n} redémarre(nt) le mélangeur').replace('{n}', rc.length))}</b>` : ' · ' + esc(t('all_hot', 'tout à chaud'))) : esc(t('no_change', 'Aucun changement'))}</span>
            ${push('apply', '', esc(rc.length ? t('apply_restart', 'Appliquer et redémarrer') : t('apply_short', 'Appliquer')), rc.length ? 'ctl-push--danger ctl-push--led' : 'ctl-push--ok ctl-push--led', (n && !M.busy) ? '' : ' disabled')}</div>`;
    }
    const RLAB = { n_inputs: ['r_n', 'Nombre d\'entrées'], format: ['r_fmt', 'Format'], genlock: ['r_genlock', 'Genlock'], align_budget: ['r_align', 'Attente d\'alignement'],
        n_keyers: ['r_nk', 'Nombre de keyers'], n_dsk: ['r_nd', 'Nombre de DSK'], out_added: ['r_oadd', 'Sortie ajoutée'], out_removed: ['r_orm', 'Sortie retirée'],
        out_renamed: ['r_oren', 'Sortie renommée (autre flux)'], pvw: ['r_pvw', 'Sortie PVW'] };
    const show = (v) => (typeof v === 'boolean' ? (v ? t('yes', 'oui') : t('no', 'non')) : String(v));
    function modal() {
        const M = W.M;
        if (!M.confirm) return '';
        const rc = restartChanges(M.applied, M.cfg, W.o.hostname);
        const who = M.consumers || [];
        return `<div class="mxw-modal-bg"><div class="mxw-modal" role="dialog" aria-modal="true" aria-labelledby="mxw-mt"><h2 id="mxw-mt">${esc(t('confirm_title', 'Redémarrer {h} ?').replace('{h}', W.o.hostname))}</h2>
            <div><b>${esc(t('confirm_cut', 'L\'antenne sera coupée quelques secondes.'))}</b> ${esc(t('confirm_reset', 'Les sorties s\'interrompent, puis l\'état d\'antenne repart (sélections, keyers et DSK à l\'antenne).'))}</div>
            <div class="mxw-rows">${rc.map((x) => `<div class="mxw-r-k">${esc(t(RLAB[x.k][0], RLAB[x.k][1]))}</div><div class="mxw-r-v">${esc(show(x.from))}${x.to !== '' ? ' → ' + esc(show(x.to)) : ''}</div>`).join('')}</div>
            ${who.length ? `<div class="mxw-warn"><b>${esc(t('confirm_who', 'Seront débranchés :'))}</b> ${esc(who.join(' ; '))}</div>` : ''}
            <div class="mxw-foot">${push('confirmNo', '', esc(t('cancel', 'Annuler')))}<span class="mxw-sp"></span>${push('confirmOk', '', esc(t('confirm_ok', 'Redémarrer et appliquer')), 'ctl-push--danger ctl-push--led')}</div></div></div>`;
    }
    function render() {
        if (!W) return;
        const M = W.M;
        const lead = M.first ? `<b>${esc(t('lead_first_b', 'Mélangeur neuf.'))}</b> ${esc(t('lead_first', 'Le guide propose une suite de petites étapes sur le schéma ; vous pouvez aussi toucher directement n\'importe quel élément.'))}`
            : `<b>${esc(t('lead_live_b', '{h} est en service.').replace('{h}', W.o.hostname))}</b> ${esc(t('lead_live', 'Touchez un élément pour le changer. Les réglages marqués ↻ redémarrent le mélangeur ; les autres s\'appliquent à chaud.'))}`;
        const focus = document.activeElement && W.root.contains(document.activeElement) ? (document.activeElement.dataset && (document.activeElement.dataset.a || document.activeElement.dataset.el)) : null;
        W.root.innerHTML = `<div class="mxw">
            <div class="mxw-head"><div class="mxw-headline"><span class="mxw-title">${esc(t('title', 'Assistant du mélangeur'))}</span>${stepPills()}</div><span class="mxw-lead">${lead}</span></div>
            ${stepBand()}
            ${M.err ? `<div class="mxw-warn" role="alert">${esc(M.err)}</div>` : ''}
            <div class="mxw-grid"><div><div class="mxw-sch-wrap">${schematic()}</div>
              <div class="mxw-legend"><span>${esc(t('lg_layer', 'keyer / DSK'))}</span><span class="lw">${esc(t('lg_web', 'page web (onglet Chromium)'))}</span><span class="lo">${esc(t('lg_off', 'désactivé / à ajouter'))}</span>${M.first ? '' : `<span class="lr">${esc(t('lg_restart', 'redémarre le mélangeur'))}</span>`}</div>
              <div class="mxw-help">${esc(t('legend_help', 'Le fond suit entrées → BGD A / B → transition ; les couches s\'empilent au-dessus et prennent leurs sources directement aux entrées, par le faisceau vertical — tiretés : key d\'un linéaire. Pastille pleine = couche montrée sur cette sortie ; point rouge = compte pour le tally.'))}</div></div>
              ${panelHTML()}</div>
            ${applyBar()}${modal()}</div>`;
        if (focus) { const el = W.root.querySelector(`[data-a="${focus}"],[data-el="${focus}"]`); if (el && el.focus) try { el.focus(); } catch (_) {} }
    }

    /* ── gestes ── */
    function onClick(e) {
        const a = e.target.closest && e.target.closest('[data-a]');
        if (a && a.tagName !== 'INPUT' && !a.disabled && !a.closest('svg')) { act(a.dataset.a, a.dataset.v); return; }
        const g = e.target.closest && e.target.closest('[data-el]');
        if (g) act('el', g.dataset.el);
    }
    function onKey(e) {
        const g = e.target.closest && e.target.closest('[data-el]');
        if (g && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); act('el', g.dataset.el); }
        if (e.key === 'Escape' && W && W.M.confirm) act('confirmNo');
    }
    function onInput(e) {
        const el = e.target, a = el.dataset && el.dataset.a;
        if (!a || !W) return;
        if (a.startsWith('url:')) { const [, f, i] = a.split(':'); (f === 'k' ? W.M.cfg.keyers : W.M.cfg.dsks)[Number(i)].url = el.value.trim(); }
        if (a.startsWith('oname:')) { const o = outByUid(W.M.cfg, a.slice(6)); if (o) o.name = el.value.slice(0, NAME_MAX); }
    }
    function onChange(e) {
        const el = e.target, a = el.dataset && el.dataset.a;
        if (!a || !W) return;
        const c = W.M.cfg;
        if (el.type === 'checkbox') {
            if (a.startsWith('lo:')) { const [, ou, lu] = a.split(':'); setLayerOn(c, ou, lu, el.checked); }
            else if (a.startsWith('otally:')) { const o = outByUid(c, a.slice(7)); if (o) o.tally = el.checked; }
            else if (a === 'pvwon') { c.pvw.on = el.checked; if (!outByUid(c, c.pvw.base)) c.pvw.base = c.outs[0].uid; }
            else if (a === 'genlock') c.genlock = el.checked;
            else if (a === 'temit') c.tally.emit = el.checked;
            render(); return;
        }
        if (el.type === 'number') {
            const v = asInt(el.value, 0);
            if (a === 'n') setInputs(c, v);
            else if (a === 'align') c.align = Math.max(0, Math.min(4, v));
            else if (a === 'shift') c.panel.shift = v > 0 ? Math.min(100, v) - 1 : null;
            render(); return;
        }
        if (a.startsWith('oname:')) { const o = outByUid(c, a.slice(6)); if (o) { o.name = el.value.trim().slice(0, NAME_MAX) || o.name; } render(); }
        if (a.startsWith('url:')) render();
    }
    function setLayerOn(c, ou, lu, on) {
        const o = outByUid(c, ou); if (!o) return;
        o.layers = on ? stack(c).map((l) => l.uid).filter((x) => x === lu || o.layers.includes(x)) : o.layers.filter((x) => x !== lu);
    }
    function act(a, v) {
        if (!W) return;
        const M = W.M, c = M.cfg;
        switch (a) {
            case 'el':
                if (v.startsWith('pill:')) { const [, ou, lu] = v.split(':'); const o = outByUid(c, ou); if (o) setLayerOn(c, ou, lu, !o.layers.includes(lu)); break; }
                if (v === 'addk') { const l = addLayer(c, 'k'); if (l) M.sel = 'k:' + (c.keyers.length - 1); }
                else if (v === 'addd') { const l = addLayer(c, 'd'); if (l) M.sel = 'd:' + (c.dsks.length - 1); }
                else if (v === 'addo') { const o = addOutput(c, T); if (o) M.sel = 'o:' + o.uid; }
                else M.sel = M.sel === v ? null : v;
                break;
            case 'close':
                if (M.sel) { M.sel = null; break; }
                return finish('closed');
            case 'rm': { const [f, i] = v.split(':'); removeLayer(c, f, Number(i)); M.sel = null; break; }
            case 'rmo': removeOutput(c, v); M.sel = null; break;
            case 'oall': { const o = outByUid(c, v); if (o) o.layers = stack(c).map((l) => l.uid); break; }
            case 'onone': { const o = outByUid(c, v); if (o) o.layers = []; break; }
            case 'pvwbase': c.pvw.base = v; break;
            case 'fmt': { const f = (M.formats || [])[Number(v)]; if (f) c.fmt = { w: f.w, h: f.h, fps: f.fps, scan: f.scan, chroma: f.chroma || null, bit_depth: f.bit_depth || null, colorimetry: f.colorimetry || null }; break; }
            case 'ltype': { const [f, i, ty] = v.split(':'); const l = (f === 'k' ? c.keyers : c.dsks)[Number(i)]; if (l && KTYPES.includes(ty)) l.t = ty; break; }
            case 'lsrc': { const [f, i, w, n] = v.split(':'); const l = (f === 'k' ? c.keyers : c.dsks)[Number(i)]; if (l && (w === 'fill' || w === 'key')) l[w] = Math.max(0, Math.min(c.n - 1, Number(n))); break; }
            case 'lmode': c.sync.mode = v === 'fastest' ? 'fastest' : 'fixed_1'; break;
            case 'delays':
                // Quitter l'assistant perdrait les changements en cours : les appliquer ou les annuler d'abord.
                if (JSON.stringify(M.cfg) !== JSON.stringify(M.applied)) { M.err = t('lib_pending', 'Appliquez ou annulez d\'abord vos changements.'); break; }
                return finish('delays');
            case 'pchoice': c.panel.choice = v; break;
            case 'pip': c.panel.ip = v || ''; break;
            case 'prow': c.panel.row = Number(v) || 0; break;
            case 'tpgm': c.tally.pgm = v; break;
            case 'tpvw': c.tally.pvw = v; break;
            case 'tourOpen': M.sel = v; break;
            case 'tourGo': M.tour = Math.max(0, Math.min(STEPS.length - 1, asInt(v, 0))); M.sel = null; break;
            case 'tourNext': M.tour = Math.min(STEPS.length - 1, M.tour + 1); M.sel = null; break;
            case 'tourPrev': M.tour = Math.max(0, M.tour - 1); M.sel = null; break;
            case 'tourSkip': M.tour = -1; M.sel = null; break;
            case 'tourRestart': M.tour = 0; break;
            case 'later': return finish('later');
            case 'discard': M.cfg = clone(M.applied); M.sel = null; M.err = ''; break;
            case 'apply': apply(false); return;
            case 'confirmOk': M.confirm = null; apply(true); return;
            case 'confirmNo': M.confirm = null; break;
            default: return;
        }
        render();
    }
    function finish(kind, info) {
        const cb = W && W.o.onClose;
        close();
        if (cb) cb(kind, info || {});
    }
    async function postJson(url, body) {
        const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        const d = await r.json().catch(() => ({}));
        return { ok: r.ok, status: r.status, d };
    }
    async function consumers() {
        const M = W.M, host = W.o.hostname, a = M.applied, c = M.cfg;
        const lost = a.outs.filter((p) => { const o = c.outs.find((x) => x.id === p.id); return !o || outShm(host, o, a) !== (p.shm || outShm(host, p, null)); }).map((p) => p.shm).filter(Boolean);
        if (a.pvw.on && !c.pvw.on) lost.push(host + '_pvw');
        if (!lost.length) return [];
        try {
            const r = await fetch(`/api/containers/${W.o.vmid}/flow_consumers`, { cache: 'no-store' });
            const d = await r.json();
            const by = new Map((d.outputs || []).map((o) => [o.shm, o.consumers || []]));
            const who = [];
            lost.forEach((s) => (by.get(s) || []).forEach((x) => who.push(`${s} → #${x.vmid} ${x.hostname}`)));
            return who;
        } catch (_) { return []; }
    }
    // Appliquer : le serveur d'abord (hook `setup`, sans confirmation) ; confirm_required → ↻ confirmé.
    async function apply(confirmed) {
        const M = W.M, o = W.o, vmid = o.vmid;
        if (M.busy) return;
        const c = assignIds(clone(M.cfg), M.applied);
        if (c.outs.some((x) => !x.name.trim())) { M.err = t('err_name', 'Chaque sortie doit avoir un nom.'); render(); return; }
        M.busy = true; M.err = ''; render();
        try {
            const r = await postJson(`/api/containers/${vmid}/control/setup`, buildSetup(c, M.first ? null : M.applied, confirmed));
            if (!r.ok && r.d.error === 'confirm_required') {
                M.busy = false; M.consumers = await consumers(); M.confirm = true; render(); return;
            }
            if (!r.ok) throw new Error(r.d.error || ('HTTP ' + r.status));
            const restart = !!r.d.restart || r.d.status === 'deploiement_en_cours';
            const errs = [];
            const a = M.applied;
            if (!restart) {
                for (const op of layerPushes(a, c)) {
                    const x = await postJson(`/api/containers/${vmid}/plugin/${op.path}`, op.body);
                    if (!x.ok) errs.push(op.path + ' : ' + (x.d.error || x.status));
                }
                // Délai par défaut : déjà persisté par le hook, poussé ici au moteur (à chaud).
                if (a.sync.mode !== c.sync.mode) { const x = await postJson(`/api/containers/${vmid}/plugin/latency`, { mode: c.sync.mode }); if (!x.ok) errs.push('latency : ' + (x.d.error || x.status)); }
            }
            if (JSON.stringify(a.tally) !== JSON.stringify(c.tally)) {
                const x = await postJson(`/api/containers/${vmid}/plugin_config`, { params: { tally_emit: !!c.tally.emit, tally_pgm_color: c.tally.pgm, tally_pvw_color: c.tally.pvw } });
                if (!x.ok) errs.push('tally : ' + (x.d.error || x.status));
            }
            const p = c.panel, pa = a.panel || {};
            if (o.canSettings && p.choice === 'now' && JSON.stringify(p) !== JSON.stringify(pa) && (p.ip || pa.ip)) {
                const panels = M.panels || [];
                if (pa.ip && pa.ip !== p.ip) {     // ancien pupitre : la rangée de CE mélangeur est libérée
                    const old = panels.find((x) => x.ip === pa.ip);
                    if (old) { const me = old.me.map((v) => (v === vmid ? null : v)); const x = await postJson('/api/atem/assign', { ip: pa.ip, me }); if (!x.ok) errs.push('ATEM : ' + (x.d.error || x.status)); }
                }
                if (p.ip) {
                    const cur = panels.find((x) => x.ip === p.ip) || { me: [] };
                    const me = cur.me.map((v) => (v === vmid ? null : v));
                    while (me.length <= p.row) me.push(null);
                    me[p.row] = vmid;
                    const body = { ip: p.ip, me };
                    if (c.n > 8) body.shift_key = p.shift;
                    const x = await postJson('/api/atem/assign', body);
                    if (!x.ok) errs.push('ATEM : ' + (x.d.error || x.status));
                }
            }
            M.busy = false;
            finish('applied', { restart, errors: errs });
        } catch (e) {
            if (!W) return;
            M.busy = false; M.err = t('err_apply', 'Configuration refusée : {e}').replace('{e}', e.message); render();
        }
    }

    return {
        open, close,
        // Modèle exporté pour la garde (tests/verif_mixer_assistant_ui.js) : fonctions PURES.
        model: { fromLive, stack, slug, outShm, assignIds, restartChanges, layerPushes, buildSetup, addLayer, removeLayer,
                 setInputs, addOutput, removeOutput, syncSummary, STEPS },
    };
})();
