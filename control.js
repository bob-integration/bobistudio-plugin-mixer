// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// Mélangeur — UI de contrôle embarquée (plugin). Montée par le shell Traitements via
// window.MXLPlugins.mixer.mount(el, vmid, ctx). Contrôle via le proxy plugin générique
// /api/containers/<vmid>/plugin/* (endpoints déclarés au manifeste ; /state et
// /preview.png en lecture via read_endpoints). ATEM appelle :8082 directement — inchangé.
'use strict';
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins.mixer = (function () {
    let EL = null, VMID = null, TOAST = () => {};
    let state = null, errorMsg = null;
    let tbarCtl = null;   // T-barre du catalogue (BobiControls.create('tbar')), recréée à chaque rendu complet
    let lastTransition = null, lastPgm = null, lastPvw = null;
    let mixSendPending = false, mixLatestAlpha = null;
    // Réglage de volet (0.23.44) : forme, angle, centre, symétrie, répétition, sens, douceur, bordure.
    const WIPE_DEF={direction:'linear', angle:0, center_x:0.5, center_y:0.5, symmetry:false, repeat:1, reverse:false,
                    alternate:false, softness:0.10, border_width:0, border_color:'#ffffff', border_soft_in:0, border_soft_out:0};
    let preset = { mode:"dissolve", wipe:Object.assign({}, WIPE_DEF), composite:null };
    let wipeKnobs = {}, wipeEditUntil = 0, wipePostTimer = null;
    // Keyers amont (0.23.14) : `keyers[i]` = réglages et état du keyer i+1 tels que /state les publie.
    // `keyer` = le keyer 1 (bouton KEYER TOGGLE, forme d'avant 0.23.14).
    let keyers = [defaultKeyer()];
    let keyer = keyers[0];
    let targets = {bgd:true, keyers:[false]};
    // DSK (0.23.52) : `dsks[j]` = le DSK j+1 tel que /state le publie (`dsks`) ; `layers` = keyers puis
    // DSK — le panneau de réglages adresse une COUCHE par son rang dans cette liste (i < keyers.length :
    // un keyer, sinon le DSK i − keyers.length). `dskEngine` : le moteur publie `dsks` ; sinon (moteur
    // d'avant 0.23.52) l'ancien habillage, une bascule seule.
    let dsks = [], layers = [], dskEngine = false, sideDsk = 0;
    let dskEnabled = false;
    let dskPreviewTimer = null, dskPreviewBusy = false, dskPreviewObjUrl = null;
    let pollTimer = null;
    let eventsAbort = null, eventsRetry = null;
    // Onglet affiché : exploitation (bus, T-bar) ou mise en place des délais. Préférence locale
    // du poste, jamais une donnée partagée.
    let view = 'ops';
    try { view = localStorage.getItem('mx-view') === 'delays' ? 'delays' : 'ops'; } catch (_) {}
    let prevTiming = null;   // instantané précédent de inputs_timing → taux de replis récent
    // Panneau latéral de réglages (0.23.16) : ouvert/fermé et keyer choisi, préférences du POSTE.
    let sideOpen = false, sideKeyer = 0;
    // Panneau OUVERT par défaut (demande de l'utilisateur, 0.23.17) : fermé seulement si ce poste l'a fermé.
    sideOpen = true;
    let sideSec = 'keyers';
    try { sideSec = localStorage.getItem('mx-side-sec') || 'keyers'; } catch (_) {}
    try { sideOpen = localStorage.getItem('mx-side-open') !== '0'; } catch (_) {}
    try { sideKeyer = Math.max(0, parseInt(localStorage.getItem('mx-side-k') || '0', 10) || 0); } catch (_) {}
    // Rangées de sources par keyer affichées au pupitre : {"<i>": {fill, key}}, par poste ET par
    // mélangeur (SCOPE = identité d'instance si le shell la donne, sinon le vmid). Masquées par défaut.
    let SCOPE = null, rowPrefs = {};
    let legacyEngine = false, lastTypes = '', lastPresets = '';
    // Cadence CONFIGURÉE du mélangeur (`frame_rate` de /state, 0.23.19) : les durées s'affichent et se
    // saisissent en secondes:images. 0 = moteur plus ancien → millisecondes, comme avant.
    let frameRate = 0, autoTypeOk = false;
    // Types de keyer que le moteur DÉPLOYÉ connaît (caps.keyer_types) : un moteur d'avant 0.23.22
    // refuserait chroma et motif — on ne les propose pas plutôt que de récolter un 400.
    let engineKeyerTypes = null, keyColorAssumed = false;
    // ── TALLY : trois réglages qui vivent ICI, pas au panneau ⚙ ─────────────
    // `tally_emit`, `tally_force` et `tally_level_base` ne sont lus QUE par le service TSL de
    // l'orchestrateur, à partir de `deploy_config` — le conteneur ne les voit jamais. Ils sont
    // donc `redeploiement: false` : les changer n'interrompt pas le flux. Et ils sont
    // `scope: "user"`, donc ils ne s'affichent plus qu'ici.
    // ⚠ Leur vérité est le CONFIG PERSISTÉ, pas `/state` : le conteneur répondrait la photo de
    // son déploiement, qui pour ces trois clés n'est jamais rafraîchie.
    let tallyCfg = null;          // {tally_emit, tally_force, tally_level_base} — null = pas lu
    let tallyNiveaux = null;      // [{value, label}] du site — null = pas lu
    let tallyCtl = null;          // instance du contrôle de catalogue
    const tallyEnVol = new Set(); // clés dont l'écriture n'est pas revenue
    const tallyTimers = {};
    // ── ASSISTANT DE CONFIGURATION (0.23.56) ─────────────────────────────────────────────────────
    // wizard.js (manifeste ui.wizard_js), chargé à la demande. Ouvert à la PREMIÈRE OUVERTURE d'un
    // mélangeur neuf (`setup_pending`, posé par le formulaire de création) pour qui peut déployer ;
    // les autres voient « en attente de configuration ». Un mélangeur sans ce marqueur (déployé avant)
    // est configuré : ni ouverture automatique ni bandeau, l'assistant reste au ⚙ → Assistant.
    let setupPending = false, setupParams = null, setupHost = '', wizRoot = null, wizApi = null, wizLoadP = null, setupBanner = null;

    /* i18n (catalogue plugin.mixer.*). Même contrat que le `tr` local plus bas, hissé au niveau du
     * module pour servir partout : clé non résolue → on garde le français passé en 2e argument. */
    const T = (k, repli) => {
        const v = window.t ? window.t(k) : k;
        return (v && v !== k) ? v : (repli !== undefined ? repli : k);
    };

    const esc = s => String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const $ = sel => EL ? EL.querySelector(sel) : null;
    const $all = sel => EL ? EL.querySelectorAll(sel) : [];

    function showError(m){errorMsg=m; renderError();}
    function clearError(){errorMsg=null; renderError();}
    function renderError(){
        const s=$('#mx-error-slot'); if(!s) return;
        s.innerHTML=errorMsg?`<div class="mx-error" role="alert"><span>⚠ ${esc(errorMsg)}</span>
            <button type="button" aria-label="${esc(T('plugin.mixer.close_error','Fermer'))}" onclick="__mx.clearError()">✕</button></div>`:'';
    }
    function userIsEditing(){
        const ae=document.activeElement; if(!ae) return false;
        return EL&&EL.contains(ae)&&['INPUT','SELECT','TEXTAREA'].includes(ae.tagName);
    }

    // ─── DSK preview ──────────────────────────────────────
    function dskPreviewStop(){
        if(dskPreviewTimer){clearInterval(dskPreviewTimer); dskPreviewTimer=null;}
        if(dskPreviewObjUrl){URL.revokeObjectURL(dskPreviewObjUrl); dskPreviewObjUrl=null;}
        dskPreviewBusy=false;
    }
    // Aperçu d'une page web (0.23.52 : de la couche affichée — keyer ou DSK de type web, `data-preview`
    // = son chemin d'aperçu ; l'ancien habillage = preview.png). Une image VISIBLE à la fois.
    function previewImg(){
        const all=Array.from($all('img[data-preview]')||[]);
        return all.find(im=>!(im.closest&&im.closest('[hidden]')))||$('#mx-dsk-preview-img');
    }
    async function dskPreviewTick(){
        if(dskPreviewBusy||VMID==null) return;
        const img=previewImg(); if(!img){dskPreviewStop(); return;}
        const src=(img.dataset&&img.dataset.preview)||'preview.png';
        dskPreviewBusy=true;
        try {
            const r=await fetch(`/api/containers/${VMID}/plugin/${src}`,{cache:'no-store'});
            if(r.ok && r.status!==204){
                const blob=await r.blob(), url=URL.createObjectURL(blob);
                img.src=url;
                if(dskPreviewObjUrl) URL.revokeObjectURL(dskPreviewObjUrl);
                dskPreviewObjUrl=url;
            }
        } catch(_){}
        finally{dskPreviewBusy=false;}
    }
    function dskPreviewStart(){
        dskPreviewStop(); if(VMID==null||!previewImg()) return;
        dskPreviewTick(); dskPreviewTimer=setInterval(dskPreviewTick,200);
    }

    // ─── SVGs / HTML bits ─────────────────────────────────
    // Cibles : le fond et chaque keyer (« KEY » seul quand il n'y en a qu'un, comme avant 0.23.14).
    function targetTypes(){
        const n=Math.max(1,keyers.length);
        return [{key:'bgd',label:'BGD'}].concat(Array.from({length:n},(_,i)=>({key:'k'+i,label:n===1?'KEY':'KEY '+(i+1)})));
    }
    const MX_MODE_TYPES=[{key:'dissolve',label:'Dissolve'},{key:'additif',label:'Additif'},{key:'wipe',label:'Wipe'}];
    // Formes de volet (0.23.44). Horizontal, vertical et diagonal sont des volets LINÉAIRES à angle
    // prédéfini : au fond et au motif, l'icône pose « linear » + l'angle ; à l'AUTO d'un keyer (sans
    // angle propre) le nom est envoyé tel quel.
    const MX_LEGACY_ANGLES={horizontal:0, vertical:90, diagonal:45};
    const MX_WIPE_TYPES=[{key:'horizontal',label:T('plugin.mixer.wipe_horizontal','Horizontal')},{key:'vertical',label:T('plugin.mixer.wipe_vertical','Vertical')},
        {key:'diagonal',label:T('plugin.mixer.wipe_diagonal','Diagonal')},{key:'iris',label:T('plugin.mixer.wipe_iris','Iris')},
        {key:'box',label:T('plugin.mixer.wipe_box','Boîte')},{key:'diamond',label:T('plugin.mixer.wipe_diamond','Losange')},
        {key:'star',label:T('plugin.mixer.wipe_star','Étoile')},{key:'heart',label:T('plugin.mixer.wipe_heart','Cœur')},
        {key:'clock',label:T('plugin.mixer.wipe_clock','Horloge')}];
    // Icône allumée : la forme, ou — pour un linéaire — le nom dont l'angle prédéfini est l'angle courant.
    function shapeOn(key,dir,ang){
        if(key in MX_LEGACY_ANGLES) return dir===key||(dir==='linear'&&Math.abs((+ang||0)-MX_LEGACY_ANGLES[key])<0.01);
        return dir===key;
    }

    // Pictogrammes des formes (0.23.47) : touche `.ctl-shape` du CATALOGUE, tracés en deux teintes du
    // thème (ctl-shape__a le fond, ctl-shape__b la forme) — plus aucune couleur en dur.
    function wipeIconSvg(k){
        const B='class="ctl-shape__b"';
        const inner={horizontal:`<rect x="0" y="0" width="12" height="24" ${B}/>`,
            vertical:`<rect x="0" y="0" width="24" height="12" ${B}/>`,
            diagonal:`<polygon points="0,0 24,0 0,24" ${B}/>`,
            iris:`<circle cx="12" cy="12" r="7" ${B}/>`,
            box:`<rect x="5" y="7" width="14" height="10" ${B}/>`,
            heart:`<path ${B} d="M12 20 C 8.2 17 3.5 13.8 3.5 9 C 3.5 6.4 5.5 4.6 8 4.6 C 9.7 4.6 11.2 5.6 12 7.2 C 12.8 5.6 14.3 4.6 16 4.6 C 18.5 4.6 20.5 6.4 20.5 9 C 20.5 13.8 15.8 17 12 20 Z"/>`,
            diamond:`<polygon points="12,4 20,12 12,20 4,12" ${B}/>`,
            star:`<polygon points="12,3.5 14.1,9.1 20.1,9.4 15.4,13.1 17,18.9 12,15.6 7,18.9 8.6,13.1 3.9,9.4 9.9,9.1" ${B}/>`,
            clock:`<path d="M12 12 L12 3 A9 9 0 0 1 20.6 14.8 Z" ${B}/>`};
        return `<svg viewBox="0 0 24 24" aria-hidden="true"><rect width="24" height="24" class="ctl-shape__a"/>${inner[k]||''}</svg>`;
    }
    function wipeIconsHtml(){
        return `<div class="ctl-shape-row" role="radiogroup" aria-label="${esc(T('plugin.mixer.wipe_type_aria','Forme du volet'))}">
            ${MX_WIPE_TYPES.map(w=>{ const on=!preset.composite&&shapeOn(w.key,preset.wipe.direction,preset.wipe.angle); return `<button type="button" class="ctl-shape" data-direction="${w.key}"
                role="radio" aria-checked="${on?'true':'false'}" aria-label="${esc(w.label)}" title="${esc(w.label)}"
                onclick="__mx.wipeIconClick('${w.key}')">${wipeIconSvg(w.key)}</button>`; }).join('')}</div>`;
    }
    // Sens du volet (0.23.20) : NORM, N/R (s'inverse de lui-même après chaque volet) ou REV — les
    // trois touches d'un pupitre. En N/R, la touche dit le sens du PROCHAIN volet (N ou R).
    // 2026-10-29 (0.23.43) : touches À VOYANT du catalogue (`ctl-push--led` + `ctl-led`) — la
    // variante `--small` seule n'a aucun état allumé, la touche choisie ne s'allumait donc pas. Et
    // le « · N » / « · R » ajouté à côté disparaît : c'est la LETTRE du prochain sens, dans « N/R »
    // même, qui est allumée (`ctl-push__alt.on`), l'autre atténuée.
    function dirMode(rev,alt){ return alt?'nr':(rev?'rev':'norm'); }
    function nrLabel(rev,alt){
        if(!alt) return 'N/R';
        return `<span class="ctl-push__alt${rev?'':' on'}">N</span>/<span class="ctl-push__alt${rev?' on':''}">R</span>`;
    }
    function dirModesHtml(rev,alt,attrs){
        const cur=dirMode(rev,alt);
        return `<div class="mx-modes mx-dirmodes" role="radiogroup" aria-label="${esc(T('plugin.mixer.wipe_dir','Sens du volet'))}">${
            [['norm','NORM'],['nr','N/R'],['rev','REV']].map(([m,l])=>`<button type="button" class="ctl-push ctl-push--small ctl-push--led" ${attrs(m)}
                role="radio" aria-checked="${cur===m?'true':'false'}" aria-pressed="${cur===m?'true':'false'}"${
                m==='nr'&&alt?` aria-label="${esc(T('plugin.mixer.dir_nr_next','N/R, prochain volet : {d}').replace('{d}',rev?'R':'N'))}"`:''}
                title="${esc(T('plugin.mixer.dir_'+m,{norm:'Toujours dans le sens normal',nr:'Le sens s\'inverse après chaque volet',rev:'Toujours dans le sens inverse'}[m]))}"><span class="ctl-led"></span>${
                m==='nr'?nrLabel(rev,alt):l}</button>`).join('')}</div>`;
    }
    // T-barre : composant du CATALOGUE (geste, clavier, fin de course, animation) ; le mélangeur ne
    // fait que la relier à l'état du moteur (tbarSyncWithState) et envoyer /mix.
    function tbarHtml(){ return `<div class="mx-tbar ctl-tbar" id="mx-tbar"></div>`; }
    function tbarMount(){
        if(tbarCtl){ try{ tbarCtl.destroy(); }catch(_){} tbarCtl=null; }
        const el=$('#mx-tbar'); if(!el||!window.BobiControls) return;
        tbarCtl=window.BobiControls.create('tbar', el, {label:T('plugin.mixer.tbar_aria','T-Bar de transition')});
        tbarCtl.on('input', a=>sendMix(a));
        tbarCtl.on('change', a=>sendMix(a));
    }

    // ─── Pupitre devant, réglages à côté (0.23.16) ───────────────
    // Vue Exploitation = le PUPITRE : bus BGD A / BGD B, rangées de sources des keyers (au choix,
    // par poste), une ligne compacte par keyer, T-bar, transition, CUT/AUTO. Tous les réglages
    // détaillés (keyer choisi, transition, habillage, tally, sauvegardes) vivent dans un PANNEAU
    // LATÉRAL droit, repliable ; à l'étroit (iPad portrait) il passe par-dessus le pupitre et se
    // ferme d'un geste (croix, voile, glissement vers la droite, Échap).
    // Touches de bus : modèle « pupitre » de la famille partagée (ctl-bus ctl-bus--panel).
    function busKeyHtml(bus, i, on, wired, label, aria, action, extra){
        return `<button type="button" class="ctl-bus ctl-bus--panel mx-slot-btn${wired?'':' ctl-bus--unwired'}" data-bus="${bus}" data-idx="${i}"${extra||''}
            role="radio" aria-checked="${on?'true':'false'}" aria-label="${esc(aria)}" onclick="${action}"><span class="ctl-bus-cap">#${i+1}<small>${esc(label)}</small></span></button>`;
    }
    // ─── Rangées de SOURCES paginées (0.23.66, décision de Cyril du 31/10) ─────────────────────────
    // Touches à taille FIXE (catalogue). On compte ce qui tient dans la rangée (busFit) ; si toutes les
    // entrées tiennent, pas de SHIFT. Sinon n−1 entrées + SHIFT (touche du catalogue, au bout), qui
    // passe aux n−1 suivantes — verrouillé, il boucle sur les pages s'il y en a plus de deux, et son
    // libellé dit la page affichée (« 9-16 »). UNE page pour toutes les rangées (BGD A, BGD B, keyers) :
    // les colonnes restent alignées. Recalculé au redimensionnement (ResizeObserver), page gardée
    // valide. SHIFT D'ÉCRAN seulement : rien à voir avec le Shift du pupitre ATEM.
    let busCap=Infinity, busPage=0, busRO=null;
    // Touches qui tiennent dans `width` px (touche `keyW`, écart `gap`) ; Infinity = mesure impossible.
    function busFit(width,keyW,gap){
        if(!(width>0)||!(keyW>0)) return Infinity;
        return Math.max(1,Math.floor((width+(gap||0))/(keyW+(gap||0))));
    }
    // Disposition de `n` entrées pour `cap` touches visibles à la page `page` (ramenée dans les bornes).
    function busLayout(n,cap,page){
        if(!(cap<n)) return {per:n, pages:1, page:0, start:0, end:n, shift:false};
        const per=Math.max(1,cap-1), pages=Math.ceil(n/per), p=(((page|0)%pages)+pages)%pages;
        return {per, pages, page:p, start:p*per, end:Math.min(n,(p+1)*per), shift:true};
    }
    function busN(s){ return (s&&(s.n_inputs||(s.inputs?s.inputs.length:0)))|0; }
    // Les rangées de sources de l'état `s` : BGD A, BGD B, puis les rangées de keyers affichées.
    function srcRowsOf(s){
        const inputs=s.inputs||[], unw=T('plugin.mixer.unwired','non câblé'), BL={pgm:'BGD A',pvw:'BGD B'};
        const rows=['pgm','pvw'].map(bus=>{ const sel=bus==='pgm'?s.pgm:s.pvw;
            return {id:'mx-bus-'+bus, bus, sel, tag:BL[bus], labelId:'mx-'+bus+'-label',
                key:i=>busKeyHtml(bus,i,sel===i,!!inputs[i],inputs[i]||unw,
                    T('plugin.mixer.bus_input_aria','{bus} entrée #{i}').replace('{bus}', BL[bus]).replace('{i}', i),
                    bus==='pgm'?`__mx.clickPgm(${i})`:`__mx.clickPvw(${i})`)}; });
        keyers.forEach((k,i)=>{
            const p=rowPref(i), lin=k.type==='linear'&&!legacyEngine;
            const add=(field,tag)=>rows.push({id:`mx-kb-${i}-${field}`, bus:'dsk', sel:k[field], tag, labelId:`mx-kr-${i}-${field}`,
                key:j=>busKeyHtml('dsk',j,k[field]===j,!!inputs[j],inputs[j]||unw,
                    tag+' #'+(j+1),`__mx.keyerRowPick(${i},'${field}',${j})`,` data-kr="${i}" data-field="${field}"`)});
            if(p.fill) add('input', (lin?T('plugin.mixer.k_row_fill','K{n} FILL'):T('plugin.mixer.k_row_src','K{n} SRC')).replace('{n}',i+1));
            if(p.key&&lin) add('key_input', T('plugin.mixer.k_row_key','K{n} KEY').replace('{n}',i+1));
        });
        return rows;
    }
    // Touches d'une rangée à la page courante, et SHIFT au bout s'il le faut. L'entrée choisie sur la
    // rangée (PGM, PVW, source du keyer) tombée sur une page masquée teinte le SHIFT (ctl-bus--hint).
    function srcKeysHtml(row,n){
        const L=busLayout(n,busCap,busPage);
        let h='';
        for(let i=L.start;i<L.end;i++) h+=row.key(i);
        h=`<span class="mx-bus-keys" role="radiogroup" aria-labelledby="${row.labelId}">${h}</span>`;
        if(!L.shift) return h;
        const range=(L.start+1)+'-'+L.end, sel=row.sel, hidden=Number.isInteger(sel)&&sel>=0&&sel<n&&(sel<L.start||sel>=L.end);
        const aria=T('plugin.mixer.shift_aria','SHIFT : entrées {r} affichées, toucher pour la page suivante').replace('{r}',range)
            +(hidden?' — '+T('plugin.mixer.shift_hidden','#{n} choisie sur une autre page').replace('{n}',sel+1):'');
        return h+`<button type="button" class="ctl-bus ctl-bus--panel mx-shift${hidden?' ctl-bus--hint':''}" data-bus="${row.bus}" data-shift="${row.id}"
            aria-pressed="${L.page>0?'true':'false'}" aria-label="${esc(aria)}" title="${esc(aria)}" onclick="__mx.busShift()"><span class="ctl-bus-cap">${
            esc(T('plugin.mixer.shift','SHIFT'))}<small>${range}</small></span></button>`;
    }
    function busRowHtml(row,n){
        return `<div class="mx-bus-row"><span class="ctl-bus-lab ctl-bus-lab--tag" data-bus="${row.bus}" id="${row.labelId}">${esc(row.tag)}</span>
            <div class="mx-bus-btns" id="${row.id}">${srcKeysHtml(row,n)}</div></div>`;
    }
    // Redessine les touches des rangées de sources (page, sélection, libellés) sans refaire la page —
    // et seulement celles qui ont changé : un rendu sous le doigt perdrait l'appui.
    function busRowsRender(s){
        s=s||state; if(!s) return;
        const n=busN(s);
        srcRowsOf(s).forEach(row=>{
            const el=$('#'+row.id); if(!el) return;
            const h=srcKeysHtml(row,n);
            if(el._mxKeys!==h){ el.innerHTML=h; el._mxKeys=h; }
        });
    }
    // Nouvelle largeur de rangée : combien de touches tiennent ; la page garde sa première entrée visible.
    function busResize(width,keyW,gap){
        const n=busN(state), cap=busFit(width,keyW,gap);
        if(cap===busCap) return false;
        const first=busLayout(n,busCap,busPage).start;
        busCap=cap;
        const L=busLayout(n,cap,0);
        busPage=L.shift?Math.floor(first/L.per):0;
        busRowsRender();
        return true;
    }
    function busMeasure(){
        const el=$('#mx-bus-pgm'); if(!el) return;
        let keyW=NaN, gap=0;
        try{ const cs=window.getComputedStyle(el); keyW=parseFloat(cs.getPropertyValue('--ctl-bus-panel-w')); gap=parseFloat(cs.columnGap)||0; }catch(_){}
        busResize(el.clientWidth,keyW,gap);
    }
    function busObserve(){
        if(busRO){ try{ busRO.disconnect(); }catch(_){} }
        const el=$('#mx-bus-pgm');
        if(!el||typeof window.ResizeObserver!=='function'){ busRO=null; return; }
        busRO=new window.ResizeObserver(()=>busMeasure());
        busRO.observe(el);
    }
    // SHIFT : page suivante, en boucle (verrouillé : la page reste jusqu'au prochain appui).
    function busShift(){
        const L=busLayout(busN(state),busCap,busPage); if(!L.shift) return;
        busPage=(L.page+1)%L.pages;
        busRowsRender();
    }
    function renderEditor(s){
        keyers=keyersFromState(s); keyer=keyers[0]||defaultKeyer();
        dskEngine=Array.isArray(s.dsks); dsks=dskEngine?s.dsks.map(layerOf):[]; layers=keyers.concat(dsks);
        legacyEngine=!Array.isArray(s.keyers);
        outputsFromState(s);
        frameRate=parseInt(s.frame_rate,10)||0;
        if(s.preset) preset=presetFromState(s.preset);   // AVANT le HTML : le sens (N/R) s'y dessine
        autoTypeOk=!!(s.caps&&Array.isArray(s.caps.keyer_auto_modes));
        engineKeyerTypes=(s.caps&&Array.isArray(s.caps.keyer_types))?s.caps.keyer_types:null;
        keyColorAssumed=!!(s.caps&&s.caps.key_colorimetry_assumed);
        if(sideKeyer>=keyers.length) sideKeyer=0;
        if(sideDsk>=dsks.length) sideDsk=0;
        lastTypes=layers.map(k=>k.type+'|'+(k.url||'')).join(',');
        lastPresets=JSON.stringify(s.keyer_presets||null);
        if(s.next_targets) targets=targetsFromState(s.next_targets);
        const n=busN(s), pgm=s.pgm, pvw=s.pvw;
        const srcRows=srcRowsOf(s);
        const transStatus=s.transition?T('plugin.mixer.transition_running',' · transition en cours'):'';
        // Un pupitre ATEM n'a qu'un nombre FIXE de boutons de bus selon son modèle (8 sur la
        // plupart). Au-delà, ces entrées ne sont atteignables que depuis cette page : on le DIT.
        const tr=(k,fb)=>{const v=window.t?window.t(k):''; return (v&&v!==k)?v:fb;};
        const overflowNote=n>8
            ? `<div class="mx-inputs-note meta">${esc(tr('js.mixer.inputs_note',
                "{n} entrées. Un pupitre ATEM n'adresse que les entrées couvertes par ses boutons de bus (8 sur la plupart des modèles) ; les suivantes se pilotent ici.").replace('{n}', n))}</div>`
            : '';
        const cfgWarn=s.config_warning
            ? `<div class="mx-error" role="alert"><span>⚠ ${esc(s.config_warning)}</span></div>` : '';
        EL.innerHTML=`<div class="tabs" role="tablist">
                <button type="button" class="tab${view==='ops'?' active':''}" role="tab" data-view="ops" aria-selected="${view==='ops'}">${esc(T('plugin.mixer.tab_ops','Exploitation'))}</button>
                <button type="button" class="tab${view==='delays'?' active':''}" role="tab" data-view="delays" aria-selected="${view==='delays'}">${esc(T('plugin.mixer.tab_delays','Entrées & délais'))}</button>
            </div>
            <div id="mx-error-slot"></div>
            <div id="mx-view-delays"${view==='delays'?'':' hidden'}></div>
            <div id="mx-view-ops" class="mx-ops${sideOpen?' side-open':''}"${view==='ops'?'':' hidden'}>
            <div class="mx-desk">${cfgWarn}
            <div class="mx-desk-head">
                <div id="mx-state-bar" class="mx-state-bar">BGD A = #${pgm+1} · BGD B = #${pvw+1}${transStatus}</div>
                <button type="button" class="ctl-push ctl-push--small mx-side-toggle" id="mx-side-toggle" aria-controls="mx-side" aria-expanded="${sideOpen?'true':'false'}"
                    onclick="__mx.sideToggle()">${esc(T('plugin.mixer.side_open','Réglages'))}</button>
            </div>
            <div id="mx-atem-chip" class="meta" style="margin:4px 0"></div>
            ${overflowNote}
            <div class="mx-transition-zone">${keyerRowsHtml(srcRows.slice(2),n)}${tbarHtml()}
                <div class="mx-me">
                ${busRowHtml(srcRows[0],n)}
                ${busRowHtml(srcRows[1],n)}
                ${takeBlockHtml(s)}
                </div>
            </div>
            </div>
            <div class="mx-side-scrim" onclick="__mx.sideClose()" aria-hidden="true"></div>
            <aside class="mx-side" id="mx-side" aria-label="${esc(T('plugin.mixer.side_title','Réglages'))}"${sideOpen?'':' hidden'}>
                ${sideHtml(s)}
            </aside>
            </div>`;
        $all('.tabs [data-view]').forEach(b=>b.addEventListener('click',()=>setView(b.dataset.view)));
        renderDelays(s);
        renderError(); bindKeyers(EL); patternKnobsMount(EL); keyerKnobsMount(EL); dveMount(); sideBindGestures();
        busObserve(); busMeasure();      // combien de touches tiennent (et SHIFT s'il en manque)
        tallyCtl = null;                 // l'innerHTML vient de jeter le contrôle précédent
        tallyMonter();
        dskEnabled=!!s.overlay_enabled;
        wipeKnobsMount();
        presetApplyToUi();
        compInk=null; compRender(); compLibSync(s);
        tbarMount(); lastTransition=s.transition||null; lastPgm=s.pgm; lastPvw=s.pvw;
        if(tbarCtl){
            if(s.transition){const elapsed=Math.max(0,Date.now()-(s.transition.start||0)*1000), remaining=Math.max(0,s.transition.duration_ms-elapsed); tbarCtl.set(Math.min(1,elapsed/s.transition.duration_ms)); if(remaining>0) tbarCtl.animateTo(1,remaining);}
            else if(s.manual_mix) tbarCtl.set(Math.max(0,Math.min(1,s.manual_mix.alpha)));
            else tbarCtl.set(0);
        }
        dskPreviewStart();
        refreshAtemChip();
    }

    // ─── Réglages du volet (0.23.44) : rotatifs du CATALOGUE (BobiControls), montés après le rendu ─
    // Géométrie (angle, centre, répétition, symétrie) : figée au départ d'un volet, vaut pour le
    // suivant. Bordure (largeur, douceurs, couleur) et douceur : vivantes, agissent sur le volet en cours.
    const WIPE_KNOBS=[
        {f:'angle',  g:'geo', label:['plugin.mixer.wipe_angle','Angle'], min:0, max:360, step:1, def:0, fmt:'deg',
         tip:['plugin.mixer.wipe_angle_tip','Linéaire : direction du volet. Formes : rotation.']},
        {f:'center_x', g:'geo', label:['plugin.mixer.wipe_center_x','Centre X'], min:0, max:1, step:0.01, def:0.5, fmt:'pct'},
        {f:'center_y', g:'geo', label:['plugin.mixer.wipe_center_y','Centre Y'], min:0, max:1, step:0.01, def:0.5, fmt:'pct'},
        {f:'repeat', g:'geo', label:['plugin.mixer.wipe_repeat','Répétition'], min:1, max:16, step:1, def:1, fmt:'int',
         tip:['plugin.mixer.wipe_repeat_tip','Linéaire : nombre de bandes (stores). Formes : grille N × N.']},
        {f:'border_width', g:'border', label:['plugin.mixer.wipe_border_width','Largeur'], min:0, max:0.5, step:0.005, def:0, fmt:'pct'},
        {f:'border_soft_in', g:'border', label:['plugin.mixer.wipe_border_soft_in','Douceur interne'], min:0, max:1, step:0.01, def:0, fmt:'pct',
         tip:['plugin.mixer.wipe_border_soft_in_tip','Bord de la bordure du côté de la source qui part']},
        {f:'border_soft_out', g:'border', label:['plugin.mixer.wipe_border_soft_out','Douceur externe'], min:0, max:1, step:0.01, def:0, fmt:'pct',
         tip:['plugin.mixer.wipe_border_soft_out_tip','Bord de la bordure du côté de la source qui arrive']}];
    function wipeFmt(fmt){
        return fmt==='deg'?(v=>Math.round(v)+'°'):fmt==='int'?(v=>String(Math.round(v))):(v=>Math.round(v*1000)/10+'%');
    }
    function wipeSettingsHtml(){
        const kn=g=>WIPE_KNOBS.filter(k=>k.g===g).map(k=>`<div class="mx-wknob" data-wf="${k.f}"${k.tip?` title="${esc(T(...k.tip))}"`:''}></div>`).join('');
        const w=preset.wipe;
        return `<div id="mx-wipe-settings" class="mx-wipe-controls mx-wipe-settings" hidden>
            <div class="mx-k-row mx-k-knobs"><span class="mx-preset-section-label">${esc(T('plugin.mixer.wipe_geometry','Géométrie'))}</span>${kn('geo')}
                <button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" id="mx-wipe-sym" aria-pressed="${w.symmetry?'true':'false'}"
                    title="${esc(T('plugin.mixer.wipe_symmetry_tip','Linéaire : départ du centre vers les deux bords (porte coulissante). Horloge : deux aiguilles.'))}"
                    onclick="__mx.wipeSymToggle()"><span class="ctl-led"></span>${esc(T('plugin.mixer.wipe_symmetry','Symétrie'))}</button></div>
            <div class="mx-k-row mx-k-knobs"><span class="mx-preset-section-label">${esc(T('plugin.mixer.wipe_border','Bordure'))}</span>${kn('border')}
                <label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.wipe_border_color','Couleur'))}</span>
                    <input type="color" class="ctl-color" id="mx-wipe-bcolor" value="${esc(String(w.border_color||'#ffffff').toLowerCase())}"
                        aria-label="${esc(T('plugin.mixer.wipe_border_color_aria','Couleur de la bordure du volet'))}" onchange="__mx.wipeColorSet(this.value)"></label></div>
        </div>`;
    }
    function wipeKnobsMount(){
        Object.values(wipeKnobs).forEach(c=>{ try{ c.destroy(); }catch(_){} });
        wipeKnobs={};
        if(!window.BobiControls) return;
        $all('.mx-wknob[data-wf]').forEach(el=>{
            const k=COMP_KNOBS.find(x=>x.f===el.dataset.wf); if(!k) return;   // douceur comprise (0.23.65)
            const v=preset.wipe[k.f]!=null?preset.wipe[k.f]:k.def;
            const c=window.BobiControls.create('knob', el, {label:T(...k.label), min:k.min, max:k.max, step:k.step,
                default:k.def, value:v, format:wipeFmt(k.fmt), integer:k.fmt==='int'});
            c.on('input', x=>wipeFieldSet(k.f, x, false));
            c.on('change', x=>wipeFieldSet(k.f, x, true));
            wipeKnobs[k.f]=c;
        });
    }
    // Un geste de rotatif : la valeur part au moteur au plus toutes les 100 ms, et à la fin du geste.
    function wipeFieldSet(f,v,final){
        preset.wipe[f]=v; wipeEditUntil=Date.now()+1500;
        if(final){ if(wipePostTimer){ clearTimeout(wipePostTimer); wipePostTimer=null; } presetChange(); return; }
        if(!wipePostTimer) wipePostTimer=setTimeout(()=>{ wipePostTimer=null; presetChange(); },100);
    }
    function wipeSymToggle(){ preset.wipe.symmetry=!preset.wipe.symmetry; presetApplyToUi(); presetChange(); }
    function wipeColorSet(v){ preset.wipe.border_color=String(v||'#ffffff').toLowerCase(); presetChange(); }

    // ─── VOLETS COMPOSÉS : bibliothèque et éditeur (0.23.47) ─────────────────────────────────
    // Choix de l'utilisateur (29/10) : maquette A (volet 1 | résultat | volet 2), la CHRONOLOGIE de
    // la B (courses des deux volets, décalage et profondeur réglables sur la ligne de temps) et la
    // BIBLIOTHÈQUE en vignettes animées de la C. Contrôles du catalogue : BobiControls (rotatifs,
    // courbes à poignées, fader), ctl-shape, ctl-push (--tile, --led, --danger), ctl-color,
    // ctl-preview. L'éditeur travaille sur un BROUILLON : rien ne part au moteur avant
    // « Enregistrer » ; « Prochaine transition » rappelle le volet composé enregistré.
    // Les aperçus sont calculés ici avec les formules du moteur (champ de distance, rampe, bordure
    // en deux couches, union = max, intersection = min, décalage = clamp(m1 − c2(a2)),
    // a2 = profondeur · (1 − |2u − 1|)) — une image réduite, pas les octets du moteur.
    const COMP_OPS=[['union','plugin.mixer.comp_op_union','Union'],['intersection','plugin.mixer.comp_op_intersection','Intersection'],
                    ['offset','plugin.mixer.comp_op_offset','Décalage']];
    const COMP_KNOBS=[{f:'softness', g:'geo', label:['plugin.mixer.softness','Douceur'], min:0, max:1, step:0.01, def:0.10, fmt:'pct'}].concat(WIPE_KNOBS);
    let compLib={}, compNamesSeen=null, compDraft=null, compOpen=false, compSel=null, compAnim=true, compT=0;
    let compCtl=[], compCurves=null, compFader=null, compTicking=false, compRaf=0, compT0=0;
    // Bibliothèque COMMUNE (0.23.54, décision de Cyril du 29/10) : celle de l'INSTALLATION, partagée
    // par tous les mélangeurs (app/plugin_library.py, genre wipe_composite). Lecture au login ;
    // enregistrer, supprimer et rappeler sous le droit opérateur (plugins.operate) — l'orchestrateur
    // décide, l'interface traduit son refus. Un rappel COPIE le volet ici (jamais un lien) : supprimé
    // de la bibliothèque, il reste dans les mélangeurs qui l'ont rappelé.
    const LIB_KIND='wipe_composite';
    let libSaves=[], libLoaded=false, libSel=null;
    const compFields=new Map();
    function compDefault(){
        return {op:'union', offset:0.30, depth:0.50,
                a:Object.assign({}, WIPE_DEF, {direction:'iris'}), b:Object.assign({}, WIPE_DEF, {direction:'linear', angle:30})};
    }
    function compClone(c){ return JSON.parse(JSON.stringify(c)); }
    // ── moteur d'aperçu (mêmes formules que le moteur, image réduite) ──
    const STAR_K=0.381966;
    // Cœur : même table que le moteur (_heart_table : 1024 directions, 4000 pas de 0,0005 à 2,
    // interpolation linéaire entre directions) — calculée une fois, à la première vignette.
    let heartTab=null;
    function heartR(p){
        const N=1024;
        if(!heartTab){
            heartTab=new Float64Array(N); const T=4000, t0=0.0005, dt=(2.0-t0)/(T-1);
            for(let i=0;i<N;i++){ const a=i/N*2*Math.PI, c=Math.cos(a), sn=Math.sin(a); let j=0, t=t0;
                for(;j<T;j++){ t=t0+j*dt; const X=t*c, Y=t*sn, q=X*X+Y*Y-1; if(q*q*q-X*X*Y*Y*Y>=0) break; }
                heartTab[i]=j<T?t:t0; }
            heartTab.min=Math.min(...heartTab);
        }
        const x=p/(2*Math.PI)*N, i0=Math.floor(x)%N, f=x-Math.floor(x);
        return heartTab[i0]*(1-f)+heartTab[(i0+1)%N]*f;
    }
    function compGeom(w){
        const name=String(w.direction||'linear'), shape=(name in MX_LEGACY_ANGLES)?'linear':name;
        return {shape, ang:((MX_LEGACY_ANGLES[name]||0)+(+w.angle||0))%360, cx:+(w.center_x??0.5), cy:+(w.center_y??0.5),
                sym:!!w.symmetry, rep:Math.max(1,Math.min(16,Math.round(+w.repeat||1)))};
    }
    function compField(w,W,H){
        const g=compGeom(w), key=[W,H,g.shape,g.ang,g.cx,g.cy,g.sym,g.rep].join('|');
        if(compFields.has(key)) return compFields.get(key);
        const d=new Float32Array(W*H), a=g.ang*Math.PI/180, c=Math.cos(a), sn=Math.sin(a), cx=g.cx*W, cy=g.cy*H, rep=g.rep;
        let umin=Infinity; for(const x of [0,W-1]) for(const y of [0,H-1]) umin=Math.min(umin,(x+.5-cx)*c+(y+.5-cy)*sn);
        const lh=W/2*Math.abs(c)+H/2*Math.abs(sn), P=2*lh/rep, cw=W/rep, chh=H/rep;
        let hx=W/2, hy=H/2; if(g.shape!=='linear'&&rep>1){ hx=cw/2; hy=chh/2; }
        const rd=Math.hypot(hx,hy);
        for(let y=0;y<H;y++) for(let x=0;x<W;x++){
            let u=(x+.5-cx)*c+(y+.5-cy)*sn, v=(y+.5-cy)*c-(x+.5-cx)*sn, dd;
            if(g.shape==='linear'){
                if(g.sym){ if(rep>1){ const ph=rep%2?0:P/2; u=((u-ph+P/2)%P+P)%P-P/2; } dd=Math.abs(u); }
                else { dd=u-umin; if(rep>1) dd=dd%P; }
                dd/=lh;
            } else {
                if(rep>1){ const pu=rep%2?0:cw/2, pv=rep%2?0:chh/2; u=((u-pu+cw/2)%cw+cw)%cw-cw/2; v=((v-pv+chh/2)%chh+chh)%chh-chh/2; }
                const r=Math.hypot(u,v);
                if(g.shape==='box') dd=Math.max(Math.abs(u)/hx,Math.abs(v)/hy);
                else if(g.shape==='diamond') dd=(Math.abs(u)/hx+Math.abs(v)/hy)/2;
                else if(g.shape==='iris') dd=r/rd;
                else if(g.shape==='clock'){ const p=Math.atan2(u,-v); dd=g.sym?Math.abs(p)/Math.PI:((p+2*Math.PI)%(2*Math.PI))/(2*Math.PI); }
                else if(g.shape==='star'){ const z=2*Math.PI/5; let p=((Math.atan2(u,-v)%z)+z)%z; p=Math.min(p,z-p);
                    const c0=Math.cos(Math.PI/5), s0=Math.sin(Math.PI/5), rho=STAR_K*s0/(STAR_K*s0*Math.cos(p)+(1-STAR_K*c0)*Math.sin(p)); dd=r*STAR_K/(rd*rho); }
                else { const p=((Math.atan2(-v,u)%(2*Math.PI))+2*Math.PI)%(2*Math.PI), R=heartR(p); dd=r*heartTab.min/(rd*R); }
            }
            d[y*W+x]=dd;
        }
        let mn=Infinity, mx=-Infinity; for(const v of d){ if(v<mn) mn=v; if(v>mx) mx=v; }
        for(let i=0;i<d.length;i++) d[i]-=mn;
        // Un pixel en unités de d (douceur minimale) : mêmes règles que le moteur (_shape_px).
        const out={d, M:Math.max(mx-mn,1e-6), px:g.shape==='linear'?1/lh:(g.shape==='clock'?0.002:((g.shape==='box'||g.shape==='diamond')?1/Math.min(hx,hy):1/rd))};
        if(compFields.size>40) compFields.clear();
        compFields.set(key,out); return out;
    }
    function compMasks(alpha,w,W,H){
        const {d,M,px}=compField(w,W,H), n=d.length, c=new Float32Array(n), b=new Float32Array(n), a=Math.min(1,Math.max(0,alpha)), rev=!!w.reverse;
        const ramp=(dd,e,s)=>Math.min(1,Math.max(0,(rev?(dd-(M-e-s/2)):(e+s/2-dd))/s));
        const bw=(+w.border_width||0)*M;
        if(bw<=0){ const s=Math.max(px,(w.softness??0.1)*M), mg=.01*s, e=(-s/2-mg)+a*(M+s+2*mg);
            for(let i=0;i<n;i++) c[i]=b[i]=ramp(d[i],e,s); return {c,b,color:null}; }
        const sA=Math.max(px,(+w.border_soft_in||0)*M), sB=Math.max(px,(+w.border_soft_out||0)*M), mg=.01*Math.max(sA,sB);
        const e0=Math.min(-sB/2,-sA/2-bw)-mg, e1=Math.max(M+sB/2,M+sA/2-bw)+mg, e=e0+a*(e1-e0);
        for(let i=0;i<n;i++){ c[i]=ramp(d[i],e+bw,sA); b[i]=ramp(d[i],e,sB); }
        return {c,b,color:w.border_color||'#ffffff'};
    }
    function compA2(C,a){
        if(C.op!=='offset') return a;
        const u=a<=C.offset?0:Math.min(1,(a-C.offset)/(1-C.offset));
        return C.depth*(1-Math.abs(2*u-1));
    }
    function compMix(alpha,C,W,H){
        const a=Math.min(1,Math.max(0,alpha)), a2=compA2(C,a), A=compMasks(a,C.a,W,H), B=compMasks(a2,C.b,W,H);
        const n=A.b.length, c=new Float32Array(n), b=new Float32Array(n);
        for(let i=0;i<n;i++){
            if(C.op==='union'){ b[i]=Math.max(A.b[i],B.b[i]); c[i]=Math.max(A.c[i],B.c[i]); }
            else if(C.op==='intersection'){ b[i]=Math.min(A.b[i],B.b[i]); c[i]=Math.min(A.c[i],B.c[i]); }
            else { b[i]=Math.max(0,A.b[i]-B.c[i]); c[i]=Math.max(0,A.c[i]-B.b[i]); }
        }
        return {c,b,color:(+C.a.border_width>0?C.a.border_color:(+C.b.border_width>0?C.b.border_color:null))||'#ffffff', a2};
    }
    // Teintes du thème : A = fond (rayé), B = texte atténué (uni) — lisibles sans la couleur ; la
    // bordure garde SA couleur (c'est un réglage).
    let compInk=null;
    function compColors(cv){
        if(compInk) return compInk;
        const ctx=cv.getContext('2d'), cs=(window.getComputedStyle&&EL)?window.getComputedStyle(EL):null;
        const rgb=(css,fb)=>{ try{ ctx.fillStyle=fb; ctx.fillStyle=(css||'').trim()||fb; const f=ctx.fillStyle;
            if(f[0]==='#') return [parseInt(f.slice(1,3),16),parseInt(f.slice(3,5),16),parseInt(f.slice(5,7),16)];
            const m=f.match(/\d+(\.\d+)?/g); return m?m.slice(0,3).map(Number):[0,0,0]; }catch(_){ return [0,0,0]; } };
        compInk={a:rgb(cs&&cs.getPropertyValue('--bg-input'),'#181a1f'), a2:rgb(cs&&cs.getPropertyValue('--border'),'#2a2d34'),
                 b:rgb(cs&&cs.getPropertyValue('--text-muted'),'#8a8d94'), rgb};
        return compInk;
    }
    function compPaint(cv,m){
        if(!cv||!cv.getContext) return;
        const W=cv.width, H=cv.height, ctx=cv.getContext('2d'); if(!ctx) return;
        const ink=compColors(cv), col=ink.rgb(m.color||'#ffffff','#ffffff'), img=ctx.createImageData(W,H);
        for(let y=0;y<H;y++) for(let x=0;x<W;x++){
            const i=y*W+x, stripe=((x+y)>>2)&1, A=stripe?ink.a2:ink.a;
            for(let k=0;k<3;k++){ const t=A[k]*(1-m.c[i])+col[k]*m.c[i]; img.data[i*4+k]=t*(1-m.b[i])+ink.b[k]*m.b[i]; }
            img.data[i*4+3]=255;
        }
        ctx.putImageData(img,0,0);
    }
    // ── bibliothèque ──
    async function compLoad(){
        if(VMID==null) return;
        const lib=libFetch();
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/wipe/composites`); if(!r.ok) throw new Error('HTTP '+r.status);
            const j=await r.json(); compLib=(j&&j.composites)||{}; compNamesSeen=JSON.stringify((j&&j.names)||[]);
        }catch(_){ compLib={}; }
        await lib;
        compRender();
    }
    async function libFetch(){
        try{
            const r=await fetch(`/api/plugins/mixer/library?kind=${LIB_KIND}`); if(!r.ok) throw new Error('HTTP '+r.status);
            const j=await r.json(); libSaves=Array.isArray(j&&j.saves)?j.saves:[]; libLoaded=true;
        }catch(_){ libSaves=[]; libLoaded=false; }
        if(libSel!=null&&!libSaves.some(x=>x.id===libSel)) libSel=null;
    }
    async function libLoad(){ await libFetch(); compRender(); }
    function libDoc(sv){ const C=(sv&&sv.summary&&sv.summary.composite)||null;     // le résumé porte le volet entier
        return C&&C.a&&C.b?C:null; }
    function libErr(code,name){
        const known=['name_taken_local','local_full','bad_save','unknown_save','container_unreachable','unknown_composite','bad_name','forbidden'];
        const c=known.includes(code)?code:'other';
        return T('plugin.mixer.lib_err_'+c,{name_taken_local:'un volet de ce mélangeur porte déjà le nom « {name} » et il est différent : renommez-le d\'abord.',
            local_full:'ce mélangeur a déjà son nombre maximal de volets composés.',bad_save:'sauvegarde illisible.',
            unknown_save:'ce volet n\'est plus dans la bibliothèque.',container_unreachable:'mélangeur injoignable.',
            unknown_composite:'volet introuvable dans ce mélangeur (enregistrez-le d\'abord).',bad_name:'nom invalide.',
            forbidden:'droit opérateur requis.',other:'refusé ({code}).'}[c]).replace('{name}',name||'').replace('{code}',code||'');
    }
    async function libReq(method,url,body){
        const r=await fetch(url,Object.assign({method},body?{headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{}));
        let j={}; try{ j=await r.json(); }catch(_){}
        return {ok:r.ok, status:r.status, j:j||{}};
    }
    function libFail(res,name){
        const code=res.status===403?'forbidden':((res.j&&res.j.error)||('HTTP '+res.status));
        showError(T('plugin.mixer.lib_err','Bibliothèque commune : {msg}').replace('{msg}',libErr(code,(res.j&&res.j.params&&res.j.params.name)||name)));
    }
    // « Verser dans la bibliothèque » : le volet tel qu'ENREGISTRÉ dans ce mélangeur (capturé par
    // l'orchestrateur chez lui), jamais le brouillon de l'éditeur. Nom déjà pris → remplacer, sur
    // confirmation.
    async function compShare(){
        if(VMID==null||!compSel) return;
        const name=compSel, url=`/api/containers/${VMID}/library`;
        let res=await libReq('POST',url,{kind:LIB_KIND, name, item:name});
        if(res.status===409&&res.j.error==='name_taken'){
            const msg=T('plugin.mixer.lib_replace_msg','La bibliothèque commune a déjà un volet « {name} ». Le remplacer par celui de ce mélangeur ? Les mélangeurs qui l\'ont déjà rappelé gardent leur copie.').replace('{name}',name);
            const ok=window.mxlConfirm?await window.mxlConfirm({title:T('plugin.mixer.lib_share','Verser dans la bibliothèque'), message:msg,
                    confirmLabel:T('plugin.mixer.lib_replace','Remplacer'), cancelLabel:T('plugin.mixer.out_cancel','Annuler')})
                :window.confirm(msg);
            if(!ok) return;
            res=await libReq('POST',url,{kind:LIB_KIND, name, item:name, overwrite:true});
        }
        if(!res.ok){ libFail(res,name); return; }
        clearError();
        TOAST(T('plugin.mixer.lib_shared','« {name} » versé dans la bibliothèque commune').replace('{name}',name));
        libSel=res.j.id||null; await libLoad();
    }
    function libPick(id){ libSel=(libSel===id)?null:id; compRender(); }
    async function libAction(act){
        const sv=libSaves.find(x=>x.id===libSel); if(!sv||VMID==null) return;
        if(act==='delete'){
            const msg=T('plugin.mixer.lib_delete_msg','Retirer « {name} » de la bibliothèque commune ? Les mélangeurs qui l\'utilisent en gardent leur copie.').replace('{name}',sv.name);
            const ok=window.mxlConfirm?await window.mxlConfirm({title:T('plugin.mixer.lib_delete','Supprimer de la bibliothèque'), message:msg,
                    confirmLabel:T('plugin.mixer.comp_delete','Supprimer'), cancelLabel:T('plugin.mixer.out_cancel','Annuler'), danger:true})
                :window.confirm(msg);
            if(!ok) return;
            const res=await libReq('DELETE',`/api/plugins/mixer/library/${sv.id}`);
            if(!res.ok){ libFail(res,sv.name); return; }
            clearError(); libSel=null; await libLoad(); return;
        }
        if(act==='next'||act==='copy'){
            const res=await libReq('POST',`/api/containers/${VMID}/library/${sv.id}/recall`,{next:act==='next'});
            if(!res.ok){ libFail(res,sv.name); return; }
            clearError();
            if(act==='next'){ preset.mode='wipe'; preset.composite=sv.name; presetApplyToUi(); }
            TOAST(T(act==='next'?'plugin.mixer.lib_recalled_next':'plugin.mixer.lib_recalled_copy',
                    act==='next'?'« {name} » copié ici, prochaine transition':'« {name} » copié dans ce mélangeur').replace('{name}',sv.name));
            await compLoad();
        }
    }
    function compLibSync(s){       // /state porte la liste des noms : la recharger quand elle change
        const names=JSON.stringify((s&&s.wipe_composites)||[]);
        if(s&&Array.isArray(s.wipe_composites)&&names!==compNamesSeen){ compNamesSeen=names; compLoad(); }
        else compTilesLight();
    }
    // Marque d'un volet LOCAL : copie d'un volet de la bibliothèque commune (`lib` = id de la
    // sauvegarde), ou copie dont l'original a été retiré de la bibliothèque depuis.
    function compLibTag(C){
        if(!C||!C.lib) return '';
        const gone=libLoaded&&!libSaves.some(x=>x.id===C.lib);
        return `<span class="ctl-flag ctl-flag--tag" data-ctag="${gone?'gone':'lib'}" title="${esc(gone
            ?T('plugin.mixer.lib_tag_gone_tip','Copie d\'un volet retiré depuis de la bibliothèque commune : il reste ici.')
            :T('plugin.mixer.lib_tag_tip','Copie d\'un volet de la bibliothèque commune'))}">${esc(gone
            ?T('plugin.mixer.lib_tag_gone','Retiré'):T('plugin.mixer.lib_tag','Biblio.'))}</span>`;
    }
    function compTilesHtml(){
        const names=Object.keys(compLib).sort();
        const tiles=names.map(n=>`<button type="button" class="ctl-push ctl-push--tile${preset.composite===n?' on2':''}" data-cname="${esc(n)}"
                aria-pressed="${compOpen&&compSel===n?'true':'false'}" style="--ctl-tone2:var(--bus-pvw)"
                aria-label="${esc(T('plugin.mixer.comp_tile_aria','{name} : ouvrir dans l\'éditeur').replace('{name}',n))}">
                <canvas width="96" height="54" aria-hidden="true"></canvas>
                <span class="ctl-push__foot"><span>${esc(n)}</span>${compLibTag(compLib[n])}<span class="ctl-flag ctl-flag--2">${esc(T('plugin.mixer.comp_next_flag','Prochain'))}</span></span></button>`).join('');
        const sel=libSaves.find(x=>x.id===libSel);
        const common=libSaves.map(sv=>`<button type="button" class="ctl-push ctl-push--tile" data-clib="${sv.id}"
                aria-pressed="${libSel===sv.id?'true':'false'}"
                aria-label="${esc(T('plugin.mixer.lib_tile_aria','{name} (bibliothèque commune) : choisir').replace('{name}',sv.name))}"
                title="${esc(sv.author?T('plugin.mixer.lib_tile_by','Versé par {who}').replace('{who}',sv.author):'')}">
                <canvas width="96" height="54" aria-hidden="true"></canvas>
                <span class="ctl-push__foot"><span>${esc(sv.name)}</span></span></button>`).join('');
        return `<span class="mx-preset-section-label mx-comp-group">${esc(T('plugin.mixer.lib_local','Dans ce mélangeur'))}</span>
            <div class="mx-comp-lib" role="group" aria-label="${esc(T('plugin.mixer.lib_local','Dans ce mélangeur'))}">${tiles}
            <button type="button" class="ctl-push ctl-push--tile mx-comp-new" data-cnew aria-pressed="${compOpen&&!compSel?'true':'false'}">
                <span class="mx-comp-plus" aria-hidden="true">+</span><span>${esc(T('plugin.mixer.comp_new','Nouveau volet composé'))}</span></button></div>
            ${names.length?'':`<p class="meta">${esc(T('plugin.mixer.comp_none','Aucun volet composé dans ce mélangeur.'))}</p>`}
            <div class="mx-comp-common" data-ccommon>
                <span class="mx-preset-section-label mx-comp-group">${esc(T('plugin.mixer.lib_common','Bibliothèque commune'))}</span>
                <p class="meta">${esc(T('plugin.mixer.lib_common_hint','Partagée par tous les mélangeurs de l\'installation. Un rappel en fait une copie dans ce mélangeur.'))}</p>
                ${libLoaded?(libSaves.length?`<div class="mx-comp-lib" role="group" aria-label="${esc(T('plugin.mixer.lib_common','Bibliothèque commune'))}">${common}</div>`
                    :`<p class="meta">${esc(T('plugin.mixer.lib_empty','La bibliothèque commune est vide : « Verser dans la bibliothèque », dans l\'éditeur, y range un volet de ce mélangeur.'))}</p>`)
                    :`<p class="meta">${esc(T('plugin.mixer.lib_unavailable','Bibliothèque commune indisponible.'))}</p>`}
                ${sel?`<div class="mx-k-row mx-comp-actions" data-clibbar>
                    <button type="button" class="ctl-push ctl-push--small" data-lact="next"
                        title="${esc(T('plugin.mixer.lib_next_tip','Copie ce volet dans ce mélangeur et en fait la prochaine transition (Wipe)'))}">${esc(T('plugin.mixer.comp_recall','Prochaine transition'))}</button>
                    <button type="button" class="ctl-push ctl-push--small" data-lact="copy">${esc(T('plugin.mixer.lib_copy','Copier dans ce mélangeur'))}</button>
                    <button type="button" class="ctl-push ctl-push--small ctl-push--danger" data-lact="delete">${esc(T('plugin.mixer.lib_delete','Supprimer de la bibliothèque'))}</button>
                </div>`:''}
            </div>`;
    }
    function compTilesLight(){
        $all('[data-cname]').forEach(b=>{ b.classList.toggle('on2',preset.composite===b.dataset.cname);
            b.setAttribute('aria-pressed',compOpen&&compSel===b.dataset.cname?'true':'false'); });
    }
    function compSideHtml(side){
        const w=compDraft[side], lab=side==='a'?T('plugin.mixer.comp_side_a','Volet 1'):T('plugin.mixer.comp_side_b','Volet 2');
        return `<div class="mx-comp-side" data-cside="${side}">
            <span class="mx-preset-section-label">${esc(lab)}</span>
            <canvas class="ctl-preview" width="128" height="72" data-cpv="${side}" aria-hidden="true"></canvas>
            <div class="ctl-shape-row" role="radiogroup" aria-label="${esc(lab+' — '+T('plugin.mixer.wipe_type_aria','Forme du volet'))}">${
                MX_WIPE_TYPES.map(t=>`<button type="button" class="ctl-shape" role="radio" data-cshape="${t.key}" data-cside="${side}"
                    aria-checked="${shapeOn(t.key,w.direction,w.angle)?'true':'false'}" aria-label="${esc(t.label)}" title="${esc(t.label)}">${wipeIconSvg(t.key)}</button>`).join('')}</div>
            <div class="mx-k-row">
                <button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" data-cflag="symmetry" data-cside="${side}" aria-pressed="${w.symmetry?'true':'false'}"
                    title="${esc(T('plugin.mixer.wipe_symmetry_tip','Linéaire : départ du centre vers les deux bords (porte coulissante). Horloge : deux aiguilles.'))}"><span class="ctl-led"></span>${esc(T('plugin.mixer.wipe_symmetry','Symétrie'))}</button>
                <button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" data-cflag="reverse" data-cside="${side}" aria-pressed="${w.reverse?'true':'false'}"
                    title="${esc(T('plugin.mixer.comp_rev_tip','Ce volet dans le sens inverse (une forme se referme)'))}"><span class="ctl-led"></span>REV</button>
                <label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.wipe_border_color','Couleur'))}</span>
                    <input type="color" class="ctl-color" data-ccolor="${side}" value="${esc(String(w.border_color||'#ffffff').toLowerCase())}"
                        aria-label="${esc(lab+' — '+T('plugin.mixer.wipe_border_color_aria','Couleur de la bordure du volet'))}"></label></div>
            <div class="mx-k-row mx-k-knobs">${COMP_KNOBS.map(k=>`<div class="mx-cknob" data-cside="${side}" data-cf="${k.f}"${k.tip?` title="${esc(T(...k.tip))}"`:''}></div>`).join('')}</div>
        </div>`;
    }
    function compEditorHtml(){
        const C=compDraft;
        return `<div class="mx-comp-ed" id="mx-comp-ed">
            <div class="mx-comp-grid">
                ${compSideHtml('a')}
                <div class="mx-comp-mid">
                    <div class="mx-k-row" role="radiogroup" aria-label="${esc(T('plugin.mixer.comp_op','Combinaison'))}">${
                        COMP_OPS.map(([k,key,fb])=>`<button type="button" class="ctl-push ctl-push--led ctl-push--small" role="radio" data-cop="${k}"
                            aria-checked="${C.op===k?'true':'false'}"><span class="ctl-led"></span>${esc(T(key,fb))}</button>`).join('')}</div>
                    <span class="mx-preset-section-label">${esc(T('plugin.mixer.comp_result','Résultat'))}</span>
                    <canvas class="ctl-preview" width="192" height="108" data-cpv="r" aria-hidden="true"></canvas>
                    <span class="mx-preset-section-label">${esc(T('plugin.mixer.comp_timeline','Course des deux volets'))}</span>
                    <div id="mx-comp-curves"></div>
                    <div class="mx-k-row mx-k-knobs" id="mx-comp-opknobs"${C.op==='offset'?'':' hidden'}>
                        <div class="mx-cknob" data-cside="op" data-cf="offset"></div><div class="mx-cknob" data-cside="op" data-cf="depth"></div></div>
                    <div class="mx-k-row">
                        <button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" id="mx-comp-anim" aria-pressed="${compAnim?'true':'false'}"><span class="ctl-led"></span>${esc(T('plugin.mixer.comp_anim','Animer'))}</button>
                        <div class="mx-comp-scrub" id="mx-comp-scrub"></div></div>
                </div>
                ${compSideHtml('b')}
            </div>
            <div class="mx-k-row mx-comp-actions">
                <input type="text" id="mx-comp-name" maxlength="32" value="${esc(compSel||'')}" placeholder="${esc(T('plugin.mixer.comp_name','Nom du volet composé'))}"
                    aria-label="${esc(T('plugin.mixer.comp_name','Nom du volet composé'))}">
                <button type="button" class="ctl-push ctl-push--small" data-cact="save">${esc(T('plugin.mixer.comp_save','Enregistrer'))}</button>
                <button type="button" class="ctl-push ctl-push--small ctl-push--led" data-cact="recall" aria-pressed="${compSel&&preset.composite===compSel?'true':'false'}"${compSel?'':' disabled'}
                    title="${esc(T('plugin.mixer.comp_recall_tip','La prochaine transition (Wipe) sera ce volet composé, tel qu\'enregistré'))}"><span class="ctl-led"></span>${esc(T('plugin.mixer.comp_recall','Prochaine transition'))}</button>
                <button type="button" class="ctl-push ctl-push--small" data-cact="share"${compSel?'':' disabled'}
                    title="${esc(T('plugin.mixer.lib_share_tip','Range dans la bibliothèque commune ce volet tel qu\'enregistré dans ce mélangeur (pas le brouillon en cours)'))}">${esc(T('plugin.mixer.lib_share','Verser dans la bibliothèque'))}</button>
                <button type="button" class="ctl-push ctl-push--small ctl-push--danger" data-cact="delete"${compSel?'':' disabled'}>${esc(T('plugin.mixer.comp_delete','Supprimer'))}</button>
                <button type="button" class="ctl-push ctl-push--small" data-cact="close">${esc(T('plugin.mixer.comp_close','Fermer'))}</button>
            </div></div>`;
    }
    function compDestroy(){
        compCtl.forEach(c=>{ try{ c.destroy(); }catch(_){} }); compCtl=[];
        if(compCurves){ try{ compCurves.destroy(); }catch(_){} } compCurves=null;
        if(compFader){ try{ compFader.destroy(); }catch(_){} } compFader=null;
    }
    function compRender(){
        const host=$('#mx-comp'); if(!host) return;
        compDestroy();
        host.innerHTML=`<span class="mx-preset-section-label">${esc(T('plugin.mixer.comp_title','Volets composés'))}</span>${compTilesHtml()}${compOpen&&compDraft?compEditorHtml():''}`;
        const ops=$('#mx-view-ops'); if(ops&&ops.classList) ops.classList.toggle('side-wide', !!(compOpen&&compDraft));
        compBind(host);
        compStart();
    }
    function compBind(host){
        if(!host.querySelectorAll) return;
        host.querySelectorAll('[data-cname]').forEach(b=>b.addEventListener('click',()=>compEdit(b.dataset.cname)));
        host.querySelectorAll('[data-cnew]').forEach(b=>b.addEventListener('click',()=>compEdit(null)));
        host.querySelectorAll('[data-clib]').forEach(b=>b.addEventListener('click',()=>libPick(Number(b.dataset.clib))));
        host.querySelectorAll('[data-lact]').forEach(b=>b.addEventListener('click',()=>libAction(b.dataset.lact)));
        if(!(compOpen&&compDraft)) return;
        host.querySelectorAll('[data-cshape]').forEach(b=>b.addEventListener('click',()=>compShape(b.dataset.cside,b.dataset.cshape)));
        host.querySelectorAll('[data-cflag]').forEach(b=>b.addEventListener('click',()=>compFlag(b.dataset.cside,b.dataset.cflag)));
        host.querySelectorAll('[data-ccolor]').forEach(i=>i.addEventListener('change',()=>{ compDraft[i.dataset.ccolor].border_color=String(i.value||'#ffffff').toLowerCase(); }));
        host.querySelectorAll('[data-cop]').forEach(b=>b.addEventListener('click',()=>compOp(b.dataset.cop)));
        host.querySelectorAll('[data-cact]').forEach(b=>b.addEventListener('click',()=>compAction(b.dataset.cact)));
        const an=host.querySelector('#mx-comp-anim'); if(an&&an.addEventListener) an.addEventListener('click',()=>compAnimSet(!compAnim));
        if(!window.BobiControls) return;
        host.querySelectorAll('.mx-cknob[data-cf]').forEach(el=>{
            const side=el.dataset.cside, f=el.dataset.cf;
            const d=side==='op'
                ? (f==='offset'?{label:['plugin.mixer.comp_offset','Décalage'], min:0, max:0.9, step:0.01, def:0.3, fmt:'pct'}
                               :{label:['plugin.mixer.comp_depth','Profondeur'], min:0.05, max:1, step:0.01, def:0.5, fmt:'pct'})
                : COMP_KNOBS.find(k=>k.f===f);
            if(!d) return;
            const obj=side==='op'?compDraft:compDraft[side];
            const c=window.BobiControls.create('knob', el, {label:T(...d.label), min:d.min, max:d.max, step:d.step, default:d.def,
                value:(obj[f]!=null?obj[f]:d.def), format:wipeFmt(d.fmt), integer:d.fmt==='int'});
            const set=v=>{ obj[f]=v; if(side==='op') compCurvesSync(); };
            c.on('input', set); c.on('change', set);
            c._cf=side+'.'+f; compCtl.push(c);
        });
        const cv=host.querySelector('#mx-comp-curves');
        if(cv&&cv.classList){
            compCurves=window.BobiControls.create('curves', cv, {label:T('plugin.mixer.comp_timeline','Course des deux volets'),
                handles:[{key:'offset', axis:'x', x:compDraft.offset, y:0, xmin:0, xmax:0.9, tone:'2', label:T('plugin.mixer.comp_offset_handle','Départ du second volet')},
                         {key:'depth', axis:'y', x:(1+compDraft.offset)/2, y:compDraft.depth, ymin:0.05, ymax:1, tone:'2', label:T('plugin.mixer.comp_depth_handle','Profondeur du second volet')}]});
            const mv=(r)=>{ if(!r) return; if(r.key==='offset'){ compDraft.offset=r.x; } else compDraft.depth=r.y; compCurvesSync(); compKnobsOpSync(); };
            compCurves.on('input', mv); compCurves.on('change', mv);
            compCurvesSync();
        }
        const sc=host.querySelector('#mx-comp-scrub');
        if(sc&&sc.classList){
            compFader=window.BobiControls.create('fader', sc, {label:T('plugin.mixer.comp_scrub','Position de l\'aperçu'), orientation:'horizontal',
                min:0, max:1, step:0.001, value:compT, format:v=>Math.round(v*100)+'%'});
            const f=v=>{ compT=v; if(compAnim) compAnimSet(false); compDraw(true); };
            compFader.on('input', f); compFader.on('change', f);
        }
    }
    function compCurvesSync(){
        if(!compCurves) return;
        const C=compDraft, pa=[], pb=[];
        for(let i=0;i<=50;i++){ const a=i/50; pa.push([a,a]); pb.push([a,compA2(C,a)]); }
        compCurves.setCurves([{points:pa, tone:'accent'}, {points:pb, tone:'2', dashed:C.op!=='offset'}]);
        const off=C.op==='offset';
        compCurves.showHandle('offset',off); compCurves.showHandle('depth',off);
        compCurves.setHandle('offset',C.offset,0);
        compCurves.setHandle('depth',(1+C.offset)/2,C.depth);
    }
    function compKnobsOpSync(){
        compCtl.forEach(c=>{ if(c._cf==='op.offset') c.set(compDraft.offset); else if(c._cf==='op.depth') c.set(compDraft.depth); });
    }
    function compEdit(name){
        compSel=name&&compLib[name]?name:null;
        compDraft=compSel?compClone(compLib[compSel]):compDefault();
        ['a','b'].forEach(sd=>{ compDraft[sd]=Object.assign({}, WIPE_DEF, compDraft[sd]||{}); });
        compOpen=true; compRender();
    }
    function compOpenFromDesk(){ sideSecSet('transitions'); if(preset.composite) compEdit(preset.composite); }
    function compShape(side,key){
        const w=compDraft[side]; if(!w) return;
        if(key in MX_LEGACY_ANGLES){ w.direction='linear'; w.angle=MX_LEGACY_ANGLES[key]; } else w.direction=key;
        $all(`[data-cshape][data-cside="${side}"]`).forEach(b=>b.setAttribute('aria-checked',shapeOn(b.dataset.cshape,w.direction,w.angle)?'true':'false'));
        compCtl.forEach(c=>{ if(c._cf===side+'.angle') c.set(w.angle); });
        compDraw(true);
    }
    function compFlag(side,f){
        const w=compDraft[side]; if(!w) return; w[f]=!w[f];
        $all(`[data-cflag="${f}"][data-cside="${side}"]`).forEach(b=>b.setAttribute('aria-pressed',w[f]?'true':'false'));
        compDraw(true);
    }
    function compOp(op){
        compDraft.op=op;
        $all('[data-cop]').forEach(b=>b.setAttribute('aria-checked',b.dataset.cop===op?'true':'false'));
        const k=$('#mx-comp-opknobs'); if(k) k.hidden=op!=='offset';
        compCurvesSync(); compDraw(true);
    }
    function compAnimSet(on){
        compAnim=!!on; compT0=0;
        const b=$('#mx-comp-anim'); if(b) b.setAttribute('aria-pressed',compAnim?'true':'false');
        compStart();
    }
    function compBody(){
        const C=compDraft, part=w=>{ const o={}; Object.keys(WIPE_DEF).forEach(f=>{ if(f!=='alternate') o[f]=(w[f]!=null)?w[f]:WIPE_DEF[f]; }); return o; };
        return {op:C.op, offset:C.offset, depth:C.depth, a:part(C.a), b:part(C.b)};
    }
    async function compPost(path,body){
        if(VMID==null) return null;
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/wipe/composite/${path}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
            let j={}; try{ j=await r.json(); }catch(_){}
            if(!r.ok) throw new Error((j&&j.error)||('HTTP '+r.status));
            clearError(); return j||{};
        }catch(e){ showError(T('plugin.mixer.comp_err','Volet composé : {msg}').replace('{msg}',e.message)); return null; }
    }
    async function compAction(act){
        if(act==='close'){ compOpen=false; compSel=null; compDraft=null; compRender(); return; }
        if(act==='save'){
            const inp=$('#mx-comp-name'), nm=((inp&&inp.value)||'').trim();
            if(!nm){ showError(T('plugin.mixer.comp_need_name','Donnez un nom au volet composé.')); return; }
            const j=await compPost('save',Object.assign({name:nm},compBody())); if(!j) return;
            compSel=j.name||nm; compLib[compSel]=j.composite||compBody();
            TOAST(T('plugin.mixer.comp_saved','« {name} » enregistré').replace('{name}',compSel));
            await compLoad(); return;
        }
        if(act==='share'){ await compShare(); return; }
        if(act==='recall'&&compSel){
            const j=await compPost('recall',{name:compSel}); if(!j) return;
            preset.mode='wipe'; preset.composite=compSel; presetApplyToUi(); compRender(); return;
        }
        if(act==='delete'&&compSel){
            const j=await compPost('delete',{name:compSel}); if(!j) return;
            if(preset.composite===compSel){ preset.composite=null; presetApplyToUi(); }
            delete compLib[compSel]; compOpen=false; compSel=null; compDraft=null; await compLoad();
        }
    }
    // Animation : une transition de 2,4 s puis une pause, en boucle, tant que la rubrique est visible.
    function compVisible(){ const h=$('#mx-comp'); return !!(h&&!h.hidden&&h.offsetParent!==null&&sideOpen&&sideSecCur()==='transitions'); }
    function compStart(){ if(!compRaf&&typeof requestAnimationFrame==='function') compRaf=requestAnimationFrame(compTick); }
    function compTick(now){
        compRaf=0;
        if(compTicking||!EL) return;
        compTicking=true;
        try{
            if(!compVisible()) return;          // rubrique cachée : on s'arrête, compRender relance
            if(compAnim){ if(!compT0) compT0=now; compT=Math.min(1,(((now-compT0)/2400)%1.35)); if(compFader) compFader.set(compT); }
            compDraw(false);
            compRaf=requestAnimationFrame(compTick);
        } finally { compTicking=false; }
    }
    let compFrame=0;
    function compDraw(force){
        compFrame++;
        if(compOpen&&compDraft){
            const r=$('[data-cpv="r"]'), m=compMix(compT,compDraft,192,108);
            compPaint(r,m);
            compPaint($('[data-cpv="a"]'), compMasks(compT,compDraft.a,128,72));
            compPaint($('[data-cpv="b"]'), compMasks(m.a2,compDraft.b,128,72));
            if(compCurves) compCurves.setPlayhead(compT);
        }
        if(force||compFrame%4===0){
            $all('[data-cname]').forEach(b=>{ const C=compLib[b.dataset.cname]; if(!C||!b.querySelector) return;
                const cv=b.querySelector('canvas'); if(cv) compPaint(cv, compMix(compT,Object.assign({},C,{a:Object.assign({},WIPE_DEF,C.a),b:Object.assign({},WIPE_DEF,C.b)}),96,54)); });
            $all('[data-clib]').forEach(b=>{ const C=libDoc(libSaves.find(x=>x.id===Number(b.dataset.clib))); if(!C||!b.querySelector) return;
                const cv=b.querySelector('canvas'); if(cv) compPaint(cv, compMix(compT,Object.assign({},C,{a:Object.assign({},WIPE_DEF,C.a),b:Object.assign({},WIPE_DEF,C.b)}),96,54)); });
        }
    }

    // ─── Panneau latéral ──────────────────────────────────
    function sideHtml(s){
        const ovlOn=s.overlay_enabled, ovlUrl=s.overlay_url||'';
        return `<div class="mx-side-head">
                <span class="mx-side-title">${esc(T('plugin.mixer.side_title','Réglages'))}</span>
                <button type="button" class="ctl-push ctl-push--small" onclick="__mx.sideClose()" aria-label="${esc(T('plugin.mixer.side_close','Fermer les réglages'))}">✕</button>
            </div>
            ${sideTabsHtml()}
            <section class="mx-side-sec" data-sec="keyers" id="mx-side-keyer"${sideSecHidden('keyers')}>${sideKeyerHtml(s)}</section>
            <section class="mx-side-sec" data-sec="outputs" id="mx-side-outputs"${sideSecHidden('outputs')}>${sideOutputsHtml(s)}</section>
            <section class="mx-side-sec" data-sec="transitions"${sideSecHidden('transitions')}>
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.sec_transition','Transition'))}</span>
                <div id="mx-wipe-soft" class="mx-wipe-controls" hidden><div class="mx-wknob" data-wf="softness"></div></div>
                ${wipeSettingsHtml()}
                <div id="mx-comp" class="mx-wipe-controls mx-comp" hidden></div>
                <p class="meta mx-wipe-hint" id="mx-wipe-hint">${esc(T('plugin.mixer.wipe_hint2','Douceur du volet : choisissez le type Wipe dans le bloc transition.'))}</p>
            </section>
            ${dskEngine?(dsks.length?`<section class="mx-side-sec" data-sec="dsk" id="mx-side-dsk"${sideSecHidden('dsk')}>${sideKeyerHtml(s,true)}</section>`:''):`<section class="mx-side-sec mx-overlay-box" data-sec="dsk"${sideSecHidden('dsk')}>
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.side_dsk','Habillage (DSK)'))}</span>
                <div class="row">
                <label for="mx-overlay-url" class="meta" style="font-size:0.85em">${esc(T('plugin.mixer.dsk_url','URL DSK :'))}</label>
                <input id="mx-overlay-url" type="text" value="${esc(ovlUrl)}" placeholder="https://…">
                <button type="button" class="btn btn-blue" onclick="__mx.applyOverlayUrl()">${esc(T('plugin.mixer.apply','Appliquer'))}</button>
            </div>${ovlUrl?`<div class="mx-dsk-preview-wrap">
                <div class="mx-dsk-preview-label"><span>${esc(T('plugin.mixer.dsk_preview','Aperçu DSK'))}</span>
                    <span class="mx-dsk-preview-status${ovlOn?' on':''}"></span></div>
                <img class="mx-dsk-preview" id="mx-dsk-preview-img" alt="${esc(T('plugin.mixer.dsk_preview','Aperçu DSK'))}"></div>`
            :`<div class="mx-dsk-preview-wrap mx-dsk-preview-empty">${esc(T('plugin.mixer.dsk_none',"Pas d'URL DSK configurée."))}</div>`}</section>`}
            <section class="mx-side-sec mx-tally-box" data-sec="tally"${sideSecHidden('tally')}>
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.sec_tally','Tally'))}</span>
                <div class="palette-field-check">
                    <label for="mx-tally-emit"><input type="checkbox" class="ios-toggle" id="mx-tally-emit"
                        onchange="__mx.tallySet('tally_emit', this.checked)">
                        ${esc(T('plugin.mixer.tally_emit',"Émettre le tally des sources à l'image (keyers compris)"))}</label>
                </div>
                <div class="palette-field-check" id="mx-tally-force-row">
                    <label for="mx-tally-force"><input type="checkbox" class="ios-toggle" id="mx-tally-force"
                        onchange="__mx.tallySet('tally_force', this.checked)">
                        ${esc(T('plugin.mixer.tally_force',"Émettre même si ma sortie n'est pas à l'antenne"))}</label>
                </div>
                <div class="mx-tally-field" id="mx-tally-map-field">
                    <span class="mx-tally-titre">${esc(T('plugin.mixer.tally_map','Couleur émise par bus'))}</span>
                    ${tallyMapHtml()}
                </div>
                <div class="mx-tally-field" id="mx-tally-lv-field">
                    <span class="mx-tally-titre">${esc(T('plugin.mixer.tally_levels','Niveaux de tally émis'))}</span>
                    <div id="mx-tally-levels"></div>
                </div>
            </section>`;
    }
    // Rubriques du panneau (demande de l'utilisateur, 0.23.17) : on RAPPELLE un type de réglage, on
    // ne fait plus défiler une pile. Les motifs de volet restent au bloc transition, pas ici.
    function sideSections(){
        const all=[['keyers',T('plugin.mixer.side_sec_keyers','Keyers')],['dsk',T('plugin.mixer.side_sec_dsk','DSK')],
            ['outputs',T('plugin.mixer.side_sec_outputs','Sorties')],['transitions',T('plugin.mixer.side_sec_transitions','Transitions')],
            ['tally',T('plugin.mixer.side_sec_tally','Tally')]];
        // Moteur à DSK (0.23.52) sans DSK déployé : pas de rubrique DSK.
        const lst=(dskEngine&&!dsks.length)?all.filter(x=>x[0]!=='dsk'):all;
        return keyers.length?lst:lst.filter(x=>x[0]!=='keyers'&&(x[0]!=='outputs'||outs));
    }
    function sideSecCur(){ const l=sideSections().map(x=>x[0]); return l.includes(sideSec)?sideSec:l[0]; }
    function sideSecHidden(sec){ return sideSecCur()===sec?'':' hidden'; }
    // Rubriques = `.ctl-segtabs` du catalogue (pastille qui glisse jusqu'à la rubrique, choisie par
    // l'utilisateur le 28/10) : `--n` rubriques, `--i` la courante.
    function sideTabsHtml(){
        const cur=sideSecCur(), secs=sideSections(), i=Math.max(0,secs.findIndex(x=>x[0]===cur));
        return `<div class="ctl-segtabs mx-side-tabs" role="tablist" style="--n:${secs.length};--i:${i}" aria-label="${esc(T('plugin.mixer.side_title','Réglages'))}"><span class="ctl-segtabs-ind" aria-hidden="true"></span>${
            secs.map(([k,l])=>`<button type="button" class="ctl-segtab${k===cur?' on':''}" role="tab" data-sec="${k}"
                aria-selected="${k===cur}" onclick="__mx.sideSecSet('${k}')">${esc(l)}</button>`).join('')}</div>`;
    }
    function sideSecSet(sec){
        sideSec=sec;
        try { localStorage.setItem('mx-side-sec', sec); } catch (_) {}
        const cur=sideSecCur();
        $all('.mx-side-tabs [data-sec]').forEach(b=>{ const a=b.dataset.sec===cur; b.classList.toggle('on',a); b.setAttribute('aria-selected',a?'true':'false'); });
        const bar=$('.mx-side-tabs'); if(bar&&bar.style&&bar.style.setProperty) bar.style.setProperty('--i', String(Math.max(0,sideSections().findIndex(x=>x[0]===cur))));
        $all('.mx-side-sec[data-sec]').forEach(x=>{ x.hidden=x.dataset.sec!==cur; });
        sideSet(true);
        if(cur==='transitions'&&VMID!=null) libLoad();      // bibliothèque commune : un autre mélangeur a pu y verser
    }
    // Sorties : MATRICE keyers × sorties (variante choisie par l'utilisateur le 28/10, « comme une
    // grille de routage »). Une COLONNE par sortie, décrite ici : ajouter une sortie (un 2e PGM, un
    // AUX…) = ajouter une entrée. `kact`/`field` = geste et champ du keyer qui basculent le point de
    // croisement ; sans `kact`, la sortie porte TOUS les keyers à l'antenne (point fixe, allumé).
    const MX_OUTPUTS=[{id:'pgm', label:'PGM'},
                      {id:'clean', label:'CLEAN', kact:'clean', field:'clean'}];
    let xpInputs=[];   // noms des entrées, pour la sous-ligne « type · source » (busRefresh les tient à jour)
    function xpSub(k){
        if(k.type==='web') return kTypeLabel(k.type)+(k.url?' · '+k.url:'');
        const t=kTypeLabel(k.type).replace(/\s*\(.*\)$/,''), src=k.input!=null?`#${k.input+1} ${xpInputs[k.input]||T('plugin.mixer.unwired','non câblé')}`:'';
        return src?`${t} · ${src}`:t;
    }
    function sideOutputsHtml(s){
        if(s&&s.inputs) xpInputs=s.inputs;
        if(outs) return outputsMatrixHtml()+outputsShapeHtml();
        if(!keyers.length) return '';
        const th=MX_OUTPUTS.map(o=>`<th scope="col">${esc(o.label)}</th>`).join('');
        const cell=(o,k,i)=>{
            if(!o.kact) return `<td><button type="button" class="ctl-push ctl-push--led ctl-push--small mx-xp" aria-pressed="true" disabled
                title="${esc(T('plugin.mixer.out_always','{o} porte toujours les keyers à l\'antenne').replace('{o}',o.label))}"><span class="ctl-led"></span></button></td>`;
            if(legacyEngine) return `<td><span class="meta" title="${esc(T('plugin.mixer.k_clean_legacy','Moteur trop ancien'))}">—</span></td>`;
            return `<td><button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small mx-xp" data-kact="${o.kact}" data-k="${i}"
                aria-pressed="${k[o.field]?'true':'false'}" aria-label="${esc(kName(i)+' → '+o.label)}"
                title="${esc(T('plugin.mixer.out_toggle','{k} sur la sortie {o}').replace('{k}',kName(i)).replace('{o}',o.label))}"><span class="ctl-led"></span></button></td>`;
        };
        // DSK (0.23.52) : au PGM quand ils sont à l'antenne, JAMAIS sur le CLEAN (prélevé avant eux).
        const dcell=(o)=>`<td><button type="button" class="ctl-push ctl-push--led ctl-push--small mx-xp" aria-pressed="${o.kact?'false':'true'}" disabled
                title="${esc(o.kact?T('plugin.mixer.out_dsk_never','Un DSK n\'est jamais sur la sortie {o}').replace('{o}',o.label)
                    :T('plugin.mixer.out_always','{o} porte toujours les keyers à l\'antenne').replace('{o}',o.label))}"><span class="ctl-led"></span></button></td>`;
        const rows=keyers.map((k,i)=>`<tr><th scope="row"><span class="mx-xp-k">${esc(kName(i))}</span><span class="mx-xp-sub" data-xp-sub="${i}">${esc(xpSub(k))}</span></th>${
            MX_OUTPUTS.map(o=>cell(o,k,i)).join('')}</tr>`).join('')
            + dsks.map((k,j)=>{ const i=keyers.length+j; return `<tr><th scope="row"><span class="mx-xp-k">${esc(kName(i))}</span><span class="mx-xp-sub" data-xp-sub="${i}">${esc(xpSub(k))}</span></th>${
                MX_OUTPUTS.map(dcell).join('')}</tr>`; }).join('');
        return `<span class="mx-preset-section-label">${esc(T('plugin.mixer.out_title','Sorties des keyers'))}</span>
            <p class="meta">${esc(T('plugin.mixer.out_help','Le PGM porte tous les keyers à l\'antenne ; la sortie CLEAN, seulement ceux allumés ici.'))}</p>
            <table class="mx-xp-table"><thead><tr><th scope="col">${esc(T('plugin.mixer.side_keyer','Keyer'))}</th>${th}</tr></thead><tbody>${rows}</tbody></table>`;
    }
    // ─── Sorties programme (0.23.53) ──────────────────────────────────────────────────────────
    // Moteur à sorties (`s.outputs`) : MATRICE couches × sorties (une colonne par sortie, son nom),
    // une ligne « Tally » (la sortie compte-t-elle pour le tally), la base du PVW — tout à chaud par
    // POST /outputs (persisté). Puis la FORME (ajouter, renommer, retirer une sortie ; PVW oui/non) :
    // un brouillon, appliqué par redéploiement (↻) après une confirmation qui liste les abonnés qui
    // seront débranchés (GET /api/containers/<vmid>/flow_consumers). Moteur plus ancien : la matrice
    // d'avant (PGM fixe, CLEAN par le booléen des keyers).
    let outs=null, pvwBase=null, pvwShm=null, outMax=4, outNameMax=24, lastOutputs='', outDraft=null;
    function outputsFromState(s){
        outs=Array.isArray(s.outputs)?s.outputs.map(o=>({id:o.id, name:o.name, shm:o.shm, tally:!!o.tally, layers:(o.layers||[]).slice()})):null;
        pvwBase=(s.pvw_base===undefined)?0:s.pvw_base; pvwShm=s.pvw_shm||null;
        outMax=(s.caps&&s.caps.max_outputs)||4; outNameMax=(s.caps&&s.caps.output_name_max)||24;
        lastOutputs=JSON.stringify([s.outputs||null, s.pvw_base===undefined?0:s.pvw_base]);
    }
    function layerKey(i){ return i<keyers.length?`k${i+1}`:`d${i-keyers.length+1}`; }
    function outCellHtml(o,i){
        const key=layerKey(i), on=o.layers.includes(key);
        return `<td><button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small mx-xp" data-out="${o.id}" data-layer="${key}"
            aria-pressed="${on?'true':'false'}" aria-label="${esc(kName(i)+' → '+o.name)}" onclick="__mx.outLayer(${o.id},'${key}')"
            title="${esc(T('plugin.mixer.out_toggle','{k} sur la sortie {o}').replace('{k}',kName(i)).replace('{o}',o.name))}"><span class="ctl-led"></span></button></td>`;
    }
    function outRowHtml(i,k){
        return `<tr><th scope="row"><span class="mx-xp-k">${esc(kName(i))}</span><span class="mx-xp-sub" data-xp-sub="${i}">${esc(xpSub(k))}</span></th>${
            outs.map(o=>outCellHtml(o,i)).join('')}</tr>`;
    }
    function outputsMatrixHtml(){
        const th=outs.map(o=>`<th scope="col" title="${esc(o.shm)}">${esc(o.name)}</th>`).join('');
        const rows=keyers.map((k,i)=>outRowHtml(i,k)).join('')+dsks.map((k,j)=>outRowHtml(keyers.length+j,k)).join('');
        const tally=`<tr class="mx-out-tally"><th scope="row"><span class="mx-xp-k">${esc(T('plugin.mixer.out_tally','Tally'))}</span><span class="mx-xp-sub">${esc(T('plugin.mixer.out_tally_sub','compte pour le tally'))}</span></th>${
            outs.map(o=>`<td><button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small mx-xp" style="--ctl-tone:var(--status-stopped-fg)"
                data-out-tally="${o.id}" aria-pressed="${o.tally?'true':'false'}" onclick="__mx.outTally(${o.id})"
                aria-label="${esc(T('plugin.mixer.out_tally_of','{o} compte pour le tally').replace('{o}',o.name))}"><span class="ctl-led"></span></button></td>`).join('')}</tr>`;
        const pvw=pvwBase==null
            ? `<p class="meta">${esc(T('plugin.mixer.out_pvw_none','Pas de sortie PVW.'))}</p>`
            : `<label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.out_pvw_base','PVW basé sur'))}</span>
                <select class="ctl-select" id="mx-out-pvw" onchange="__mx.outPvwBase(this.value)" aria-label="${esc(T('plugin.mixer.out_pvw_base','PVW basé sur'))}">${
                outs.map(o=>`<option value="${o.id}"${o.id===pvwBase?' selected':''}>${esc(o.name)}</option>`).join('')}</select></label>`;
        return `<span class="mx-preset-section-label">${esc(T('plugin.mixer.out_title','Sorties des keyers'))}</span>
            <p class="meta">${esc(T('plugin.mixer.out_help_multi','Fond, transition et état des couches sont communs ; chaque sortie choisit ses couches.'))}</p>
            <table class="mx-xp-table"><thead><tr><th scope="col">${esc(T('plugin.mixer.out_layer','Couche'))}</th>${th}</tr></thead><tbody>${rows}${tally}</tbody></table>
            ${pvw}`;
    }
    // FORME (↻) : brouillon local, appliqué par redéploiement.
    function outDraftReset(){ outDraft={list:outs.map(o=>({id:o.id, name:o.name, tally:o.tally, layers:o.layers.slice()})), pvw:pvwBase!=null, pvwBase:pvwBase}; }
    function outputsShapeHtml(){
        if(!outDraft) outDraftReset();
        const rows=outDraft.list.map((o,n)=>`<div class="mx-out-row">
                <input type="text" maxlength="${outNameMax}" value="${esc(o.name)}" data-out-name="${n}" oninput="__mx.outName(${n},this.value)"
                    aria-label="${esc(T('plugin.mixer.out_name','Nom de la sortie'))}">
                <span class="meta mx-out-id">${o.id==null?esc(T('plugin.mixer.out_new','nouvelle')):esc(T('plugin.mixer.out_slot','slot {n}').replace('{n}',o.id))}</span>
                ${o.id===0?`<span class="meta">${esc(T('plugin.mixer.out_pgm_always','toujours là'))}</span>`
                    :`<button type="button" class="ctl-push ctl-push--small" onclick="__mx.outRemove(${n})">${esc(T('plugin.mixer.out_remove','Retirer'))}</button>`}
            </div>`).join('');
        const full=outDraft.list.length>=outMax;
        return `<span class="mx-preset-section-label">${esc(T('plugin.mixer.out_shape','Sorties (↻ redéploiement)'))}</span>
            <p class="meta">${esc(T('plugin.mixer.out_shape_help','Ajouter, renommer ou retirer une sortie, ou le PVW, redémarre le mélangeur : l\'antenne est coupée quelques secondes. Le nom donne le flux : <machine>_<nom>.'))}</p>
            <div class="mx-out-rows">${rows}</div>
            <div class="mx-out-actions">
                <button type="button" class="ctl-push ctl-push--small"${full?' disabled':''} onclick="__mx.outAdd()">${esc(T('plugin.mixer.out_add','Ajouter une sortie'))}</button>
                <button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" aria-pressed="${outDraft.pvw?'true':'false'}" onclick="__mx.outPvwToggle()"><span class="ctl-led"></span>${esc(T('plugin.mixer.out_pvw','Sortie PVW'))}</button>
                <button type="button" class="ctl-push ctl-push--small" onclick="__mx.outApply()">${esc(T('plugin.mixer.out_apply','Appliquer ↻'))}</button>
                <button type="button" class="ctl-push ctl-push--small" onclick="__mx.outCancel()">${esc(T('plugin.mixer.out_cancel','Annuler'))}</button>
            </div>`;
    }
    function outputsRender(){
        const el=$('#mx-side-outputs'); if(!el) return;
        el.innerHTML=sideOutputsHtml(null);
    }
    async function outPost(body){
        if(VMID==null) return false;
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/outputs`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
            const d=await r.json().catch(()=>({}));
            if(!r.ok) throw new Error(d.error||('HTTP '+r.status));
            if(Array.isArray(d.outputs)){ outs=d.outputs; if('pvw_base' in d) pvwBase=d.pvw_base; outputsRender(); }
            clearError(); return true;
        }catch(e){ showError(T('plugin.mixer.out_err','Sorties : ')+e.message); outputsRender(); return false; }
    }
    function outLayer(id,key){
        const o=(outs||[]).find(x=>x.id===id); if(!o) return;
        const on=!o.layers.includes(key);
        o.layers=on?o.layers.concat([key]):o.layers.filter(x=>x!==key);
        outputsRender();
        outPost({output:id, layer:key, on});
    }
    function outTally(id){
        const o=(outs||[]).find(x=>x.id===id); if(!o) return;
        o.tally=!o.tally; outputsRender(); outPost({output:id, tally:o.tally});
    }
    function outPvwBase(v){ const id=parseInt(v,10); if(!Number.isInteger(id)) return; pvwBase=id; outPost({pvw_base:id}); }
    function outName(n,v){ if(outDraft&&outDraft.list[n]) outDraft.list[n].name=v; }
    function outAdd(){
        if(!outDraft) outDraftReset();
        if(outDraft.list.length>=outMax) return;
        // Nouvelle sortie : aucune couche (une couche s'ajoute à la seule sortie de base du PVW).
        // Nom par défaut : le premier « AUX n » libre (un nom en double donnerait le même flux).
        const nm=n=>T('plugin.mixer.out_new_name','AUX {n}').replace('{n}',n), used=outDraft.list.map(o=>o.name.trim());
        let n=1; while(used.includes(nm(n))) n++;
        outDraft.list.push({id:null, name:nm(n), tally:false, layers:[]});
        outputsRender();
    }
    function outRemove(n){ if(outDraft&&outDraft.list[n]&&outDraft.list[n].id!==0){ outDraft.list.splice(n,1); outputsRender(); } }
    function outPvwToggle(){ if(!outDraft) outDraftReset(); outDraft.pvw=!outDraft.pvw; outputsRender(); }
    function outCancel(){ outDraft=null; outputsRender(); }
    // Suffixe du flux d'une sortie : MÊME règle que hooks.py:output_slug (le serveur fait foi ; ceci ne
    // sert qu'à ne pas annoncer comme perdu le flux d'une sortie dont seul le libellé change, 0.23.55).
    function outSlug(name,id){
        let s=String(name||'').normalize('NFKD').replace(/[^\x00-\x7f]/g,'').toLowerCase().replace(/[^a-z0-9_]/g,'_');
        while(s.includes('__')) s=s.replace(/__/g,'_');
        s=s.replace(/^_+|_+$/g,'').slice(0,outNameMax).replace(/^_+|_+$/g,'');
        return s||('out'+id);
    }
    // Flux qui disparaissent avec ce brouillon : sorties retirées ou renommées (autre flux), PVW retiré.
    function outLost(){
        const keep=new Map(outDraft.list.filter(o=>o.id!=null).map(o=>[o.id,o.name.trim()]));
        const lost=outs.filter(o=>!keep.has(o.id)||(keep.get(o.id)!==o.name&&outSlug(keep.get(o.id),o.id)!==outSlug(o.name,o.id))).map(o=>o.shm);
        if(pvwBase!=null&&!outDraft.pvw&&pvwShm) lost.push(pvwShm);
        return lost;
    }
    async function outApply(){
        if(VMID==null||!outDraft) return;
        const list=outDraft.list.map(o=>Object.assign({name:o.name.trim(), tally:o.tally, layers:o.layers}, o.id!=null?{id:o.id}:{}));
        if(list.some(o=>!o.name)){ showError(T('plugin.mixer.out_name_required','Chaque sortie doit avoir un nom.')); return; }
        let pb=null;
        if(outDraft.pvw) pb=(pvwBase!=null&&list.some(o=>o.id===pvwBase))?pvwBase:0;
        // 0.23.55 : le serveur d'abord, SANS confirmation — forme inchangée (libellé seul, couches,
        // tally) → appliqué à chaud, rien n'est coupé ; sinon il répond confirm_required et l'on
        // demande la confirmation ↻ comme avant.
        try{
            const r0=await fetch(`/api/containers/${VMID}/control/outputs`,{method:'POST',headers:{'Content-Type':'application/json'},
                body:JSON.stringify({outputs:list, pvw_base:pb})});
            const d0=await r0.json().catch(()=>({}));
            if(r0.ok){
                outDraft=null; clearError();
                TOAST(T('plugin.mixer.out_applied_hot','Sorties mises à jour, sans coupure.'),'info');
                fetchState({fullRebuild:false}); return;
            }
            if(d0.error!=='confirm_required') throw new Error(d0.error||('HTTP '+r0.status));
        }catch(e){ showError(T('plugin.mixer.out_err','Sorties : ')+e.message); return; }
        const lost=outLost();
        let who=[];
        try{
            const r=await fetch(`/api/containers/${VMID}/flow_consumers`,{cache:'no-store'});
            const d=await r.json();
            const by=new Map((d.outputs||[]).map(o=>[o.shm,o.consumers||[]]));
            lost.forEach(shm=>(by.get(shm)||[]).forEach(c=>who.push(`${shm} → #${c.vmid} ${c.hostname}`)));
        }catch(e){ showError(T('plugin.mixer.out_consumers_err','Abonnés introuvables : ')+e.message); return; }
        const msg=T('plugin.mixer.out_confirm_msg','Le mélangeur va redémarrer : l\'antenne sera coupée quelques secondes.')
            +(lost.length?'\n\n'+T('plugin.mixer.out_confirm_lost','Flux qui disparaissent : {l}').replace('{l}',lost.join(', ')):'')
            +(who.length?'\n\n'+T('plugin.mixer.out_confirm_who','Seront débranchés : {w}').replace('{w}',who.join(' ; '))
                        :(lost.length?'\n\n'+T('plugin.mixer.out_confirm_nobody','Personne ne les lit.'):''));
        const ok=window.mxlConfirm?await window.mxlConfirm({title:T('plugin.mixer.out_confirm_title','Changer les sorties'), message:msg,
                confirmLabel:T('plugin.mixer.out_apply','Appliquer ↻'), cancelLabel:T('plugin.mixer.out_cancel','Annuler')})
            :window.confirm(msg);
        if(!ok) return;
        try{
            const r=await fetch(`/api/containers/${VMID}/control/outputs`,{method:'POST',headers:{'Content-Type':'application/json'},
                body:JSON.stringify({outputs:list, pvw_base:pb, confirm:true})});
            const d=await r.json().catch(()=>({}));
            if(!r.ok) throw new Error(d.error||('HTTP '+r.status));
            outDraft=null; clearError();
            TOAST(T('plugin.mixer.out_applied','Sorties enregistrées — redémarrage du mélangeur.'),'info');
        }catch(e){ showError(T('plugin.mixer.out_err','Sorties : ')+e.message); }
    }
    // Éditeur d'une couche du panneau : `dsk` faux = les keyers (sélecteur parmi eux), vrai = les DSK
    // (0.23.52, rubrique DSK). Mêmes réglages ; un DSK n'a ni CLEAN ni sauvegardes de keyer.
    function sideKeyerHtml(s,dsk){
        const list=dsk?dsks:keyers, off=dsk?keyers.length:0;
        if(!list.length) return '';
        const sel=dsk?sideDsk:sideKeyer;
        const i=off+Math.min(sel,list.length-1), k=layers[i]||list[0];
        const pick=list.length>1
            ? `<select class="ctl-select" id="${dsk?'mx-side-dpick':'mx-side-kpick'}" aria-label="${esc(dsk?T('plugin.mixer.side_dsk_pick','DSK'):T('plugin.mixer.side_keyer','Keyer'))}">${
                list.map((_,j)=>`<option value="${j}"${off+j===i?' selected':''}>${esc(kName(off+j))}</option>`).join('')}</select>`
            : `<span class="mx-k-name">${esc(kName(off))}</span>`;
        const legacy=legacyEngine;
        // Copier/coller et sauvegardes demandent un moteur 0.23.16 (/keyer/settings, /keyer/preset/*),
        // reconnaissable à `keyer_presets` dans /state ; un moteur plus ancien les MASQUE et le dit.
        // DSK : copier/coller oui (/dsk/<n>/settings), sauvegardes non (elles sont celles des keyers).
        const modern=!legacy&&Array.isArray(s&&s.keyer_presets);
        const withPresets=modern&&!dsk;
        const presets=modern?s.keyer_presets:[];
        const caps=(s&&s.caps)||{};
        const clipOk=!!clipRead();
        const tools=!modern?'':`<div class="mx-k-row mx-k-tools">
                <button type="button" class="ctl-push ctl-push--small" data-kact="copy" data-k="${i}">${esc(T('plugin.mixer.k_copy','Copier les réglages'))}</button>
                <button type="button" class="ctl-push ctl-push--small" data-kact="paste" data-k="${i}"${clipOk?'':' disabled'}
                    title="${esc(T('plugin.mixer.k_paste_tip','Coller les réglages copiés (type, entrées, position, masque, découpe, durée, clean — jamais l\'antenne)'))}">${esc(T('plugin.mixer.k_paste','Coller'))}</button>
            </div>`;
        const presetsHtml=!withPresets?'':`<div class="mx-k-presets">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.k_presets','Sauvegardes de ce mélangeur'))}</span>
                ${presets.length?`<ul class="mx-k-preset-list">${presets.map(nm=>`<li class="mx-k-preset">
                    <span class="mx-k-preset-name">${esc(nm)}</span>
                    <button type="button" class="ctl-push ctl-push--small" data-kact="recall" data-k="${i}" data-name="${esc(nm)}">${esc(T('plugin.mixer.k_preset_recall','Rappeler'))}</button>
                    <button type="button" class="ctl-push ctl-push--small ctl-push--danger" data-kact="pdelete" data-k="${i}" data-name="${esc(nm)}"
                        aria-label="${esc(T('plugin.mixer.k_preset_delete','Supprimer')+' '+nm)}">${esc(T('plugin.mixer.k_preset_delete','Supprimer'))}</button></li>`).join('')}</ul>`
                    :`<p class="meta">${esc(T('plugin.mixer.k_preset_none','Aucune sauvegarde.'))}</p>`}
                <div class="mx-k-row">
                    <input type="text" id="mx-k-preset-name" maxlength="${caps.keyer_preset_name_max||32}" placeholder="${esc(T('plugin.mixer.k_preset_name','Nom de la sauvegarde'))}"
                        aria-label="${esc(T('plugin.mixer.k_preset_name','Nom de la sauvegarde'))}">
                    <button type="button" class="ctl-push ctl-push--small" data-kact="psave" data-k="${i}">${esc(T('plugin.mixer.k_preset_save','Sauvegarder'))}</button>
                </div></div>`;
        const dskNote=dsk?`<p class="meta">${esc(T('plugin.mixer.dsk_help','Composé après la transition, au-dessus des keyers, jamais sur la sortie CLEAN. TIE : il part avec la prochaine transition du fond.'))}</p>`:'';
        return `<div class="mx-side-khead"><span class="mx-preset-section-label">${esc(dsk?T('plugin.mixer.side_dsk_pick','DSK'):T('plugin.mixer.side_keyer','Keyer'))}</span>${pick}</div>
            ${legacy?`<p class="meta">${esc(T('plugin.mixer.k_legacy','Le moteur déployé ne connaît qu\'un keyer, sans découpe : redéployez le conteneur pour les keyers linear et luma.'))}</p>`:''}
            ${dskNote}
            ${dveSurfaceHtml()}
            ${keyerDetailHtml(s,k,i,legacy)}
            ${dsk||k.type==='web'?'':rowPrefsHtml(k,i,legacy,'side')}
            ${tools}${presetsHtml}
            ${(!legacy&&!modern)?`<p class="meta">${esc(T('plugin.mixer.k_presets_stale','Copier/coller et sauvegardes de keyer : redéployez le conteneur.'))}</p>`:''}`;
    }
    function renderSideKeyer(){
        if(!state) return;
        const host=$('#mx-side-keyer');
        if(host){ host.innerHTML=sideKeyerHtml(state); bindKeyers(host); }
        const hd=dskEngine?$('#mx-side-dsk'):null;
        if(hd){ hd.innerHTML=sideKeyerHtml(state,true); bindKeyers(hd); }
        patternKnobsMount(EL);
        keyerKnobsMount(EL);
        dveMount();
        dskPreviewStart();
    }
    // Rotatifs du keyer MOTIF (0.23.47) : catalogue (BobiControls), plus les gestes propres au
    // mélangeur (kKnob). Un geste envoie au plus toutes les 100 ms, et à la fin.
    const PATTERN_KNOBS={
        pattern_size:{label:['plugin.mixer.k_pattern_size','Taille'], min:0, max:1, step:0.01, def:0.5, fmt:'pct'},
        pattern_x:{label:['plugin.mixer.k_pattern_x','X'], min:0, max:1, step:0.01, def:0.5, fmt:'pct'},
        pattern_y:{label:['plugin.mixer.k_pattern_y','Y'], min:0, max:1, step:0.01, def:0.5, fmt:'pct'},
        pattern_soft:{label:['plugin.mixer.k_pattern_soft','Douceur'], min:0, max:1, step:0.01, def:0.10, fmt:'pct'},
        pattern_angle:{label:['plugin.mixer.wipe_angle','Angle'], min:0, max:360, step:1, def:0, fmt:'deg'},
        pattern_repeat:{label:['plugin.mixer.wipe_repeat','Répétition'], min:1, max:16, step:1, def:1, fmt:'int'},
        pattern_border:{label:['plugin.mixer.wipe_border_width','Largeur'], min:0, max:0.5, step:0.005, def:0, fmt:'pct'},
        pattern_border_soft_in:{label:['plugin.mixer.wipe_border_soft_in','Douceur interne'], min:0, max:1, step:0.01, def:0, fmt:'pct',
            tip:['plugin.mixer.k_pattern_bsi_tip','Bord extérieur de la bordure, côté fond']},
        pattern_border_soft_out:{label:['plugin.mixer.wipe_border_soft_out','Douceur externe'], min:0, max:1, step:0.01, def:0, fmt:'pct',
            tip:['plugin.mixer.k_pattern_bso_tip','Bord du fill ; remplace la douceur du motif quand il y a une bordure']}};
    let patternKnobs=[], patternPost={};
    function patternKnobsMount(root){
        patternKnobs.forEach(c=>{ try{ c.destroy(); }catch(_){} }); patternKnobs=[];
        const r=root||EL; if(!r||!window.BobiControls) return;
        r.querySelectorAll('.mx-pknob[data-pf]').forEach(el=>{
            const f=el.dataset.pf, d=PATTERN_KNOBS[f], i=parseInt(el.dataset.k||'0',10), k=layers[i]; if(!d||!k) return;
            if(d.tip&&el.setAttribute) el.setAttribute('title',T(...d.tip));
            const c=window.BobiControls.create('knob', el, {label:T(...d.label), min:d.min, max:d.max, step:d.step, default:d.def,
                value:(k[f]!=null?k[f]:d.def), format:wipeFmt(d.fmt), integer:d.fmt==='int'});
            c.on('input', v=>patternFieldSet(i,f,v,false));
            c.on('change', v=>patternFieldSet(i,f,v,true));
            patternKnobs.push(c);
        });
    }
    function patternFieldSet(i,f,v,final){
        const k=layers[i]; if(!k) return;
        k[f]=v; const key=i+':'+f;
        const send=()=>{ patternPost[key]=null; const b={keyer:i+1}; b[f]=k[f]; keyerPost('pattern',b); };
        if(final){ if(patternPost[key]){ clearTimeout(patternPost[key]); } send(); return; }
        if(!patternPost[key]) patternPost[key]=setTimeout(send,100);
    }
    function sideSet(open){
        sideOpen=!!open;
        try { localStorage.setItem('mx-side-open', sideOpen?'1':'0'); } catch (_) {}
        const o=$('#mx-view-ops'), a=$('#mx-side'), b=$('#mx-side-toggle');
        if(o) o.classList.toggle('side-open', sideOpen);
        if(a) a.hidden=!sideOpen;
        if(b) b.setAttribute('aria-expanded', sideOpen?'true':'false');
    }
    function sideToggle(){ sideSet(!sideOpen); }
    function sideClose(){ sideSet(false); }
    function sideEditKeyer(i){
        i=i|0;
        if(dskEngine&&i>=keyers.length){       // une couche DSK : sa rubrique (0.23.52)
            sideDsk=Math.max(0,Math.min(dsks.length-1,i-keyers.length));
            renderSideKeyer();
            sideSecSet('dsk');
            return;
        }
        sideKeyer=Math.max(0,Math.min(keyers.length-1,i));
        try { localStorage.setItem('mx-side-k', String(sideKeyer)); } catch (_) {}
        renderSideKeyer();
        sideSecSet('keyers');
    }
    // Fermer d'un GESTE : glisser vers la droite (au doigt comme à la souris), Échap. Les rotatifs et
    // les champs gardent leurs propres gestes : un glissement qui part d'eux n'est pas compté.
    function sideBindGestures(){
        const a=$('#mx-side'); if(!a||!a.addEventListener) return;
        let sw=null;
        a.addEventListener('pointerdown',e=>{
            const t=e.target;
            if(t&&t.closest&&t.closest('.ctl-knob-hit,input,select,textarea,.mx-dl-table')) return;
            sw={x:e.clientX,y:e.clientY};
        });
        a.addEventListener('pointerup',e=>{
            if(!sw) return; const dx=e.clientX-sw.x, dy=Math.abs(e.clientY-sw.y); sw=null;
            if(dx>70 && dy<dx) sideClose();
        });
        a.addEventListener('pointercancel',()=>{ sw=null; });
        a.addEventListener('keydown',e=>{ if(e.key==='Escape'){ sideClose(); const b=$('#mx-side-toggle'); if(b&&b.focus) b.focus(); } });
    }

    // ─── Keyers amont (0.23.14) ───────────────────────────
    // Au pupitre : une LIGNE COMPACTE par keyer (nom, type en lecture, ON AIR, AUTO, CLEAN, rangées
    // de sources affichées, « Régler »). Dans le panneau : le détail du keyer choisi (type, entrées,
    // fly, masque, découpe, durée de l'AUTO). Contrôles du catalogue partagé (ctl-select, ctl-push,
    // ctl-knob, ctl-readout, ctl-bus--panel).
    const KEYER_TYPES=[
        {v:'dve',    k:'plugin.mixer.ktype_dve',    fb:'DVE (image placée)'},
        {v:'linear', k:'plugin.mixer.ktype_linear', fb:'Linear (fill + key)'},
        {v:'luma',   k:'plugin.mixer.ktype_luma',   fb:'Luma'},
        {v:'chroma', k:'plugin.mixer.ktype_chroma', fb:'Chroma'},
        {v:'pattern',k:'plugin.mixer.ktype_pattern',fb:'Motif'},
        {v:'web',    k:'plugin.mixer.ktype_web',    fb:'Page web'},
    ];
    // Moteur sans caps.keyer_types (avant 0.23.14 de fait) : les trois types d'origine.
    function keyerTypesAvail(){ const ok=engineKeyerTypes||['dve','linear','luma']; return KEYER_TYPES.filter(t=>ok.includes(t.v)); }
    // Couleurs de clé proposées d'un appui (la couleur exacte se règle au sélecteur).
    const CHROMA_SWATCHES=[{v:'#00b140', k:'plugin.mixer.k_swatch_green', fb:'Vert'}, {v:'#0047bb', k:'plugin.mixer.k_swatch_blue', fb:'Bleu'}];
    const KEYER_GROUP_FIELDS={transform:['x','y','scale','border_width'], mask:['left','right','top','bottom'],
                              key:['clip','gain'], rate:['auto_ms','auto_soft'],
                              chroma:['chroma_tol','chroma_soft','chroma_spill'],
                              pattern:['pattern_size','pattern_x','pattern_y','pattern_soft','pattern_angle','pattern_repeat',
                                       'pattern_border','pattern_border_soft_in','pattern_border_soft_out']};
    // Ce qu'on COPIE d'un keyer : ses réglages, jamais son état d'antenne (active, moving, auto).
    const KEYER_COPY_FIELDS=['type','input','key_input','x','y','scale','border_width','clip','gain','invert','auto_ms','clean',
                             'auto_mode','auto_dir','auto_rev','auto_soft','auto_alt',
                             'chroma_color','chroma_tol','chroma_soft','chroma_spill',
                             'pattern','pattern_size','pattern_x','pattern_y','pattern_soft',
                             'pattern_angle','pattern_repeat','pattern_border','pattern_border_color','pattern_border_soft_in','pattern_border_soft_out',
                             'url'];
    // Copier/coller d'un keyer : presse-papiers de la BASE COMMUNE des éditeurs (static/js/editor.js,
    // BobiEditor.clipboard, 0.23.59 — décision du 30/10 : aucune fonction d'édition privée), avec le
    // presse-papiers système pour coller dans un autre poste.
    const CLIP_KIND='bobi.mixer.keyer';
    function defaultKeyer(){
        return {type:'dve', input:0, key_input:0, active:false, moving:false, auto:false, x:0.5, y:0.5, scale:0.30,
                border_width:0, mask:{left:0,right:0,top:0,bottom:0}, clip:0, gain:1, invert:false, auto_ms:500, clean:false,
                auto_mode:'mix', auto_dir:'horizontal', auto_rev:false, auto_soft:0.10, auto_alt:false,
                chroma_color:'#00b140', chroma_tol:0.20, chroma_soft:0.15, chroma_spill:0.50,
                pattern:'iris', pattern_size:0.50, pattern_x:0.5, pattern_y:0.5, pattern_soft:0.10,
                pattern_angle:0, pattern_repeat:1, pattern_border:0, pattern_border_color:'#ffffff', pattern_border_soft_in:0, pattern_border_soft_out:0};
    }
    function keyersFromState(s){
        // Moteur d'avant 0.23.14 : pas de `keyers`, le seul `keyer`.
        const src=(Array.isArray(s.keyers)&&s.keyers.length) ? s.keyers : (s.keyer ? [s.keyer] : []);
        return src.map(layerOf);
    }
    // Une couche (keyer ou DSK) telle que /state la publie → l'objet de l'interface.
    function layerOf(k){
        {
            const d=defaultKeyer(), m=k.mask||{};
            return {type:k.type||'dve', input:k.input??0, key_input:k.key_input??(k.input??0), active:!!k.active,
                    moving:!!k.moving, auto:!!k.auto, x:k.x??d.x, y:k.y??d.y, scale:k.scale??d.scale,
                    border_width:k.border_width??0, mask:{left:m.left||0,right:m.right||0,top:m.top||0,bottom:m.bottom||0},
                    clip:k.clip??0, gain:k.gain??1, invert:!!k.invert, auto_ms:k.auto_ms??d.auto_ms, clean:!!k.clean,
                    auto_mode:k.auto_mode||d.auto_mode, auto_dir:k.auto_dir||d.auto_dir, auto_rev:!!k.auto_rev, auto_alt:!!k.auto_alt,
                    auto_soft:k.auto_soft??d.auto_soft,
                    chroma_color:k.chroma_color||d.chroma_color, chroma_tol:k.chroma_tol??d.chroma_tol,
                    chroma_soft:k.chroma_soft??d.chroma_soft, chroma_spill:k.chroma_spill??d.chroma_spill,
                    pattern:k.pattern||d.pattern, pattern_size:k.pattern_size??d.pattern_size,
                    pattern_x:k.pattern_x??d.pattern_x, pattern_y:k.pattern_y??d.pattern_y,
                    pattern_soft:k.pattern_soft??d.pattern_soft,
                    pattern_angle:k.pattern_angle??d.pattern_angle, pattern_repeat:k.pattern_repeat??d.pattern_repeat,
                    pattern_border:k.pattern_border??d.pattern_border, pattern_border_color:k.pattern_border_color||d.pattern_border_color,
                    pattern_border_soft_in:k.pattern_border_soft_in??d.pattern_border_soft_in,
                    pattern_border_soft_out:k.pattern_border_soft_out??d.pattern_border_soft_out,
                    // couche web (0.23.52) : page, état de son onglet ; DSK : lien (tie)
                    url:k.url||'', web:k.web||null, tie:!!k.tie};
        }
    }
    function targetsFromState(nt){
        const ks=Array.isArray(nt.keyers) ? nt.keyers.map(Boolean) : [!!nt.keyer];
        while(ks.length<keyers.length) ks.push(false);
        return {bgd:nt.bgd!==false, keyers:ks};
    }
    function kName(i){
        if(i>=keyers.length) return T('plugin.mixer.dsk_name','DSK {n}').replace('{n}',i-keyers.length+1);
        return T('plugin.mixer.k_name','Key {n}').replace('{n}',i+1);
    }
    function isDsk(i){ return i>=keyers.length; }
    // Route d'un réglage de COUCHE : /keyer/<ep> {keyer: n} pour un keyer ; /dsk/<n>/<ep> pour un DSK
    // (0.23.52 — « active » y est « cut »).
    function layerRoute(ep,body){
        const n=body&&body.keyer;
        if(!n||n<=keyers.length) return [`keyer/${ep}`, body];
        const b=Object.assign({},body); delete b.keyer;
        return [`dsk/${n-keyers.length}/${ep==='active'?'cut':ep}`, b];
    }
    // Refus du moteur porteur d'un CODE (0.23.52 : limite de couches web), traduit ici.
    function engineErr(j,status){
        if(j&&j.code==='web_limit') return T('plugin.mixer.err_web_limit','Couches web : {max} au plus sur ce mélangeur — retirez la page d\'une autre couche d\'abord.')
            .replace('{max}', (j.params&&j.params.max)!=null?j.params.max:'?');
        if(j&&j.code==='no_dsk') return T('plugin.mixer.err_no_dsk','Ce mélangeur n\'a pas de DSK.');
        return (j&&j.error)||('HTTP '+status);
    }
    function kTypeLabel(t){ const d=KEYER_TYPES.find(x=>x.v===t); return d?T(d.k,d.fb):t; }
    function inputOpts(s,sel){
        const n=s.n_inputs||(s.inputs?s.inputs.length:0), inputs=s.inputs||[];
        return Array.from({length:n},(_,i)=>`<option value="${i}"${sel===i?' selected':''}>#${i+1} — ${esc(inputs[i]||T('plugin.mixer.unwired','non câblé'))}</option>`).join('');
    }
    // Durée en ms ↔ « s:ii » (secondes:images) à la cadence du mélangeur. Saisie : « 1:12 », ou un
    // nombre seul = des IMAGES (convention des pupitres). Sans cadence connue : des ms.
    function fmtDur(ms){
        ms=Math.max(0,Number(ms)||0);
        if(!frameRate) return Math.round(ms)+' ms';
        const n=Math.round(ms*frameRate/1000), sec=Math.floor(n/frameRate), im=n%frameRate;
        return sec+':'+String(im).padStart(2,'0');
    }
    function parseDur(str){
        const t=String(str==null?'':str).trim().replace(/\s*ms$/i,'');
        if(!frameRate) return Math.max(0,parseInt(t,10)||0);
        let n;
        if(t.includes(':')){ const [a,b]=t.split(':'); n=(parseInt(a,10)||0)*frameRate+(parseInt(b,10)||0); }
        else n=parseInt(t,10)||0;
        return Math.max(0,Math.min(10000,Math.round(n*1000/frameRate)));
    }
    function autoModeLabel(m){ return m==='wipe'?T('plugin.mixer.mode_wipe','Wipe'):T('plugin.mixer.mode_dissolve','Mix'); }
    // Rotatif de keyer (0.23.65) : un emplacement, monté par keyerKnobsMount avec le rotatif du
    // CATALOGUE (BobiControls) — glisser, Maj = fin, molette, flèches, double-clic / ↺ = défaut, au
    // doigt comme à la souris. Le geste privé d'avant (pointeur, molette, clavier écrits ici) est retiré.
    function kKnob(i,group,field,label,min,max,step,value,def){
        return `<div class="mx-kknob" data-knob="${field}" data-k="${i||0}" data-group="${group||'transform'}" data-min="${min}" data-max="${max}"
            data-step="${step}" data-default="${def}" data-label="${esc(label)}"></div>`;
    }

    // Rangées de sources par keyer : au choix PAR POSTE (et par mélangeur), masquées par défaut.
    function rowPrefsKey(){ return 'mx-krows:'+(SCOPE||('vmid'+VMID)); }
    function rowPrefsLoad(){
        try { const v=JSON.parse(localStorage.getItem(rowPrefsKey())||'{}'); return (v&&typeof v==='object')?v:{}; }
        catch (_) { return {}; }
    }
    function rowPref(i){ const p=rowPrefs[String(i)]||{}; return {fill:!!p.fill, key:!!p.key}; }
    function rowPrefSet(i,which,on){
        const p=rowPref(i); p[which]=!!on; rowPrefs[String(i)]=p;
        try { localStorage.setItem(rowPrefsKey(), JSON.stringify(rowPrefs)); } catch (_) {}
        if(state) renderEditor(state);
    }
    function rowPrefsHtml(k,i,legacy,where){
        const p=rowPref(i), lin=k.type==='linear'&&!legacy;
        const btn=(which,label,tip)=>`<button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" data-kact="row${which}" data-k="${i}" data-where="${where}"
            aria-pressed="${p[which]?'true':'false'}" title="${esc(tip)}"><span class="ctl-led"></span>${esc(label)}</button>`;
        return `<div class="mx-k-row mx-k-rowprefs">${where==='side'?`<span class="mx-preset-section-label">${esc(T('plugin.mixer.k_rows','Rangées au pupitre'))}</span>`:''}
            ${btn('fill', lin?T('plugin.mixer.k_fill','Fill'):T('plugin.mixer.k_source','Source'), T('plugin.mixer.k_row_fill_tip','Afficher au pupitre la rangée de sources de ce keyer'))}
            ${lin?btn('key', T('plugin.mixer.k_key','Key'), T('plugin.mixer.k_row_key_tip','Afficher au pupitre la rangée key de ce keyer (linear)')):''}</div>`;
    }
    function keyerRowsHtml(rows,n){
        return rows.length?`<div class="mx-keyer-rows">${rows.map(r=>busRowHtml(r,n)).join('')}</div>`:'';
    }
    // ─── Bloc transition (0.23.18, maquettes du 25/10 : gauche = « C », droite = « A ») ──────────
    // Gauche : transition suivante (une touche par couche, voyant d'antenne au-dessus de chaque
    // keyer), type, motif et sens du volet, puis durée, AUTO et CUT. Droite : keyers INDÉPENDANTS —
    // ON AIR, AUTO et durée propre de chaque keyer, et le DSK (bascule seule : le moteur ne le fond pas).
    function takeBlockHtml(s){
        // État d'antenne au-dessus de chaque touche (maquette C) : « K1 » puis « ON » en rouge
        // d'antenne quand le keyer est à l'antenne — étiquette du catalogue (ctl-flag--2 sous .on2).
        const lamp=(t)=>{
            if(t.key==='bgd') return '<span class="mx-air-state" aria-hidden="true">&#160;</span>';
            const i=parseInt(t.key.slice(1),10), on=!!(keyers[i]&&keyers[i].active);
            return `<span class="mx-air-state${on?' on2':''}" data-lamp="${i}">K${i+1}<span class="ctl-flag ctl-flag--2">ON</span></span>`;
        };
        const tts=targetTypes();
        const next=tts.map(t=>`<div class="mx-next-cell">${lamp(t)}
            <button type="button" class="ctl-bus ctl-bus--panel" data-bus="dsk" data-target="${t.key}" aria-pressed="false"
                aria-label="${esc(T('plugin.mixer.next_aria','Transition suivante : {l}').replace('{l}',t.label))}" onclick="__mx.targetToggle('${t.key}')">
                <span class="ctl-bus-cap">${esc(t.label)}</span></button></div>`).join('');
        const modes=MX_MODE_TYPES.map(m=>`<button type="button" class="ctl-bus ctl-bus--panel" data-bus="dis" data-mode="${m.key}"
                role="radio" aria-checked="false" onclick="__mx.modeIconClick('${m.key}')"><span class="ctl-bus-cap">${esc(T('plugin.mixer.mode_'+m.key,m.label))}</span></button>`).join('');
        const left=`<div class="mx-take-main">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.next_transition_state','Transition suivante · état'))}</span>
                <div class="mx-next" role="group">${next}</div>
                <div class="mx-take-row">
                    <div class="mx-modes" role="radiogroup" aria-label="${esc(T('plugin.mixer.sec_type','Type'))}">${modes}</div>
                    <div class="mx-wipe-controls" id="mx-wipe-controls" hidden>${wipeIconsHtml()}<button type="button" class="ctl-push ctl-push--small ctl-push--led" id="mx-comp-chip"
                        aria-pressed="${preset.composite?'true':'false'}"${preset.composite?'':' hidden'} onclick="__mx.compOpenFromDesk()"
                        title="${esc(T('plugin.mixer.comp_chip_tip','Le prochain volet est ce volet composé — toucher pour l\'éditer'))}"><span class="ctl-led"></span><span data-chip-name>${esc(T('plugin.mixer.comp_chip','Composé : {name}').replace('{name}',preset.composite||''))}</span></button><span id="mx-dirmodes">${dirModesHtml(preset.wipe.reverse,preset.wipe.alternate,m=>`data-dirmode="${m}" onclick="__mx.dirModeSet('${m}')"`)}</span></div>
                </div>
                <div class="mx-take-row mx-exec">
                    <label class="ctl-readout ctl-readout--panel mx-dur" title="${esc(frameRate?T('plugin.mixer.duration_si_tip','Durée en secondes:images (« 1:12 »), ou un nombre d\'images'):T('plugin.mixer.duration_ms','Durée (ms)'))}">
                        <input type="text" inputmode="numeric" id="mx-dissolve-ms" value="${esc(frameRate?fmtDur(s.default_dissolve_ms||0):String(s.default_dissolve_ms||0))}"
                            aria-label="${esc(T('plugin.mixer.duration','Durée'))}" onchange="__mx.dissolveMsCommit()"><span class="ctl-unit">${frameRate?'s:im':'ms'}</span></label>
                    <button type="button" class="ctl-bus ctl-bus--panel ctl-bus--action ctl-bus--big btn-dissolve" data-bus="dis" onclick="__mx.take()" aria-label="AUTO">
                        <span class="ctl-bus-cap">AUTO</span></button>
                    <button type="button" class="ctl-bus ctl-bus--panel ctl-bus--action ctl-bus--big btn-cut" data-bus="pgm" onclick="__mx.take(0)" aria-label="Cut">
                        <span class="ctl-bus-cap">CUT</span></button>
                </div>
            </div>`;
        const cols=keyers.map((k,i)=>{
            const name=kName(i);
            const air=`<button type="button" class="ctl-bus ctl-bus--panel" data-bus="dsk" data-kact="onair" data-k="${i}"
                aria-pressed="${k.active?'true':'false'}" aria-label="${esc(name+' ON AIR')}"
                title="${esc(T('plugin.mixer.k_onair_tip','À l\'antenne / hors antenne, sans transition'))}"><span class="ctl-bus-cap">${esc(name)}<small>ON AIR</small></span></button>`;
            const auto=legacyEngine?'<span></span>':`<button type="button" class="ctl-bus ctl-bus--panel ctl-bus--action" data-bus="dis" data-kact="auto" data-k="${i}"
                aria-busy="${k.auto?'true':'false'}" aria-label="${esc(name+' AUTO')}"
                title="${esc(T('plugin.mixer.k_auto_tip','Fondu du keyer sur sa propre durée'))}"><span class="ctl-bus-cap">${esc(name)}<small>AUTO</small></span></button>`;
            const dur=legacyEngine?'<span></span>':`<span class="ctl-readout ctl-readout--panel mx-kdur" data-kdur="${i}" title="${esc(T('plugin.mixer.k_rate','Durée AUTO'))}"><b>${esc(fmtDur(k.auto_ms??500))}</b>${
                autoTypeOk?`<span class="ctl-unit" data-kmode="${i}">${esc(autoModeLabel(k.auto_mode))}</span>`:''}</span>`;
            return {air,auto,dur};
        });
        // Moteur d'avant 0.23.52 : l'habillage, une bascule seule, en bout de rangée des keyers.
        const dsk={air:`<button type="button" id="mx-dsk-toggle-btn" class="ctl-bus ctl-bus--panel" data-bus="dsk"
                onclick="__mx.dskToggle()" aria-pressed="${s.overlay_enabled?'true':'false'}" aria-label="DSK ON AIR">
                <span class="ctl-bus-cap">DSK<small>ON AIR</small></span></button>`, auto:'<span></span>', dur:'<span></span>'};
        const all=dskEngine?cols:cols.concat([dsk]);
        const right=`<div class="mx-take-keys" role="group" aria-label="${esc(T('plugin.mixer.keyers_indep','Keyers indépendants'))}">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.keyers_indep','Keyers indépendants'))}</span>
                <div class="mx-keys-grid">${all.map(c=>`<div class="mx-kcol">${c.air}${c.auto}${c.dur}</div>`).join('')}</div>
            </div>`;
        return `<div class="mx-take" role="group" aria-label="${esc(T('plugin.mixer.transition_aria','Transition'))}">${left}${right}${dskRowHtml()}</div>`;
    }
    // Rangée DSK (0.23.52), distincte des keyers, comme sur un ATEM : par DSK, CUT (ON AIR), AUTO (sur
    // sa durée), TIE (part avec la prochaine transition du fond) et la durée de son AUTO.
    function dskRowHtml(){
        if(!dskEngine||!dsks.length) return '';
        const cols=dsks.map((d,j)=>{
            const name=kName(keyers.length+j);
            return {
                air:`<button type="button" class="ctl-bus ctl-bus--panel" data-bus="dsk" data-dact="cut" data-d="${j}"
                    aria-pressed="${d.active?'true':'false'}" aria-label="${esc(name+' CUT')}"
                    title="${esc(T('plugin.mixer.dsk_cut_tip','À l\'antenne / hors antenne, sans transition'))}"><span class="ctl-bus-cap">${esc(name)}<small>ON AIR</small></span></button>`,
                auto:`<button type="button" class="ctl-bus ctl-bus--panel ctl-bus--action" data-bus="dis" data-dact="auto" data-d="${j}"
                    aria-busy="${d.auto?'true':'false'}" aria-label="${esc(name+' AUTO')}"
                    title="${esc(T('plugin.mixer.dsk_auto_tip','Fondu du DSK sur sa propre durée'))}"><span class="ctl-bus-cap">${esc(name)}<small>AUTO</small></span></button>`,
                tie:`<button type="button" class="ctl-bus ctl-bus--panel" data-bus="pvw" data-dact="tie" data-d="${j}"
                    aria-pressed="${d.tie?'true':'false'}" aria-label="${esc(name+' TIE')}"
                    title="${esc(T('plugin.mixer.dsk_tie_tip','Lié : entre ou sort avec la prochaine transition du fond'))}"><span class="ctl-bus-cap">${esc(name)}<small>TIE</small></span></button>`,
                dur:`<span class="ctl-readout ctl-readout--panel mx-kdur" data-ddur="${j}" title="${esc(T('plugin.mixer.k_rate','Durée AUTO'))}"><b>${esc(fmtDur(d.auto_ms??500))}</b></span>`};
        });
        return `<div class="mx-take-dsks" role="group" aria-label="${esc(T('plugin.mixer.dsk_row','DSK'))}">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.dsk_row','DSK'))}</span>
                <div class="mx-keys-grid">${cols.map(c=>`<div class="mx-kcol">${c.air}${c.auto}${c.tie}${c.dur}</div>`).join('')}</div>
            </div>`;
    }
    function dskStripRefresh(){
        dsks.forEach((d,j)=>{
            const c=$(`[data-dact="cut"][data-d="${j}"]`); if(c) c.setAttribute('aria-pressed',d.active?'true':'false');
            const a=$(`[data-dact="auto"][data-d="${j}"]`); if(a) a.setAttribute('aria-busy',d.auto?'true':'false');
            const t=$(`[data-dact="tie"][data-d="${j}"]`); if(t) t.setAttribute('aria-pressed',d.tie?'true':'false');
            const du=$(`[data-ddur="${j}"] b`); if(du&&d.auto_ms!=null) du.textContent=fmtDur(d.auto_ms);
        });
    }
    // Touches de la rangée DSK : CUT (bascule), AUTO, TIE (bascule) → /dsk/<n>/<geste>.
    async function dskButton(j,act){
        const d=dsks[j]; if(!d||VMID==null) return;
        let body={};
        if(act==='cut'){ d.active=!d.active; body={active:d.active}; }
        else if(act==='tie'){ d.tie=!d.tie; body={tie:d.tie}; }
        else if(act==='auto'){ d.auto=true; }
        else return;
        dskStripRefresh();
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/dsk/${j+1}/${act}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
            if(!r.ok){ let e={}; try { e=await r.json(); } catch (_) {} throw new Error(engineErr(e,r.status)); }
            clearError();
        }catch(e){ showError(T('plugin.mixer.dsk_err','DSK : {msg}').replace('{msg}', e.message)); }
    }
    function keyerDetailHtml(s,k,i,legacy){
        const n=i+1, lin=k.type==='linear', cut=k.type!=='dve';
        const typeSel=legacy?'':`<label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.k_type','Type'))}</span><select class="ctl-select" data-kact="type" data-k="${i}" aria-label="${esc(T('plugin.mixer.k_type_of','Type du keyer {n}').replace('{n}',n))}">${
            keyerTypesAvail().map(t=>`<option value="${t.v}"${k.type===t.v?' selected':''}>${esc(T(t.k,t.fb))}</option>`).join('')}</select></label>`;
        const web=k.type==='web';
        const fill=web?'':`<label class="mx-k-field"><span class="ctl-knob-name">${esc(lin?T('plugin.mixer.k_fill','Fill'):T('plugin.mixer.k_source','Source'))}</span>
            <select class="ctl-select" data-kact="input" data-k="${i}">${inputOpts(s,k.input)}</select></label>`;
        const key=(lin&&!legacy)?`<label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.k_key','Key'))}</span>
            <select class="ctl-select" data-kact="key_input" data-k="${i}">${inputOpts(s,k.key_input)}</select></label>`:'';
        const fly=[kKnob(i,'transform','x','X',0,1,0.01,k.x,0.5), kKnob(i,'transform','y','Y',0,1,0.01,k.y,0.5),
                   kKnob(i,'transform','scale',T('plugin.mixer.keyer_size','Taille'),0.05,1,0.01,k.scale,1)];
        if(k.type==='dve') fly.push(kKnob(i,'transform','border_width',T('plugin.mixer.keyer_border','Bordure'),0,30,1,k.border_width,0));
        const mask=legacy?[]:[['left','plugin.mixer.k_mask_l','Masque G'],['right','plugin.mixer.k_mask_r','Masque D'],
                              ['top','plugin.mixer.k_mask_t','Masque H'],['bottom','plugin.mixer.k_mask_b','Masque B']]
            .map(([f,kk,fb])=>kKnob(i,'mask',f,T(kk,fb),0,1,0.01,k.mask[f],0));
        const invertBtn=`<button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small" data-kact="invert" data-k="${i}" aria-pressed="${k.invert?'true':'false'}"><span class="ctl-led"></span>${esc(T('plugin.mixer.k_invert','Inverser'))}</button>`;
        const lumaCut=k.type==='linear'||k.type==='luma'||web;
        const shape=(cut&&!legacy)?(lumaCut?[kKnob(i,'key','clip',T('plugin.mixer.k_clip','Seuil'),0,1,0.01,k.clip,0),
                                    kKnob(i,'key','gain',T('plugin.mixer.k_gain','Gain'),0.1,20,0.1,k.gain,1),
            invertBtn]:k.type==='chroma'?keyerChromaHtml(k,i,invertBtn):k.type==='pattern'?keyerPatternHtml(k,i,invertBtn):[invertBtn]):[];
        return `<div class="mx-keyer-detail" data-k="${i}">
            <div class="mx-k-row">${typeSel}${fill}${key}</div>
            ${web?webLayerHtml(k,i):''}
            <div class="mx-k-row mx-k-knobs">${fly.join('')}</div>
            ${mask.length?`<div class="mx-k-row mx-k-knobs"><span class="mx-preset-section-label">${esc(T('plugin.mixer.k_mask','Masque'))}</span>${mask.join('')}</div>`:''}
            ${shape.length?`<div class="mx-k-row mx-k-knobs"><span class="mx-preset-section-label">${esc(k.type==='chroma'?T('plugin.mixer.k_chroma','Clé chroma'):k.type==='pattern'?T('plugin.mixer.k_pattern','Motif'):T('plugin.mixer.k_shape','Découpe'))}</span>${shape.join('')}</div>`:''}
            ${legacy?'':keyerAutoHtml(k,i)}
        </div>`;
    }
    // Couche WEB (0.23.52) : l'URL de la page (fill = sa couleur, alpha = sa transparence), son
    // aperçu et l'état de son onglet (chargée, erreur, cadence de capture mesurée).
    function webPreviewPath(i){ return isDsk(i)?`dsk/${i-keyers.length+1}/preview.png`:`keyer/${i+1}/preview.png`; }
    function webStatusText(k){
        const w=k.web||{}, eng=(state&&state.web_engine)||{};
        if(!k.url) return T('plugin.mixer.web_none','Aucune page : saisissez une URL.');
        if(eng.state==='unavailable') return T('plugin.mixer.web_unavailable','Navigateur absent de l\'image du conteneur : la page ne peut pas être rendue.');
        if(w.error) return T('plugin.mixer.web_error','Page en erreur : {e}').replace('{e}', w.error);
        if(!w.loaded) return T('plugin.mixer.web_loading','Chargement de la page…');
        return T('plugin.mixer.web_ok','Page rendue · {fps} captures/s · capture {ms} ms')
            .replace('{fps}', w.fps!=null?w.fps:'—').replace('{ms}', w.capture_ms!=null?w.capture_ms:'—');
    }
    function webLayerHtml(k,i){
        const max=(state&&state.web_layers_max!=null)?state.web_layers_max:null;
        return `<div class="mx-k-row mx-k-web">
                <label class="mx-k-field mx-k-url"><span class="ctl-knob-name">${esc(T('plugin.mixer.web_url','URL de la page'))}</span>
                    <input type="text" data-kurl="${i}" value="${esc(k.url||'')}" placeholder="https://…"
                        aria-label="${esc(T('plugin.mixer.web_url_of','URL de la page de {k}').replace('{k}',kName(i)))}"></label>
                <button type="button" class="ctl-push ctl-push--small" data-kact="weburl" data-k="${i}">${esc(T('plugin.mixer.apply','Appliquer'))}</button>
            </div>
            <p class="meta mx-web-status" data-webst="${i}">${esc(webStatusText(k))}${max!=null?' · '+esc(T('plugin.mixer.web_limit_note','{n}/{max} couches web').replace('{n}', state.web_layers||0).replace('{max}', max)):''}</p>
            ${k.url?`<div class="mx-dsk-preview-wrap"><img class="mx-dsk-preview" data-preview="${webPreviewPath(i)}" alt="${esc(T('plugin.mixer.web_preview','Aperçu de la page'))}"></div>`:''}`;
    }
    function webStatusRefresh(){
        layers.forEach((k,i)=>{ const p=$(`[data-webst="${i}"]`); if(p&&k.type==='web'){
            const max=(state&&state.web_layers_max!=null)?state.web_layers_max:null;
            p.textContent=webStatusText(k)+(max!=null?' · '+T('plugin.mixer.web_limit_note','{n}/{max} couches web').replace('{n}', state.web_layers||0).replace('{max}', max):'');
        } });
    }
    // Keyer CHROMA (0.23.22) : couleur de clé (sélecteur de couleur + teintes proposées), tolérance,
    // douceur, débordement, inversion. La couleur est convertie dans la colorimétrie du moteur ; s'il
    // la SUPPOSE (non déclarée), on le dit ici.
    function keyerChromaHtml(k,i,invertBtn){
        const col=String(k.chroma_color||'#00b140').toLowerCase();
        const pick=`<label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.k_chroma_color','Couleur de clé'))}</span>
            <input type="color" class="ctl-color" data-kact="ccolor" data-k="${i}" value="${esc(col)}"
                aria-label="${esc(T('plugin.mixer.k_chroma_color_of','Couleur de clé du keyer {n}').replace('{n}',i+1))}"></label>`;
        const sw=CHROMA_SWATCHES.map(c=>`<button type="button" class="ctl-push ctl-push--small" data-kact="cswatch" data-k="${i}" data-v="${c.v}"
            aria-pressed="${col===c.v?'true':'false'}"><span class="ctl-swatch" style="background:${c.v}"></span>${esc(T(c.k,c.fb))}</button>`).join('');
        const note=keyColorAssumed?`<p class="meta">${esc(T('plugin.mixer.k_chroma_assumed','Colorimétrie non déclarée : la couleur de clé est convertie en BT.709.'))}</p>`:'';
        return [pick, sw,
                kKnob(i,'chroma','chroma_tol',T('plugin.mixer.k_chroma_tol','Tolérance'),0,1,0.01,k.chroma_tol,0.20),
                kKnob(i,'chroma','chroma_soft',T('plugin.mixer.k_chroma_soft','Douceur'),0,1,0.01,k.chroma_soft,0.15),
                kKnob(i,'chroma','chroma_spill',T('plugin.mixer.k_chroma_spill','Débordement'),0,1,0.01,k.chroma_spill,0.50),
                invertBtn, note];
    }
    // Keyer MOTIF (0.23.22) : forme (motifs du volet + boîte), taille, position, douceur, inversion.
    // La forme est posée en coordonnées ÉCRAN : elle découpe le fill là où il est.
    // 0.23.44 : formes du volet (linéaire = barre, à angle), angle, répétition, bordure (couleur sous le
    // fill ; douceur interne = côté fond, externe = côté fill, qui remplace alors la douceur du motif).
    function keyerPatternHtml(k,i,invertBtn){
        const icons=`<div class="ctl-shape-row" role="radiogroup" aria-label="${esc(T('plugin.mixer.k_pattern','Motif'))}">${
            MX_WIPE_TYPES.map(w=>{ const on=shapeOn(w.key,k.pattern,k.pattern_angle); return `<button type="button" class="ctl-shape" data-kact="pshape" data-k="${i}" data-v="${w.key}"
                role="radio" aria-checked="${on?'true':'false'}" aria-label="${esc(w.label)}" title="${esc(w.label)}">${wipeIconSvg(w.key)}</button>`; }).join('')}</div>`;
        const col=String(k.pattern_border_color||'#ffffff').toLowerCase();
        const pick=`<label class="mx-k-field"><span class="ctl-knob-name">${esc(T('plugin.mixer.wipe_border_color','Couleur'))}</span>
            <input type="color" class="ctl-color" data-kact="pbcolor" data-k="${i}" value="${esc(col)}"
                aria-label="${esc(T('plugin.mixer.k_pattern_border_color_of','Couleur de la bordure du motif du keyer {n}').replace('{n}',i+1))}"></label>`;
        const pk=f=>`<div class="mx-pknob" data-k="${i}" data-pf="${f}"></div>`;
        return [icons, ...['pattern_size','pattern_x','pattern_y','pattern_soft','pattern_angle','pattern_repeat'].map(pk),
                invertBtn,
                `<span class="mx-preset-section-label">${esc(T('plugin.mixer.wipe_border','Bordure'))}</span>`,
                ...['pattern_border','pattern_border_soft_in','pattern_border_soft_out'].map(pk),
                pick];
    }
    // AUTO propre du keyer (0.23.19) : durée, et type — fondu (Mix) ou volet (Wipe, mêmes motifs que
    // le fond, avec sens et douceur). Moteur sans `keyer_auto_modes` : la durée seule.
    function keyerAutoHtml(k,i){
        // Pas du rotatif = UNE IMAGE quand la cadence est connue (sinon 50 ms, comme avant).
        const dur=kKnob(i,'rate','auto_ms',T('plugin.mixer.k_rate','Durée AUTO'),0,10000,frameRate?1000/frameRate:50,k.auto_ms,500);
        if(!autoTypeOk) return `<div class="mx-k-row mx-k-knobs"><span class="mx-preset-section-label">AUTO</span>${dur}</div>`;
        const modes=['mix','wipe'].map(m=>`<button type="button" class="ctl-push ctl-push--small ctl-push--led" data-kact="amode" data-k="${i}" data-v="${m}"
            aria-pressed="${k.auto_mode===m?'true':'false'}"><span class="ctl-led"></span>${esc(autoModeLabel(m))}</button>`).join('');
        const wipe=k.auto_mode!=='wipe'?'':`<div class="ctl-shape-row" role="radiogroup" aria-label="${esc(T('plugin.mixer.sec_pattern','Motif'))}">${
            MX_WIPE_TYPES.map(w=>`<button type="button" class="ctl-shape" data-kact="adir" data-k="${i}" data-v="${w.key}"
                role="radio" aria-checked="${k.auto_dir===w.key?'true':'false'}" aria-label="${esc(w.label)}" title="${esc(w.label)}">${wipeIconSvg(w.key)}</button>`).join('')}</div>
            ${dirModesHtml(k.auto_rev,k.auto_alt,m=>`data-kact="adirmode" data-k="${i}" data-v="${m}"`)}
            ${kKnob(i,'rate','auto_soft',T('plugin.mixer.softness','Douceur'),0,1,0.01,k.auto_soft,0.10)}`;
        return `<div class="mx-k-row mx-k-knobs mx-k-auto"><span class="mx-preset-section-label">AUTO</span>
            <div class="mx-modes" role="radiogroup" aria-label="${esc(T('plugin.mixer.k_auto_type','Type de l\'AUTO'))}">${modes}</div>${dur}</div>
            ${wipe?`<div class="mx-k-row mx-k-knobs">${wipe}</div>`:''}`;
    }
    function bindKeyers(root){
        const r=root||EL; if(!r) return;
        r.querySelectorAll('[data-kact]').forEach(el=>{
            const i=parseInt(el.dataset.k,10), act=el.dataset.kact;
            if(el.tagName==='SELECT'||el.tagName==='INPUT') el.addEventListener('change',()=>keyerSelect(i,act,el.value));
            else el.addEventListener('click',()=>keyerButton(i,act,el));
        });
        const pk=r.querySelector('#mx-side-kpick');
        if(pk&&pk.addEventListener) pk.addEventListener('change',()=>sideEditKeyer(parseInt(pk.value,10)));
        const pd=r.querySelector('#mx-side-dpick');
        if(pd&&pd.addEventListener) pd.addEventListener('change',()=>sideEditKeyer(keyers.length+parseInt(pd.value,10)));
        r.querySelectorAll('[data-dact]').forEach(el=>{
            el.addEventListener('click',()=>dskButton(parseInt(el.dataset.d,10), el.dataset.dact));
        });
    }
    function keyerStripRefresh(){
        dskStripRefresh();
        keyers.forEach((k,i)=>{
            const on=$(`[data-kact="onair"][data-k="${i}"]`); if(on) on.setAttribute('aria-pressed',k.active?'true':'false');
            const lp=$(`[data-lamp="${i}"]`); if(lp) lp.classList.toggle('on2',!!k.active);
            const du=$(`[data-kdur="${i}"] b`); if(du&&k.auto_ms!=null) du.textContent=fmtDur(k.auto_ms);
            const dm=$(`[data-kmode="${i}"]`); if(dm) dm.textContent=autoModeLabel(k.auto_mode);
            const au=$(`[data-kact="auto"][data-k="${i}"]`); if(au) au.setAttribute('aria-busy',k.auto?'true':'false');
            const iv=$(`[data-kact="invert"][data-k="${i}"]`); if(iv) iv.setAttribute('aria-pressed',k.invert?'true':'false');
            const cl=$(`[data-kact="clean"][data-k="${i}"]`); if(cl) cl.setAttribute('aria-pressed',k.clean?'true':'false');
            const xs=$(`[data-xp-sub="${i}"]`); if(xs) xs.textContent=xpSub(k);
        });
    }
    // Touches des rangées de sources (BGD A / BGD B / keyers) : état allumé, câblage, libellé.
    function busRefresh(s){
        xpInputs=s.inputs||[];
        busRowsRender(s);
    }
    async function keyerPost(ep,body){
        if(VMID==null) return false;
        const [path,b]=layerRoute(ep,body);
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/${path}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});
            if(!r.ok){ let j={}; try { j=await r.json(); } catch (_) {} throw new Error(engineErr(j,r.status)); }
            clearError(); return true;
        }catch(e){ showError(T('plugin.mixer.k_err','Keyer : {msg}').replace('{msg}', e.message)); return false; }
    }
    async function keyerPostJson(ep,body){
        if(VMID==null) return null;
        const [path,b]=layerRoute(ep,body);
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/${path}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});
            let j={}; try { j=await r.json(); } catch (_) {}
            if(!r.ok) throw new Error(engineErr(j,r.status));
            clearError(); return j||{};
        }catch(e){ showError(T('plugin.mixer.k_err','Keyer : {msg}').replace('{msg}', e.message)); return null; }
    }
    async function keyerKnobCommit(i,g){
        const k=layers[i]; if(!k) return;
        const src=g==='mask'?k.mask:k, body={keyer:i+1};
        (KEYER_GROUP_FIELDS[g]||[]).forEach(f=>{ body[f]=src[f]; });
        await keyerPost(g, body);
        if(_dve) _dve.memoriser();     // un réglage aux rotatifs s'annule comme un geste de la surface
    }
    async function keyerSelect(i,act,v){
        const k=layers[i]; if(!k) return;
        if(act==='type'){
            k.type=v; await keyerPost('type',{keyer:i+1, type:v});
            await fetchState({fullRebuild:true});   // les réglages affichés dépendent du type
            return;
        }
        if(act==='ccolor'){
            k.chroma_color=String(v||'').toLowerCase(); await keyerPost('chroma',{keyer:i+1, chroma_color:k.chroma_color});
            renderSideKeyer();
            return;
        }
        if(act==='pbcolor'){
            k.pattern_border_color=String(v||'').toLowerCase(); await keyerPost('pattern',{keyer:i+1, pattern_border_color:k.pattern_border_color});
            return;
        }
        const n=parseInt(v,10), body={keyer:i+1}; k[act]=n; body[act]=n;
        busRefresh(state||{});
        await keyerPost('input', body);
    }
    // Appui sur une touche d'une rangée de sources de keyer (fill ou key).
    function keyerRowPick(i,field,j){
        const k=layers[i]; if(!k) return;
        k[field]=j; if(state) busRefresh(state);
        const sel=$(`select[data-kact="${field}"][data-k="${i}"]`); if(sel) sel.value=String(j);
        const body={keyer:i+1}; body[field]=j;
        keyerPost('input', body);
    }
    function keyerSettingsOf(k){
        const d={}; KEYER_COPY_FIELDS.forEach(f=>{ d[f]=k[f]; }); d.mask=Object.assign({},k.mask); return d;
    }
    // {v, settings, from} si le presse-papiers porte un keyer, sinon null. Ancien format système
    // ({kind, v, settings} sans enveloppe) : la base le rend comme sa propre donnée.
    function clipOf(c){ const d=c&&c.data; return (d&&d.settings&&typeof d.settings==='object')?d:null; }
    function clipRead(){
        const E=window.BobiEditor;
        return E&&E.clipboard ? clipOf(E.clipboard.get(CLIP_KIND)) : null;
    }
    function keyerCopy(i){
        const k=layers[i]; if(!k) return;
        window.BobiEditor.clipboard.put(CLIP_KIND, {v:1, settings:keyerSettingsOf(k), from:kName(i)}, {system:true});
        TOAST(T('plugin.mixer.k_copied','Réglages du {name} copiés').replace('{name}', kName(i)));
        $all('[data-kact="paste"]').forEach(b=>{ b.disabled=false; b.removeAttribute('disabled'); });
    }
    async function keyerPaste(i){
        const clip=clipOf(await window.BobiEditor.clipboard.read(CLIP_KIND));
        if(!clip){ showError(T('plugin.mixer.k_paste_none','Rien à coller : copiez d\'abord un keyer.')); return; }
        const settings={}; KEYER_COPY_FIELDS.forEach(f=>{ if(f in clip.settings) settings[f]=clip.settings[f]; });
        if(clip.settings.mask) settings.mask=clip.settings.mask;
        const j=await keyerPostJson('settings',{keyer:i+1, settings});
        if(!j) return;
        TOAST(T('plugin.mixer.k_pasted','Réglages collés sur {name}').replace('{name}', kName(i))
              + ((j.skipped&&j.skipped.length)?' — '+T('plugin.mixer.k_skipped','champs non appliqués : {f}').replace('{f}', j.skipped.join(', ')):''));
        await fetchState({fullRebuild:true});
    }
    async function keyerPresetSave(i){
        const inp=$('#mx-k-preset-name'), nm=((inp&&inp.value)||'').trim();
        if(!nm){ showError(T('plugin.mixer.k_preset_need_name','Donnez un nom à la sauvegarde.')); return; }
        const j=await keyerPostJson('preset/save',{keyer:i+1, name:nm});
        if(!j) return;
        TOAST(T('plugin.mixer.k_preset_saved','Sauvegarde « {name} » enregistrée').replace('{name}', j.name||nm));
        await fetchState({fullRebuild:false}); renderSideKeyer();
    }
    async function keyerPresetRecall(i,nm){
        const j=await keyerPostJson('preset/recall',{keyer:i+1, name:nm});
        if(!j) return;
        TOAST(T('plugin.mixer.k_preset_recalled','« {name} » rappelée sur {k}').replace('{name}', nm).replace('{k}', kName(i))
              + ((j.skipped&&j.skipped.length)?' — '+T('plugin.mixer.k_skipped','champs non appliqués : {f}').replace('{f}', j.skipped.join(', ')):''));
        await fetchState({fullRebuild:true});
    }
    async function keyerPresetDelete(nm){
        const j=await keyerPostJson('preset/delete',{name:nm});
        if(!j) return;
        await fetchState({fullRebuild:false}); renderSideKeyer();
    }
    function keyerButton(i,act,el){
        const k=layers[i]; if(!k) return;
        if(act==='onair'){ k.active=!k.active; keyerStripRefresh(); if(i===0) keyerRefreshActiveUi(); keyerPost('active',{keyer:i+1, active:k.active}); }
        else if(act==='auto'){ k.auto=true; keyerStripRefresh(); keyerPost('auto',{keyer:i+1}); }
        else if(act==='invert'){ k.invert=!k.invert; keyerStripRefresh(); keyerPost('key',{keyer:i+1, invert:k.invert}); }
        else if(act==='amode'){ const v=el&&el.dataset.v; if(!v||v===k.auto_mode) return; k.auto_mode=v; keyerPost('rate',{keyer:i+1, auto_mode:v}); renderSideKeyer(); keyerStripRefresh(); }
        else if(act==='adir'){ const v=el&&el.dataset.v; if(!v) return; k.auto_dir=v; keyerPost('rate',{keyer:i+1, auto_dir:v}); renderSideKeyer(); }
        else if(act==='adirmode'){
            const m=el&&el.dataset.v;
            if(m==='norm'){ k.auto_rev=false; k.auto_alt=false; } else if(m==='rev'){ k.auto_rev=true; k.auto_alt=false; } else if(m==='nr') k.auto_alt=true; else return;
            keyerPost('rate',{keyer:i+1, auto_rev:k.auto_rev, auto_alt:k.auto_alt}); renderSideKeyer();
        }
        else if(act==='cswatch'){ const v=el&&el.dataset.v; if(!v) return; k.chroma_color=v; keyerPost('chroma',{keyer:i+1, chroma_color:v}); renderSideKeyer(); }
        else if(act==='pshape'){
            const v=el&&el.dataset.v; if(!v) return;
            if(v in MX_LEGACY_ANGLES){ k.pattern='linear'; k.pattern_angle=MX_LEGACY_ANGLES[v]; } else k.pattern=v;
            keyerPost('pattern',{keyer:i+1, pattern:v}); renderSideKeyer();
        }
        else if(act==='clean'){ k.clean=!k.clean; keyerStripRefresh(); keyerPost('clean',{keyer:i+1, clean:k.clean}); }
        else if(act==='weburl'){
            const inp=$(`[data-kurl="${i}"]`), url=((inp&&inp.value)||'').trim();
            keyerPost('web',{keyer:i+1, url}).then(ok=>{ if(ok){ k.url=url; fetchState({fullRebuild:true}); } });
        }
        else if(act==='edit') sideEditKeyer(i);
        else if(act==='rowfill') rowPrefSet(i,'fill',!rowPref(i).fill);
        else if(act==='rowkey') rowPrefSet(i,'key',!rowPref(i).key);
        else if(act==='copy') keyerCopy(i);
        else if(act==='paste') keyerPaste(i);
        else if(act==='psave') keyerPresetSave(i);
        else if(act==='recall') keyerPresetRecall(i, el&&el.dataset.name);
        else if(act==='pdelete') keyerPresetDelete(el&&el.dataset.name);
    }

    // ─── Entrées & délais ─────────────────────────────────
    function setView(v){
        view = v==='delays' ? 'delays' : 'ops';
        try { localStorage.setItem('mx-view', view); } catch (_) {}
        $all('.tabs [data-view]').forEach(b=>{const on=b.dataset.view===view; b.classList.toggle('active',on); b.setAttribute('aria-selected',on?'true':'false');});
        const o=$('#mx-view-ops'), d=$('#mx-view-delays');
        if(o) o.hidden = view!=='ops';
        if(d) d.hidden = view!=='delays';
        if(view==='delays') renderDelays(state);
        syncDecorate(state);
    }
    function imgs(n){ return n===1 ? T('plugin.mixer.dl_1img','1 image') : T('plugin.mixer.dl_nimg','{n} images').replace('{n}', n); }
    function delayLabel(n){ return n===0 ? T('plugin.mixer.dl_fastest','0 — au plus rapide') : imgs(n); }
    async function delaysCall(body){
        if(VMID==null) return;
        try{
            const r=await fetch(`/api/containers/${VMID}/plugin/delays`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
            if(!r.ok){
                // Refus du moteur = un CODE (renommage : unknown_group, name_required, name_taken), traduit ici.
                const j=await r.json().catch(()=>({})), code=j&&j.error;
                const msgs={unknown_group:T('plugin.mixer.dl_err_unknown_group','ce groupe n\'existe plus'),
                    name_required:T('plugin.mixer.dl_name_required','Donnez un nom au groupe.'),
                    name_taken:T('plugin.mixer.dl_err_name_taken','ce nom est déjà pris par un autre groupe')};
                throw new Error(msgs[code]||code||('HTTP '+r.status));
            }
            clearError();
        }catch(e){ showError(T('plugin.mixer.dl_err','Délais refusés : {msg}').replace('{msg}', e.message)); }
        await fetchState({fullRebuild:false});
        renderDelays(state);
    }
    // ── Arrivées (0.23.21) : à quel moment de son créneau la 1ʳᵉ tranche de chaque image arrive.
    // Point = arrivée habituelle (médiane, 4 s), barre = du 5ᵉ centile au pire cas, trait =
    // échéance de son délai : (délai + ½) créneau, l'instant où le mélangeur cesse d'attendre.
    // Échelle COMMUNE à toutes les lignes (X, X+1, X+2 au moins), élargie si une entrée arrive plus
    // tard. Moteur antérieur à 0.23.21 : pas de champ `arrival`, colonnes vides.
    function fmtMs(v){
        try { return v.toLocaleString(document.documentElement.lang||undefined,{minimumFractionDigits:1,maximumFractionDigits:1}); }
        catch(_){ return v.toFixed(1); }
    }
    function arrScale(list, slotMs){
        let lo=0, hi=3*slotMs;
        list.forEach(({a,dl})=>{
            hi=Math.max(hi, dl+slotMs/4);
            if(a&&a.n){ lo=Math.min(lo, a.lo); hi=Math.max(hi, a.max+slotMs/8); }
        });
        const start=Math.floor(lo/slotMs)*slotMs, n=Math.min(10, Math.max(3, Math.ceil((hi-start)/slotMs)));
        return {start, n, span:n*slotMs, slotMs};
    }
    function arrPct(sc, ms){ return Math.max(0, Math.min(100, (ms-sc.start)/sc.span*100)).toFixed(2)+'%'; }
    function arrBands(sc){
        return Array.from({length:sc.n},(_,j)=>`<span class="mx-arr-band" style="left:${arrPct(sc, sc.start+j*sc.slotMs)};width:${(100/sc.n).toFixed(2)}%"></span>`).join('');
    }
    function arrAxisHtml(sc){
        const first=Math.round(sc.start/sc.slotMs);
        return `<div class="mx-arr mx-arr-axis" aria-hidden="true">${Array.from({length:sc.n},(_,j)=>{
            const k=first+j; return `<span style="left:${arrPct(sc, sc.start+j*sc.slotMs)}">${k===0?'X':(k>0?'X+'+k:'X'+k)}</span>`;
        }).join('')}</div>`;
    }
    function arrKind(a, dl){ return a.med>dl ? 'late' : (a.max>dl ? 'warn' : 'ok'); }
    function arrCellHtml(a, dl, sc){
        if(!a) return '<span class="meta">—</span>';
        if(a.offgrid) return `<span class="meta">${esc(T('plugin.mixer.dl_st_offgrid','hors grille'))}</span>`;
        if(!a.n) return `<span class="meta">${esc(T('plugin.mixer.dl_arr_none','pas de mesure'))}</span>`;
        const k=arrKind(a, dl);
        const aria=T('plugin.mixer.dl_arr_aria','Arrivée habituelle {med} ms, pire cas {max} ms, échéance {dl} ms')
            .replace('{med}', fmtMs(a.med)).replace('{max}', fmtMs(a.max)).replace('{dl}', fmtMs(dl));
        return `<div class="mx-arr-cell"><div class="mx-arr" role="img" aria-label="${esc(aria)}">${arrBands(sc)}
            <span class="mx-arr-dl" style="left:${arrPct(sc, dl)}"></span>
            <span class="mx-arr-bar mx-arr-${k}" style="left:${arrPct(sc, a.lo)};width:calc(${arrPct(sc, a.max)} - ${arrPct(sc, a.lo)})"></span>
            <span class="mx-arr-dot mx-arr-${k}" data-arr="${k}" style="left:${arrPct(sc, a.med)}"></span></div>
            <span class="mx-arr-ms mx-arr-${k}">${esc(fmtMs(a.med))}</span></div>`;
    }
    // Dérive : une médiane toutes les `step` secondes sur 60 s ; pente par moindres carrés.
    function trendSlope(tr, step){
        const pts=tr.map((v,j)=>[j*step/60, v]).filter(p=>p[1]!==null&&p[1]!==undefined);
        if(pts.length<5) return null;
        const n=pts.length, mx=pts.reduce((a,p)=>a+p[0],0)/n, my=pts.reduce((a,p)=>a+p[1],0)/n;
        const sxx=pts.reduce((a,p)=>a+(p[0]-mx)*(p[0]-mx),0);
        return sxx ? pts.reduce((a,p)=>a+(p[0]-mx)*(p[1]-my),0)/sxx : null;
    }
    function trendCellHtml(a, dl, step){
        const tr=(a&&!a.offgrid&&Array.isArray(a.trend))?a.trend:[];
        const vals=tr.filter(v=>v!==null&&v!==undefined);
        if(vals.length<2) return '<span class="meta">—</span>';
        let lo=Math.min(...vals), hi=Math.max(...vals);
        if(hi-lo<2){ const m=(hi+lo)/2; lo=m-1; hi=m+1; }
        const N=Math.max(tr.length,2), y=v=>(22-(v-lo)/(hi-lo)*20).toFixed(1);
        const segs=[]; let cur=[];
        tr.forEach((v,j)=>{ if(v===null||v===undefined){ if(cur.length) segs.push(cur); cur=[]; } else cur.push(`${(j/(N-1)*100).toFixed(1)},${y(v)}`); });
        if(cur.length) segs.push(cur);
        const sl=trendSlope(tr, step);
        const txt=sl===null ? '—' : (Math.abs(sl)<0.3 ? T('plugin.mixer.dl_trend_stable','stable')
            : T('plugin.mixer.dl_trend_slope','{v} ms/min').replace('{v}', (sl>0?'+':'−')+fmtMs(Math.abs(sl))));
        const k=a&&a.n?arrKind(a, dl):'ok';
        const dlLine=(dl>=lo&&dl<=hi)?`<line x1="0" x2="100" y1="${y(dl)}" y2="${y(dl)}" class="mx-trend-dl" vector-effect="non-scaling-stroke"></line>`:'';
        const aria=T('plugin.mixer.dl_trend_aria','Arrivée habituelle sur 60 s : {s}').replace('{s}', txt);
        return `<div class="mx-trend-cell"><svg class="mx-trend" viewBox="0 0 100 24" preserveAspectRatio="none" role="img" aria-label="${esc(aria)}">${dlLine}${
            segs.map(sg=>`<polyline points="${sg.join(' ')}" class="mx-arr-${k}" vector-effect="non-scaling-stroke"></polyline>`).join('')}</svg>
            <span class="mx-trend-slope${sl!==null&&Math.abs(sl)>=1?' mx-trend-drift':''}">${esc(txt)}</span></div>`;
    }

    function renderDelays(s){
        const host=$('#mx-view-delays'); if(!host||!s||view!=='delays') return;
        if(!s.slice_mode){
            host.innerHTML=`<p class="meta">${esc(T('plugin.mixer.dl_no_slice',"Les délais et groupes s'appliquent en mode tranche. Ce mélangeur tourne en image entière : il suit l'ancien fonctionnement (entrée de référence)."))}</p>`;
            return;
        }
        if(!s.delays){
            host.innerHTML=`<div class="alert alert-warning">${esc(T('plugin.mixer.dl_stale','Le moteur déployé ne connaît pas les délais : redéployez le conteneur.'))}</div>`;
            return;
        }
        const d=s.delays, groups=d.groups||{}, gnames=Object.keys(groups).sort();
        const max=(s.caps&&s.caps.group_delay_max)||3, nameMax=(s.caps&&s.caps.group_name_max)||32;
        const defD=s.latency_mode==='fastest'?0:1;
        const timing=s.inputs_timing||{}, inputs=s.inputs||[];
        const defLabel=s.latency_mode==='fastest'?T('plugin.mixer.dl_mode_fastest','Au plus rapide'):T('plugin.mixer.dl_mode_fixed','Fixe, 1 image');
        const rows=[], late={}, needMax={};
        const slotMs=1000/(parseInt(s.frame_rate,10)||25), trendStep=(s.caps&&s.caps.arrival_trend_step_s)||2;
        // Décalage amont (0.23.22) : l'entrée est lue `amont` images plus récente que son groupe ;
        // moteur antérieur → pas de colonne. Échéance du graphique = délai réellement LU.
        const upMax=(s.caps&&s.caps.upstream_max)||0, ups=d.upstream||{};
        const upOf=k=>upMax?(parseInt(ups[k],10)||0):0;
        const delayOf=k=>{ const g=(d.inputs||{})[k]||''; return g?groups[g]:defD; };
        const dlOf=k=>(Math.max(0, delayOf(k)-upOf(k))+0.5)*slotMs;
        const sc=arrScale(inputs.map((n,i)=>n?{a:(timing[String(i+1)]||{}).arrival, dl:dlOf(String(i+1))}:null).filter(Boolean), slotMs);
        inputs.forEach((name,i)=>{
            if(!name) return;
            const k=String(i+1), t=timing[k]||{}, g=(d.inputs||{})[k]||'';
            const delay=g?groups[g]:defD, up=upOf(k), needRaw=(t.need===undefined?null:t.need);
            const need=needRaw===null?null:needRaw+up;     // ce que le groupe doit offrir
            const p=prevTiming&&prevTiming[k];
            const df=p?(t.frames-p.frames):0, pct=(p&&df>0)?Math.round(100*(t.late-p.late)/df):null;
            let st, cls;
            if(!t.frames && need===null){ st=T('plugin.mixer.dl_st_wait','en attente de mesure'); cls='ready'; }
            else if(need===null){ st=T('plugin.mixer.dl_st_offgrid','hors grille'); cls='ready'; }
            else if(need>delay && up>delay){ st=T('plugin.mixer.dl_st_upstream','amont non compensé'); cls='warn'; }
            else if(need>delay){ st=T('plugin.mixer.dl_st_late','en retard'); cls='warn'; }
            else { st=T('plugin.mixer.dl_st_ok',"à l'heure"); cls='running'; }
            const key=g||'';
            if(need!==null && (needMax[key]===undefined || need>needMax[key].need)) needMax[key]={need, i, name};
            const opts=[`<option value=""${g?'':' selected'}>${esc(T('plugin.mixer.dl_nogroup','Sans groupe (défaut : {d})').replace('{d}', imgs(defD)))}</option>`]
                .concat(gnames.map(n=>`<option value="${esc(n)}"${g===n?' selected':''}>${esc(n)} (${esc(imgs(groups[n]))})</option>`)).join('');
            rows.push(`<tr>
                <td class="mx-dl-num">#${i+1}</td>
                <td class="mx-dl-src" title="${esc(name)}">${esc(name)}</td>
                <td><select class="ctl-select" data-act="assign" data-input="${i}" aria-label="${esc(T('plugin.mixer.dl_group_of','Groupe de l\'entrée #{i}').replace('{i}', i+1))}">${opts}</select></td>
                ${upMax?`<td><select class="ctl-select" data-act="upstream" data-input="${i}" aria-label="${esc(T('plugin.mixer.dl_upstream_of','Décalage amont de l\'entrée #{i}').replace('{i}', i+1))}">${
                    Array.from({length:upMax+1},(_,n)=>`<option value="${n}"${up===n?' selected':''}>${n}</option>`).join('')}</select></td>`:''}
                <td><span class="ctl-readout"><b>${delay}</b><span>${esc(T('plugin.mixer.dl_unit','img'))}</span></span></td>
                <td${up&&needRaw!==null?` title="${esc(T('plugin.mixer.dl_need_split','mesuré {m} + amont {u}').replace('{m}', needRaw).replace('{u}', up))}"`:''}><span class="ctl-readout"><b>${need===null?'—':need}</b><span>${esc(T('plugin.mixer.dl_unit','img'))}</span></span></td>
                <td class="mx-dl-pct">${pct===null?'—':pct+' %'}</td>
                <td><span class="badge ${cls}">${esc(st)}</span></td>
                <td class="mx-dl-arrcol">${arrCellHtml(t.arrival, dlOf(k), sc)}</td>
                <td class="mx-dl-trendcol">${trendCellHtml(t.arrival, dlOf(k), trendStep)}</td></tr>`);
        });
        prevTiming=JSON.parse(JSON.stringify(timing));
        const delayOpts=v=>Array.from({length:max+1},(_,n)=>`<option value="${n}"${v===n?' selected':''}>${esc(delayLabel(n))}</option>`).join('');
        const groupRows=gnames.map(n=>{
            const nm=needMax[n];
            const prop=(nm && nm.need>groups[n])
                ? `<span class="badge warn">${esc(T('plugin.mixer.dl_needs','#{i} ({src}) demande {d}').replace('{i}', nm.i+1).replace('{src}', nm.name).replace('{d}', imgs(nm.need)))}</span>
                   <button type="button" class="ctl-push ctl-push--small" data-act="propose" data-group="${esc(n)}" data-frames="${Math.min(max, nm.need)}">${esc(T('plugin.mixer.dl_apply_need','Passer à {d}').replace('{d}', imgs(Math.min(max, nm.need))))}</button>`
                : '';
            return `<div class="mx-dl-group">
                <input type="text" class="mx-dl-gname" data-act="rename" data-group="${esc(n)}" value="${esc(n)}" maxlength="${nameMax}" aria-label="${esc(T('plugin.mixer.dl_rename_of','Nom du groupe {g}').replace('{g}', n))}" title="${esc(T('plugin.mixer.dl_rename_tip','Modifier le nom : les entrées du groupe et son délai sont conservés'))}">
                <select class="ctl-select" data-act="gdelay" data-group="${esc(n)}" aria-label="${esc(T('plugin.mixer.dl_delay_of','Délai du groupe {g}').replace('{g}', n))}">${delayOpts(groups[n])}</select>
                ${prop}
                <button type="button" class="ctl-push ctl-push--small ctl-push--danger" data-act="remove" data-group="${esc(n)}">${esc(T('plugin.mixer.dl_remove','Supprimer'))}</button></div>`;
        }).join('');
        const nm0=needMax[''];
        const defWarn=(nm0 && nm0.need>defD)
            ? `<p class="mx-dl-defwarn"><span class="badge warn">${esc(T('plugin.mixer.dl_def_needs','#{i} ({src}) demande {d} : rangez-la dans un groupe, ou passez le délai par défaut à « Fixe, 1 image » (⚙).').replace('{i}', nm0.i+1).replace('{src}', nm0.name).replace('{d}', imgs(nm0.need)))}</span></p>`
            : '';
        const refOpts=[`<option value=""${d.ref_group?'':' selected'}>${esc(T('plugin.mixer.dl_ref_default','Délai par défaut'))}</option>`]
            .concat(gnames.map(n=>`<option value="${esc(n)}"${d.ref_group===n?' selected':''}>${esc(n)}</option>`)).join('');
        host.innerHTML=`
            <div class="mx-dl-head">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.dl_default','Délai par défaut'))}</span>
                <span class="ctl-readout"><b>${esc(defLabel)}</b></span>
                <span class="meta">${esc(T('plugin.mixer.dl_default_where','se règle dans ⚙'))}</span>
                <label class="mx-dl-ref"><span class="mx-preset-section-label">${esc(T('plugin.mixer.dl_ref','Groupe de référence'))}</span>
                    <select class="ctl-select" data-act="ref">${refOpts}</select></label>
            </div>
            <p class="meta">${esc(T('plugin.mixer.dl_help',"Chaque entrée est lue à l'image « maintenant − son délai ». Les entrées d'un groupe sont calées ensemble ; une entrée sans groupe suit le délai par défaut. Le programme porte le numéro d'image du groupe de référence, qui ne change pas à la commutation. « Demande » est mesuré en continu : une entrée qui demande plus que son délai passe avec sa dernière image."))}</p>
            <p class="meta">${esc(T('plugin.mixer.dl_arr_help',"Arrivée : le point marque l'arrivée habituelle de la 1re tranche dans le créneau de son image (X), la barre va jusqu'au pire cas des 4 dernières secondes, le trait marque l'échéance de son délai. Point avant le trait : l'entrée est attendue ; après : elle passe avec son image précédente."))}</p>
            ${upMax?`<p class="meta">${esc(T('plugin.mixer.dl_upstream_help',"Amont : une source dont le contenu arrive déjà en retard (HF, liaison, traitement extérieur) se recale ici. Elle est lue d'autant plus récente que son groupe, qui doit donc offrir au moins « demande mesurée + amont ». La Demande affichée en tient compte."))}</p>`:''}
            ${defWarn}
            <div class="mx-dl-tablewrap"><table class="mx-dl-table">
                <thead><tr><th>#</th><th>${esc(T('plugin.mixer.dl_col_src','Source'))}</th><th>${esc(T('plugin.mixer.dl_col_group','Groupe'))}</th>${upMax?`<th title="${esc(T('plugin.mixer.dl_col_upstream_tip','Retard du contenu à son arrivée (HF, liaison, traitement extérieur), en images : l\'entrée est lue d\'autant plus récente que son groupe'))}">${esc(T('plugin.mixer.dl_col_upstream','Amont'))}</th>`:''}<th>${esc(T('plugin.mixer.dl_col_delay','Délai'))}</th><th>${esc(T('plugin.mixer.dl_col_need','Demande'))}</th><th>${esc(T('plugin.mixer.dl_col_late','Replis'))}</th><th>${esc(T('plugin.mixer.dl_col_state','État'))}</th><th class="mx-dl-arrcol"><span>${esc(T('plugin.mixer.dl_col_arrival','Arrivée'))}</span>${arrAxisHtml(sc)}</th><th class="mx-dl-trendcol">${esc(T('plugin.mixer.dl_col_trend','Dérive (60 s)'))}</th></tr></thead>
                <tbody>${rows.join('')||`<tr><td colspan="${upMax?10:9}" class="meta">${esc(T('plugin.mixer.dl_none','Aucune entrée câblée.'))}</td></tr>`}</tbody>
            </table></div>
            <div class="mx-dl-groups">
                <span class="mx-preset-section-label">${esc(T('plugin.mixer.dl_groups','Groupes de délai'))}</span>
                ${groupRows||`<p class="meta">${esc(T('plugin.mixer.dl_nogroups','Aucun groupe : toutes les entrées suivent le délai par défaut.'))}</p>`}
                <div class="mx-dl-new">
                    <input type="text" id="mx-dl-newname" maxlength="${nameMax}" placeholder="${esc(T('plugin.mixer.dl_newname','Nom du groupe (Plateau…)'))}">
                    <select class="ctl-select" id="mx-dl-newdelay" aria-label="${esc(T('plugin.mixer.dl_col_delay','Délai'))}">${delayOpts(1)}</select>
                    <button type="button" class="ctl-push ctl-push--small" data-act="create">${esc(T('plugin.mixer.dl_create','Créer le groupe'))}</button>
                </div>
            </div>`;
        host.querySelectorAll('[data-act="assign"]').forEach(el=>el.addEventListener('change',()=>delaysCall({input:parseInt(el.dataset.input,10), group:el.value||null})));
        host.querySelectorAll('[data-act="upstream"]').forEach(el=>el.addEventListener('change',()=>delaysCall({input:parseInt(el.dataset.input,10), upstream:parseInt(el.value,10)})));
        host.querySelectorAll('[data-act="gdelay"]').forEach(el=>el.addEventListener('change',()=>delaysCall({group:el.dataset.group, frames:parseInt(el.value,10)})));
        host.querySelectorAll('[data-act="propose"]').forEach(el=>el.addEventListener('click',()=>delaysCall({group:el.dataset.group, frames:parseInt(el.dataset.frames,10)})));
        host.querySelectorAll('[data-act="rename"]').forEach(el=>{
            const commit=()=>{
                const to=el.value.trim();
                if(to===el.dataset.group) return;
                if(!to){ el.value=el.dataset.group; showError(T('plugin.mixer.dl_name_required','Donnez un nom au groupe.')); return; }
                delaysCall({rename_group:el.dataset.group, to});
            };
            el.addEventListener('change', commit);
            el.addEventListener('keydown', ev=>{
                if(ev.key==='Enter'){ ev.preventDefault(); el.blur(); }
                else if(ev.key==='Escape'){ el.value=el.dataset.group; el.blur(); }
            });
        });
        host.querySelectorAll('[data-act="remove"]').forEach(el=>el.addEventListener('click',()=>delaysCall({remove_group:el.dataset.group})));
        const ref=host.querySelector('[data-act="ref"]'); if(ref) ref.addEventListener('change',()=>delaysCall({ref_group:ref.value||null}));
        const cr=host.querySelector('[data-act="create"]');
        if(cr) cr.addEventListener('click',()=>{
            const nm=(($('#mx-dl-newname')||{}).value||'').trim(), fr=parseInt(($('#mx-dl-newdelay')||{}).value,10);
            if(!nm){ showError(T('plugin.mixer.dl_name_required','Donnez un nom au groupe.')); return; }
            delaysCall({group:nm, frames:fr});
        });
    }

    async function refreshAtemChip(){
        const chip=$('#mx-atem-chip'); if(!chip) return;
        try {
            const r=await fetch('/api/atem/sessions'); if(!r.ok){chip.textContent=''; return;}
            const data=await r.json();
            const mine=(data.panels||[]).filter(p=>p.connected&&p.assigned_vmid===VMID);
            if(!mine.length){chip.innerHTML=''; return;}
            // Un pupitre physique n'a que les boutons de bus de son modèle : la topologie annonce
            // bien les N sources, mais au-delà de ses boutons il ne peut PAS les adresser. On le dit
            // plutôt que de laisser croire que les 20 entrées sont au pupitre.
            const n=(state&&state.n_inputs)||0;
            const tr=(k,fb)=>{const v=window.t?window.t(k):''; return (v&&v!==k)?v:fb;};
            const warn=n>8
                ? ` <span class="mx-atem-warn">${esc(tr('js.mixer.atem_limit',
                    "· n'adresse que les entrées couvertes par ses boutons de bus (8 sur la plupart des modèles)"))}</span>`
                : '';
            chip.innerHTML=`<span style="color:var(--status-running-fg)">● Pupitre${mine.length>1?'s':''} ATEM :</span> ${mine.map(p=>esc(p.label||p.ip)).join(', ')}${warn}`;
        } catch(e){chip.textContent='';}
    }

    function updateLive(s){
        if(view==='delays') renderDelays(s);
        const bar=$('#mx-state-bar'); if(bar) bar.textContent=`BGD A = #${s.pgm+1} · BGD B = #${s.pvw+1}${s.transition?T('plugin.mixer.transition_running',' · transition en cours'):''}`;
        const fresh=keyersFromState(s), freshD=Array.isArray(s.dsks)?s.dsks.map(layerOf):[];
        const freshL=fresh.concat(freshD);
        // n_keyers / n_dsk changé (redéployé) ou type / page d'une couche changé ailleurs (macro, pupitre,
        // autre poste) : la ligne compacte, les rangées et le panneau en dépendent → on refait la page.
        if(fresh.length!==keyers.length || freshD.length!==dsks.length || Array.isArray(s.dsks)!==dskEngine
           || freshL.map(k=>k.type+'|'+(k.url||'')).join(',')!==lastTypes){ renderEditor(s); return; }
        if(dveBusy()) dveKeepLocal(freshL);     // un geste de CE poste en cours : sa géométrie ne saute pas
        keyers=fresh; dsks=freshD; layers=keyers.concat(dsks); keyer=keyers[0]||defaultKeyer(); keyerRefreshActiveUi(); keyerStripRefresh();
        dveDraw(); dveKnobsSync();               // surface et rotatifs suivent l'état (macro, pupitre, autre poste)
        webStatusRefresh();
        busRefresh(s);
        const pr=JSON.stringify(s.keyer_presets||null);
        if(pr!==lastPresets){ lastPresets=pr; renderSideKeyer(); }
        // Sorties changées ailleurs (macro, autre poste, redéploiement) : la rubrique Sorties suit.
        if(JSON.stringify([s.outputs||null, s.pvw_base===undefined?0:s.pvw_base])!==lastOutputs){ outputsFromState(s); outputsRender(); }
        if(s.next_targets){ targets=targetsFromState(s.next_targets); targetsApplyToUi(); }
        dskEnabled=!!s.overlay_enabled; dskRefreshUi(); tbarSyncWithState(s);
    }
    function $$(sel){ return EL?EL.querySelector(sel):null; }

    // ─── Tally ────────────────────────────────────────────────────────────
    async function tallyCharger(){
        // Deux sources, toutes deux côté ORCHESTRATEUR : ce qui est enregistré pour ce
        // conteneur, et la liste des niveaux du site. Chargées une fois, gardées ensuite —
        // `renderEditor` peut rejouer, elles ne rebougeront pas sous les doigts.
        try {
            if (tallyCfg === null) {
                const r = await fetch(`/api/containers/${VMID}/plugin_config`);
                if (r.ok) tallyCfg = (await r.json()).params || {};
            }
            if (tallyNiveaux === null) {
                const r = await fetch('/api/tally/levels');
                if (r.ok) tallyNiveaux = (await r.json()).map(
                    n => ({value: n.uuid, label: `${n.num} — ${n.nom}`}));
            }
        } catch(e){ /* page utilisable sans : les réglages restent simplement vides */ }
    }

    function tallyListe(v){
        if (Array.isArray(v)) return v.filter(Boolean).map(String);
        return (v === null || v === undefined || v === '' || v === 0) ? [] : [String(v)];
    }

    async function tallyMonter(){
        await tallyCharger();
        if (!EL || !$('#mx-tally-levels')) return;      // la page a été refaite entre-temps
        const c = tallyCfg || {};
        const em = $$('#mx-tally-emit'), fo = $$('#mx-tally-force');
        // `tally_emit` est faux par défaut, `tally_force` VRAI : ne pas confondre « absent »
        // et « décoché », sinon l'écran contredit ce que fait le service TSL.
        if (em && !tallyEnVol.has('tally_emit'))  em.checked = !!c.tally_emit;
        if (fo && !tallyEnVol.has('tally_force')) fo.checked = (c.tally_force !== false);
        tallyMapRefresh();
        tallyVisibilite();
        const hote = $$('#mx-tally-levels');
        if (!hote || !window.MXLControls || tallyEnVol.has('tally_level_base')) return;
        tallyCtl = window.MXLControls.chooseList(hote, {
            options: tallyNiveaux || [],
            valeurs: tallyListe(c.tally_level_base),
            vide:    T('plugin.mixer.tally_vide',   '— ceux de la production —'),
            ajouter: T('plugin.mixer.tally_ajout',  '+ Ajouter un niveau…'),
            tout:    T('plugin.mixer.tally_tout',   'tous les niveaux sont choisis'),
            retirer: T('plugin.mixer.tally_retirer','Retirer'),
            onChange: v => tallySet('tally_level_base', v),
        });
    }

    function tallyVisibilite(){
        // Rien n'est émis si la case maîtresse est décochée : on masque plutôt que de laisser
        // deux réglages sans effet à l'écran.
        const on = !!(tallyCfg && tallyCfg.tally_emit);
        const r = $$('#mx-tally-force-row'), f = $$('#mx-tally-lv-field'), mp = $$('#mx-tally-map-field');
        if (r) r.hidden = !on;
        if (f) f.hidden = !on;
        if (mp) mp.hidden = !on;
    }

    // Couleur du tally PAR BUS (28/10, l'utilisateur : « un mélangeur qui alimente un écran : son
    // PGM en vert, rien sur son PVW ») — matrice bus × couleur, comme celle des sorties. Une ligne
    // par bus (un bus de plus = une entrée), au plus un point allumé par ligne ; aucun = « n'émet
    // rien ». Clés lues par l'orchestrateur (app/tally.py, COULEURS_BUS), à chaud.
    const MX_TALLY_BUSES=[{bus:'pgm', label:'PGM', key:'tally_pgm_color', def:'red',
                           sub:['plugin.mixer.tally_bus_pgm',"sources à l'image"]},
                          {bus:'pvw', label:'PVW', key:'tally_pvw_color', def:'green',
                           sub:['plugin.mixer.tally_bus_pvw','ce que montre le PVW']}];
    const MX_TALLY_COLORS=[{v:'red',   label:['plugin.mixer.tally_red','Rouge'], tone:'var(--status-stopped-fg)'},
                           {v:'green', label:['plugin.mixer.tally_green','Vert'], tone:'var(--status-running-fg)'}];
    function tallyBusColor(b){ const v=(tallyCfg||{})[b.key]; return ['red','green','off'].includes(v)?v:b.def; }
    function tallyMapHtml(){
        const th=MX_TALLY_COLORS.map(c=>`<th scope="col">${esc(T(...c.label))}</th>`).join('');
        const rows=MX_TALLY_BUSES.map(b=>`<tr><th scope="row"><span class="mx-xp-k">${esc(b.label)}</span><span class="mx-xp-sub">${esc(T(...b.sub))}</span></th>${
            MX_TALLY_COLORS.map(c=>`<td><button type="button" role="switch" class="ctl-push ctl-push--led ctl-push--small mx-xp" style="--ctl-tone:${c.tone}"
                data-tmap="${b.bus}" data-col="${c.v}" aria-pressed="${tallyBusColor(b)===c.v?'true':'false'}"
                aria-label="${esc(b.label+' → '+T(...c.label))}" onclick="__mx.tallyMap('${b.bus}','${c.v}')"><span class="ctl-led"></span></button></td>`).join('')}</tr>`).join('');
        return `<table class="mx-xp-table mx-tally-map"><thead><tr><th scope="col">${esc(T('plugin.mixer.tally_bus','Bus'))}</th>${th}</tr></thead><tbody>${rows}</tbody></table>`;
    }
    function tallyMapRefresh(){
        MX_TALLY_BUSES.forEach(b=>{ if(tallyEnVol.has(b.key)) return; const cur=tallyBusColor(b);
            MX_TALLY_COLORS.forEach(c=>{ const el=$$(`[data-tmap="${b.bus}"][data-col="${c.v}"]`); if(el) el.setAttribute('aria-pressed', cur===c.v?'true':'false'); }); });
    }
    function tallyMap(bus, col){
        const b=MX_TALLY_BUSES.find(x=>x.bus===bus); if(!b) return;
        const v = tallyBusColor(b)===col ? 'off' : col;      // rappuyer sur le point allumé l'éteint
        if (tallyCfg) tallyCfg[b.key]=v; else tallyCfg={[b.key]:v};
        MX_TALLY_COLORS.forEach(c=>{ const el=$$(`[data-tmap="${bus}"][data-col="${c.v}"]`); if(el) el.setAttribute('aria-pressed', v===c.v?'true':'false'); });
        tallySet(b.key, v);
    }

    function tallySet(cle, valeur){
        if (tallyCfg) tallyCfg[cle] = valeur;
        tallyVisibilite();
        tallyEnVol.add(cle);              // dès le PREMIER geste : plus rien ne redessine
        clearTimeout(tallyTimers[cle]);
        // Une liste s'édite par gestes SUCCESSIFS. Sans regroupement, trois niveaux choisis
        // coup sur coup partiraient en trois requêtes concurrentes.
        tallyTimers[cle] = setTimeout(() => tallyPoster(cle, valeur, 6), 400);
    }

    async function tallyPoster(cle, valeur, reste){
        try {
            const r = await fetch(`/api/containers/${VMID}/plugin_config`, {
                method:'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({params:{[cle]: valeur}}),
            });
            // 409 = un déploiement occupe la place. Ces trois clés n'en déclenchent aucun,
            // mais un autre réglage de la page peut en avoir un en vol : on réessaie.
            if (r.status === 409 && reste > 0){ setTimeout(() => tallyPoster(cle, valeur, reste-1), 700); return; }
            if (!r.ok){
                TOAST(T('plugin.mixer.tally_echec','Échec : HTTP {c}').replace('{c}', r.status));
                tallyCfg = null;          // l'écran doit remontrer ce qui est VRAIMENT enregistré
                tallyEnVol.delete(cle);
                tallyMonter();
                return;
            }
            TOAST(T('plugin.mixer.tally_ok','Réglage appliqué'));
            tallyEnVol.delete(cle);
        } catch(e){
            TOAST(T('plugin.mixer.tally_echec','Échec : HTTP {c}').replace('{c}', e.message));
            tallyEnVol.delete(cle);
        }
    }

    function keyerRefreshActiveUi(){ const btn=$('#mx-keyer-toggle-btn'); if(btn) btn.setAttribute('aria-pressed',keyer.active?'true':'false'); }
    function dskRefreshUi(){ const btn=$('#mx-dsk-toggle-btn'); if(btn) btn.setAttribute('aria-pressed',dskEnabled?'true':'false'); }

    function applyState(s, fullRebuild){
        state=s;
        if(fullRebuild||!$('.mx-bus-row')) renderEditor(state);
        else if(!userIsEditing()){ updateLive(state); presetSync(state); compLibSync(state); }
        refreshAtemChip();
        setupDecorate();
        syncDecorate(s);
        if(wizApi&&wizApi.live){ try{ wizApi.live(s); }catch(_){} }
    }
    // ─── État POUSSÉ (moteur ≥ 0.23.28) ─────────────────────────────────────────────────────
    // Le moteur pousse son /state à chaque changement sur GET /events (text/event-stream, relayé
    // par l'orchestrateur) : une commutation au pupitre ATEM apparaît ici aussitôt, au lieu
    // d'attendre l'interrogation de 2 s. L'interrogation CONTINUE (elle rafraîchit les mesures
    // d'arrivée, que le flux ne pousse pas) et prend le relais si le flux manque : moteur plus
    // ancien (404 → on n'insiste pas), ou flux coupé (nouvel essai dans 3 s).
    async function eventsStart(){
        if(VMID==null||!EL||eventsAbort) return;
        if(typeof AbortController!=='function'||typeof TextDecoder!=='function') return;   // interrogation seule
        const ctl=new AbortController(); eventsAbort=ctl;
        const vm=VMID;
        let retry=true;
        try {
            const r=await fetch(`/api/containers/${vm}/plugin/events`,{signal:ctl.signal,cache:'no-store'});
            if(!r.ok||!r.body){ if(r.status===404) retry=false; return; }
            const rd=r.body.getReader(), dec=new TextDecoder();
            let buf='';
            for(;;){
                const {value,done}=await rd.read();
                if(done) break;
                buf+=dec.decode(value,{stream:true});
                let i;
                while((i=buf.indexOf('\n\n'))>=0){
                    const ev=buf.slice(0,i); buf=buf.slice(i+2);
                    const data=ev.split('\n').filter(l=>l.startsWith('data: ')).map(l=>l.slice(6)).join('\n');
                    if(!data||VMID!==vm||!EL) continue;
                    try { applyState(JSON.parse(data), false); } catch(_){}
                }
            }
        } catch(_){ /* coupure ou abandon : l'interrogation continue */ }
        finally {
            if(eventsAbort===ctl) eventsAbort=null;
            if(retry&&!ctl.signal.aborted&&VMID===vm&&EL) eventsRetry=setTimeout(()=>{eventsRetry=null; eventsStart();},3000);
        }
    }
    function eventsStop(){
        if(eventsRetry){ clearTimeout(eventsRetry); eventsRetry=null; }
        if(eventsAbort){ const c=eventsAbort; eventsAbort=null; try{ c.abort(); }catch(_){} }
    }
    async function fetchState({fullRebuild=false}={}){
        if(VMID==null||!EL) return;
        try {
            const r=await fetch(`/api/containers/${VMID}/plugin/state`); if(!r.ok) throw new Error('HTTP '+r.status);
            applyState(await r.json(), fullRebuild);
        } catch(e){ if(EL){ EL.innerHTML=`<div style="color:var(--status-stopped-fg)">${esc(T('plugin.mixer.error','Erreur : {msg}').replace('{msg}', e.message))}</div>`; setupDecorate(); } }
    }

    // ─── Assistant de configuration (0.23.56) ─────────────────────────────────────────────────
    const canDo = (p) => typeof window.hasPerm === 'function' && !!window.hasPerm(p);
    const laterKey = () => 'mx-setup-later:' + (SCOPE || ('vmid' + VMID));
    async function setupRead(){
        if(VMID==null) return false;
        try{
            const r=await fetch(`/api/containers/${VMID}/config`,{cache:'no-store'});
            if(!r.ok) return false;
            const c=await r.json();
            const dc=(typeof c.deploy_config==='string')?JSON.parse(c.deploy_config||'{}'):(c.deploy_config||{});
            setupParams=(dc&&dc.params)||{}; setupHost=c.hostname||setupParams.hostname||'';
            const v=setupParams.setup_pending;
            setupPending=(v===true||v===1||v==='1'||v==='true');
            return true;
        }catch(_){ return false; }
    }
    // Première ouverture : l'assistant (droit de déployer), sinon « en attente de configuration ».
    async function setupCheck(){
        if(!(await setupRead())) return;
        if(!setupPending){ setupDecorate(); return; }
        let later=false; try{ later=sessionStorage.getItem(laterKey())==='1'; }catch(_){}
        if(canDo('containers.deploy') && !later) wizOpen(true);
        else setupDecorate();
    }
    // Racine de l'assistant et bandeau : remis en tête après chaque reconstruction de la page.
    function setupDecorate(){
        if(!EL) return;
        if(wizRoot){ if(wizRoot.parentNode!==EL) EL.prepend(wizRoot); EL.classList.add('mx-wiz-on'); }
        else EL.classList.remove('mx-wiz-on');
        const want=setupPending && !wizRoot;
        if(!want){ if(setupBanner&&setupBanner.parentNode) setupBanner.remove(); setupBanner=null; return; }
        if(!setupBanner){
            setupBanner=document.createElement('div');
            setupBanner.className='mx-setup-banner';
            setupBanner.setAttribute('role','status');
        }
        setupBanner.innerHTML=canDo('containers.deploy')
            ? `<span><b>${esc(T('plugin.mixer.wiz.banner_t','{h} n\'est pas configuré').replace('{h}',setupHost||('#'+VMID)))}</b> — ${esc(T('plugin.mixer.wiz.banner_x','il tourne avec les valeurs par défaut.'))}</span>
               <button type="button" class="ctl-push ctl-push--ok ctl-push--led" onclick="__mx.wizOpen()"><span class="ctl-led"></span>${esc(T('plugin.mixer.wiz.configure_now','Configurer maintenant'))}</button>`
            : `<span><b>${esc(T('plugin.mixer.wiz.waiting_t','Mélangeur en attente de configuration'))}</b> — ${esc(T('plugin.mixer.wiz.waiting_x','{h} n\'est pas encore configuré. Un utilisateur qui a le droit de déployer le prépare à sa première ouverture.').replace('{h}',setupHost||('#'+VMID)))}</span>`;
        setupBanner.classList.toggle('mx-setup-waiting', !canDo('containers.deploy'));
        if(setupBanner.parentNode!==EL) EL.prepend(setupBanner);
    }
    function wizLoad(){
        if(window.MXLMixerWizard) return Promise.resolve();
        if(wizLoadP) return wizLoadP;
        const v=(setupParams&&setupParams.plugin_version)||'';
        wizLoadP=new Promise(res=>{
            const sc=document.createElement('script');
            sc.src=`/api/plugins/mixer/ui/wizard_js?vmid=${VMID}`+(v?`&v=${encodeURIComponent(v)}`:'');
            sc.onload=()=>res(); sc.onerror=()=>{ wizLoadP=null; res(); };
            document.head.appendChild(sc);
        });
        return wizLoadP;
    }
    async function wizOpen(first){
        if(VMID==null||!EL||wizRoot) return;
        if(!canDo('containers.deploy')) return;
        await setupRead();
        await wizLoad();
        if(!window.MXLMixerWizard||!EL){ showError(T('plugin.mixer.wiz.load_err','Assistant indisponible.')); return; }
        wizRoot=document.createElement('div'); wizRoot.className='mxw-root';
        setupDecorate();
        wizApi=window.MXLMixerWizard.open(wizRoot,{vmid:VMID, hostname:setupHost||('mixer'+VMID), T, state, params:setupParams||{},
            first:first===undefined?setupPending:!!first, canSettings:canDo('settings.edit'), onClose:wizClosed});
    }
    function wizClosed(kind, info){
        wizApi=null;
        if(wizRoot&&wizRoot.parentNode) wizRoot.remove();
        wizRoot=null;
        if(kind==='later'){ try{ sessionStorage.setItem(laterKey(),'1'); }catch(_){} }
        if(kind==='applied'){
            setupPending=false;
            TOAST(info&&info.restart?T('plugin.mixer.wiz.applied_restart','Configuration appliquée — le mélangeur redémarre.')
                :T('plugin.mixer.wiz.applied_hot','Configuration appliquée, sans coupure.'),'info');
            if(info&&info.errors&&info.errors.length) showError(T('plugin.mixer.wiz.partial','Appliqué, sauf : ')+info.errors.join(' ; '));
        }
        setupDecorate();
        if(kind==='delays') view='delays';     // « Régler les groupes » : la page s'ouvre sur Entrées & délais
        fetchState({fullRebuild:true});
    }
    // ─── Sources non synchrones (0.23.57) ─────────────────────────────────────────────────────
    // Retour de Cyril (30/10) : « il faut être alerté dessus pour savoir que ça existe et que ça se
    // règle ». Le moteur signale dans /state.unsync_inputs les entrées SANS groupe en retard sur le
    // délai par défaut (hystérésis côté moteur : la liste ne change qu'à une transition). Bandeau en
    // tête de la vue d'exploitation, avec le chemin vers l'onglet Entrées & délais ; l'onglet a déjà
    // son propre avertissement, le bandeau s'y efface.
    let syncBanner=null;
    function syncDecorate(s){
        if(!EL) return;
        const us=(s&&Array.isArray(s.unsync_inputs))?s.unsync_inputs:[];
        if(!us.length||view==='delays'){ if(syncBanner&&syncBanner.parentNode) syncBanner.remove(); syncBanner=null; return; }
        if(!syncBanner){ syncBanner=document.createElement('div'); syncBanner.className='mx-sync-banner'; syncBanner.setAttribute('role','status'); }
        const names=(s.inputs||[]);
        const list=us.map(n=>'#'+n+(names[n-1]?' ('+names[n-1]+')':'')).join(', ');
        const html=`<span><b>${esc(T('plugin.mixer.sync_banner_t','Sources non synchrones ?'))}</b> — ${esc(T('plugin.mixer.sync_banner_x','{i} arrive(nt) après le délai par défaut et passe(nt) avec l\'image précédente. Rangez-les dans un groupe de délai.').replace('{i}',list))}</span>
            <button type="button" class="ctl-push ctl-push--warn" onclick="__mx.setView('delays')">${esc(T('plugin.mixer.sync_banner_go','Entrées & délais'))}</button>`;
        const key=list+'|'+(window.__lang||'');
        if(syncBanner.dataset.k!==key){ syncBanner.innerHTML=html; syncBanner.dataset.k=key; }   // pas de réécriture à chaque /state (geste en cours)
        const anchor=setupBanner&&setupBanner.parentNode===EL?setupBanner.nextSibling:EL.firstChild;
        if(syncBanner.parentNode!==EL) EL.insertBefore(syncBanner, anchor||null);
    }

    // ⚙ du shell (plugin_section) : actions propres à CE mélangeur, en tête du panneau.
    function settingsActions(inst){
        if(!canDo('containers.deploy')||(inst&&inst.vmid!==VMID)) return [];
        return [{label:T('plugin.mixer.wiz.gear','Assistant de configuration'), run:()=>wizOpen()}];
    }

    // ─── T-Bar ────────────────────────────────────────────
    function tbarRunDissolve(durationMs){ if(!tbarCtl) return; if(durationMs<=0){ tbarCtl.set(1); return; } tbarCtl.animateTo(1,durationMs); }
    function tbarSyncWithState(s){
        const cur=s.transition||null, wasIn=!!lastTransition, justEnded=wasIn&&!cur;
        // s.pgm!==s.pvw évite le faux-positif quand PGM==PVW (même source sur les 2 bus) :
        // sinon s.pgm===lastPvw && s.pvw===lastPgm sont trivialement vrais à chaque poll →
        // la T-bar oscille toute seule sans qu'aucun vrai swap n'ait eu lieu.
        const swapped=!cur&&!wasIn&&lastPgm!=null&&lastPvw!=null&&s.pgm===lastPvw&&s.pvw===lastPgm&&s.pgm!==s.pvw;
        const T_=tbarCtl, dragging=!!(T_&&T_.isDragging());
        if(T_&&cur&&!wasIn&&!T_.isAnimating()&&!dragging){ const serverStart=(cur.start||0)*1000, elapsed=Math.max(0,Date.now()-serverStart), remaining=Math.max(0,cur.duration_ms-elapsed); T_.set(Math.min(1,elapsed/cur.duration_ms)); if(remaining>0) T_.animateTo(1,remaining); else T_.set(1); }
        // Fin de transition (ou échange PGM/PVW par CUT) : la butée change ; la poignée, restée où elle
        // était, rejoint la nouvelle butée (avancement 0).
        if(T_&&(justEnded||swapped)&&!dragging){ T_.setRest(T_.getRest()===0?1:0); T_.get()>0.01?T_.animateTo(0,220,t=>1-Math.pow(1-t,4)):T_.set(0); }
        lastPgm=s.pgm; lastPvw=s.pvw; lastTransition=cur;
    }

    function sendMix(alpha){ if(VMID==null) return; mixLatestAlpha=alpha; if(mixSendPending) return; mixSendPending=true; requestAnimationFrame(()=>{ const a=mixLatestAlpha; mixSendPending=false; fetch(`/api/containers/${VMID}/plugin/mix`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({alpha:a})}).catch(()=>{}); }); }

    // ─── Preset / targets ─────────────────────────────────
    // Motif et sens (bloc transition) et douceur (panneau) n'ont de sens qu'en Wipe : sinon masqués.
    function wipeVisibility(mode){
        const h=$('#mx-wipe-hint');
        $all('.mx-wipe-controls').forEach(c=>{ if(mode==='wipe') c.removeAttribute('hidden'); else c.setAttribute('hidden',''); });
        if(h) h.hidden = mode==='wipe';
    }
    function presetApplyToUi(){
        targetsApplyToUi();
        $all('[data-mode]').forEach(b=>{const a=b.dataset.mode===preset.mode; b.classList.toggle('active',a); b.setAttribute('aria-checked',a?'true':'false');});
        wipeVisibility(preset.mode);
        wipeIconsLight();
        const dm=$('#mx-dirmodes'); if(dm) dm.innerHTML=dirModesHtml(preset.wipe.reverse,preset.wipe.alternate,m=>`data-dirmode="${m}" onclick="__mx.dirModeSet('${m}')"`);
        Object.keys(wipeKnobs).forEach(f=>{ try{ wipeKnobs[f].set(preset.wipe[f]); }catch(_){} });
        const sy=$('#mx-wipe-sym'); if(sy) sy.setAttribute('aria-pressed',preset.wipe.symmetry?'true':'false');
        const bc=$('#mx-wipe-bcolor'); if(bc&&!userIsEditing()) bc.value=String(preset.wipe.border_color||'#ffffff').toLowerCase();
    }
    function targetOn(key){ return key==='bgd' ? !!targets.bgd : !!targets.keyers[parseInt(key.slice(1),10)]; }
    function targetsApplyToUi(){ $all('[data-target]').forEach(btn=>{const a=targetOn(btn.dataset.target); btn.classList.toggle('active',a); btn.setAttribute('aria-pressed',a?'true':'false');}); }
    function wipeIconsLight(){
        const w=preset.wipe;
        $all('.ctl-shape[data-direction]').forEach(b=>{const a=!preset.composite&&shapeOn(b.dataset.direction,w.direction,w.angle); b.setAttribute('aria-checked',a?'true':'false');});
        const ch=$('#mx-comp-chip');
        if(ch){ ch.hidden=!preset.composite; ch.setAttribute('aria-pressed',preset.composite?'true':'false');
                const lb=ch.querySelector('[data-chip-name]'); if(lb) lb.textContent=T('plugin.mixer.comp_chip','Composé : {name}').replace('{name}',preset.composite||''); }
    }
    async function presetChange(){
        // Réglage COMPLET (0.23.44) : un champ omis garderait sa valeur, mais un ancien nom envoyé seul
        // remettrait l'angle à sa valeur prédéfinie.
        const w=preset.wipe, wipe={};
        Object.keys(WIPE_DEF).forEach(f=>{ wipe[f]=(w[f]!=null)?w[f]:WIPE_DEF[f]; });
        wipe.softness=Math.max(0,Math.min(1,wipe.softness)); wipe.reverse=!!wipe.reverse; wipe.alternate=!!wipe.alternate; wipe.symmetry=!!wipe.symmetry;
        // Volet composé (0.23.47) : renvoyé tel quel — la forme envoyée dans `wipe` l'effacerait sinon.
        const payload={mode:preset.mode||'dissolve', wipe, composite:preset.composite||null};
        if(VMID==null) return;
        try{const r=await fetch(`/api/containers/${VMID}/plugin/preset`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}); if(!r.ok) throw new Error('HTTP '+r.status); clearError();}
        catch(e){showError("Preset : "+e.message);}
    }
    function targetToggle(key){
        if(key==='bgd') targets.bgd=!targets.bgd;
        else { const n=parseInt(key.slice(1),10); targets.keyers[n]=!targets.keyers[n]; }
        if(!targets.bgd&&!targets.keyers.some(Boolean)) targets.bgd=true;
        targetsApplyToUi(); if(VMID==null) return;
        // `keyer` (keyer 1) en plus de `keyers` : un moteur d'avant 0.23.14 ne lit que lui.
        fetch(`/api/containers/${VMID}/plugin/targets`,{method:'POST',headers:{'Content-Type':'application/json'},
            body:JSON.stringify({bgd:targets.bgd, keyer:!!targets.keyers[0], keyers:targets.keyers})}).catch(()=>{});
    }
    function modeIconClick(mode){ preset.mode=mode; $all('[data-mode]').forEach(b=>{const a=b.dataset.mode===mode; b.classList.toggle('active',a); b.setAttribute('aria-checked',a?'true':'false');}); wipeVisibility(mode); presetChange(); }
    function wipeIconClick(dir){
        preset.composite=null;      // une forme choisie remplace le volet composé
        if(dir in MX_LEGACY_ANGLES){ preset.wipe.direction='linear'; preset.wipe.angle=MX_LEGACY_ANGLES[dir]; }
        else preset.wipe.direction=dir;
        presetApplyToUi(); presetChange();
    }
    function presetFromState(p){
        const w=p.wipe||{}, out={};
        Object.keys(WIPE_DEF).forEach(f=>{ out[f]=(w[f]!=null)?w[f]:WIPE_DEF[f]; });
        // Moteur d'avant 0.23.44 : un ancien nom sans angle → linéaire à son angle.
        if(out.direction in MX_LEGACY_ANGLES){ out.angle=MX_LEGACY_ANGLES[out.direction]; out.direction='linear'; }
        out.reverse=!!out.reverse; out.alternate=!!out.alternate; out.symmetry=!!out.symmetry;
        return {mode:p.mode||'dissolve', wipe:out, composite:p.composite||null};
    }
    // Le moteur retourne le sens en N/R à la fin d'un volet : l'interface le relit à chaque état.
    // 0.23.44 : tout le réglage du volet est relu (macro, pupitre, autre poste), sauf pendant un geste
    // sur un rotatif de ce poste (et 1,5 s après) — sinon le rotatif sauterait sous le doigt.
    function presetSync(s){
        if(!s||!s.preset||!s.preset.wipe) return;
        const nw=presetFromState(s.preset);
        if(Date.now()<wipeEditUntil){
            if(nw.wipe.reverse===preset.wipe.reverse&&nw.wipe.alternate===preset.wipe.alternate) return;
            preset.wipe.reverse=nw.wipe.reverse; preset.wipe.alternate=nw.wipe.alternate; presetApplyToUi(); return;
        }
        if(JSON.stringify(nw)!==JSON.stringify(preset)){ preset=nw; presetApplyToUi(); }
    }
    function dirModeSet(m){
        if(m==='norm'){ preset.wipe.reverse=false; preset.wipe.alternate=false; }
        else if(m==='rev'){ preset.wipe.reverse=true; preset.wipe.alternate=false; }
        else preset.wipe.alternate=true;          // N/R : part du sens courant
        presetApplyToUi(); presetChange();
    }
    function reverseToggle(){ preset.wipe.reverse=!preset.wipe.reverse; preset.wipe.alternate=false; presetApplyToUi(); presetChange(); }

    // ─── Knobs Keyer ──────────────────────────────────────
    function keyerFmtField(f,v){
        if(f==='border_width') return Math.round(v)+'px';
        if(f==='auto_ms') return fmtDur(v);
        if(['left','right','top','bottom','clip','auto_soft','chroma_tol','chroma_soft','chroma_spill','pattern_size','pattern_soft',
            'pattern_border_soft_in','pattern_border_soft_out'].includes(f)) return Math.round(v*100)+'%';
        if(f==='pattern_border') return Math.round(v*1000)/10+'%';
        if(f==='pattern_angle') return Math.round(v)+'°';
        if(f==='pattern_repeat') return String(Math.round(v));
        if(f==='gain') return v.toFixed(1);
        return v.toFixed(2);
    }
    // Rotatifs des keyers montés (0.23.65) : {c, el, i, g, f}. Remontés à chaque rendu du panneau.
    let keyerKnobs=[];
    function kSrc(i,g){ const k=layers[i]; return k?(g==='mask'?k.mask:k):null; }
    function keyerKnobsMount(root){
        keyerKnobs.forEach(x=>{ try{ x.c.destroy(); }catch(_){} }); keyerKnobs=[];
        const r=root||EL; if(!r||!window.BobiControls) return;
        r.querySelectorAll('.mx-kknob[data-knob]').forEach(el=>{
            const d=el.dataset, i=parseInt(d.k||'0',10), g=d.group||'transform', f=d.knob, src=kSrc(i,g); if(!src) return;
            const min=parseFloat(d.min), max=parseFloat(d.max), step=parseFloat(d.step), def=parseFloat(d.default);
            const c=window.BobiControls.create('knob', el, {label:d.label, min, max, step, default:def,
                value:(src[f]!=null?src[f]:def), format:v=>keyerFmtField(f,v), integer:step>=1});
            // Pendant le geste : valeur locale (la surface DVE suit) ; au relâcher / cran / défaut : envoi.
            const put=v=>{ const o=kSrc(i,g); if(!o) return false; o[f]=v; if(g==='transform') dveDraw(); return true; };
            c.on('input', v=>{ put(v); });
            c.on('change', v=>{ if(put(v)) keyerKnobCommit(i,g); });
            keyerKnobs.push({c, el, i, g, f});
        });
    }
    function keyerKnobDragging(){ return keyerKnobs.some(x=>x.el.classList&&x.el.classList.contains('ctl-dragging')); }

    // ─── SURFACE DVE (0.23.61) : la base commune des éditeurs (BobiEditor.surface) ──────────────
    // Décision de Cyril du 30/10 : un seul éditeur, les mêmes gestes partout. Chaque couche DVE
    // (keyers et DSK) est un rectangle d'un cadre au format de la sortie. Sélection (Maj ou Ctrl/⌘,
    // référence = DERNIER touché, qui devient la couche affichée au panneau), glisser, poignée au
    // format de la source (Alt = autour du centre), aimant, flèches, aligner / même taille /
    // distribuer, annuler/rétablir, copier/coller : TOUT vient de la base. Ne restent ici que ce qui
    // est propre au mélangeur : la conversion d'unités, le dessin, l'envoi au moteur.
    //
    // CORRESPONDANCE MOTEUR (script.py, _sl_keyer_geom ; bornes _KEYER_NUM) :
    //   (x, y) = CENTRE de l'image placée, en fraction de la sortie (cx = x·W, cy = y·H) ;
    //   scale  = largeur/W ET hauteur/H : l'image garde le format de la sortie, qui est celui de sa
    //            source (le moteur la réduit de W×H à scale·W × scale·H) ;
    //   bornes : x, y ∈ [0, 1] (le centre reste dans le cadre, l'image peut en sortir de MOITIÉ),
    //            scale ∈ [0.05, 1] ; le moteur arrondit ensuite au pixel pair (chroma), rien à imiter ;
    //   masque : rogne l'image placée SANS la déplacer — dessiné, jamais converti.
    // Unités de la surface = PIXELS DE SORTIE (flèche = 1 px, Maj = 10) :
    //   rect = {x:(x−s/2)·W, y:(y−s/2)·H, w:s·W, h:s·H}  ⇄  x = (rx+rw/2)/W, y = (ry+rh/2)/H, s = rw/W.
    // Envoi : aperçu PENDANT le geste (POST /keyer/transform, au plus un par couche toutes les 100 ms),
    // puis validation EN LOT au relâcher (un POST par couche touchée, même endpoint que les rotatifs,
    // persisté par le proxy de l'orchestrateur : rien n'est perdu au redéploiement).
    const DVE_SMIN=0.05, DVE_SMAX=1, DVE_MARGIN=0.25, DVE_LIVE_MS=100;
    let _dve=null, dveDrag=null, dveEditUntil=0, dveTimers={}, dveSnap=true, dveUnbind=null;
    const r4=v=>Math.round(v*1e4)/1e4;
    const clampN=(v,a,b)=>Math.max(a,Math.min(b,v));
    // Format de SORTIE du mélangeur (paramètres du conteneur ; défauts du moteur : 1280×720).
    function dveOut(){
        const p=setupParams||{};
        let W=parseInt(p.width,10)||1280, H=parseInt(p.height,10)||720;
        W-=W%2; H-=H%2;
        return {W,H};
    }
    function dveRect(k){ const {W,H}=dveOut(), s=k.scale; return {x:(k.x-s/2)*W, y:(k.y-s/2)*H, w:s*W, h:s*H}; }
    function dveParams(r){
        const {W,H}=dveOut();
        return {x:r4(clampN((r.x+r.w/2)/W,0,1)), y:r4(clampN((r.y+r.h/2)/H,0,1)), scale:r4(clampN(r.w/W,DVE_SMIN,DVE_SMAX))};
    }
    const isDve=k=>!!k&&k.type==='dve';
    function dveIdx(){ return layers.map((k,i)=>isDve(k)?i:-1).filter(i=>i>=0); }
    // Couche affichée au panneau (rubrique Keyers ou DSK).
    function dveShown(){
        if(sideSecCur()==='dsk'&&dsks.length) return keyers.length+Math.min(sideDsk,dsks.length-1);
        return Math.min(sideKeyer,Math.max(0,keyers.length-1));
    }
    function dveBusy(){ return !!dveDrag||keyerKnobDragging()||Date.now()<dveEditUntil; }
    function dveKeepLocal(fresh){
        fresh.forEach((f,i)=>{ const o=layers[i]; if(isDve(f)&&isDve(o)){ f.x=o.x; f.y=o.y; f.scale=o.scale; } });
    }
    function dveSurf(){
        if(_dve||!window.BobiEditor||!window.BobiEditor.surface) return _dve;
        _dve=window.BobiEditor.surface({
            objets:()=>{ const {W,H}=dveOut();
                return layers.map(k=>isDve(k)?Object.assign(dveRect(k),{format:W/H, min:DVE_SMIN*H, maxW:DVE_SMAX*W, maxH:DVE_SMAX*H}):null); },
            cadre:()=>{ const {W,H}=dveOut(); return {x:0,y:0,w:W,h:H}; },
            ecrire:(i,r)=>{ const k=layers[i]; if(!isDve(k)) return; Object.assign(k,dveParams(r)); if(dveDrag) dveLive(i); },
            rendu:()=>{ dveDraw(); dveKnobsSync(); },
            valider:idx=>{ dveCommit(idx); },
        },{
            horsCadre:true,
            bornesCentre:()=>{ const {W,H}=dveOut(); return {x0:0,x1:W,y0:0,y1:H}; },
            tol:()=>8*dvePxScale(), aimant:()=>dveSnap,
            pas:{fleche:1, maj:10}, rectangle:true,
        });
        return _dve;
    }
    // Envoi au moteur : la géométrie d'une couche, par l'endpoint des rotatifs (keyer ou DSK).
    function dveSend(i){
        const k=layers[i]; if(!k) return Promise.resolve(false);
        return keyerPost('transform',{keyer:i+1, x:k.x, y:k.y, scale:k.scale, border_width:k.border_width});
    }
    function dveLive(i){
        dveEditUntil=Date.now()+1500;
        if(dveTimers[i]) return;
        dveTimers[i]=setTimeout(()=>{ dveTimers[i]=null; dveSend(i); },DVE_LIVE_MS);
    }
    function dveCommit(idx){
        dveEditUntil=Date.now()+1500;
        idx.forEach(i=>{ if(dveTimers[i]){ clearTimeout(dveTimers[i]); dveTimers[i]=null; } });
        const lot=Promise.all(idx.map(dveSend));
        if(_dve) _dve.memoriser();
        return lot;
    }
    // Les rotatifs x / y / taille suivent la surface et l'état (sauf celui qu'on tourne).
    function dveKnobsSync(){
        keyerKnobs.forEach(({c,i,g,f})=>{
            if(g!=='transform'||(f!=='x'&&f!=='y'&&f!=='scale')) return;
            const k=layers[i]; if(!k||k[f]==null) return;
            c.set(k[f]);                  // n'émet rien, et ne touche pas un rotatif sous le doigt
        });
    }
    // La sélection de la surface suit la couche affichée au panneau (référence = elle).
    function dveSync(){
        const S=_dve, i=dveShown(); if(!S||!isDve(layers[i])||S.primaire===i) return;
        if(S.sel.includes(i)) S.primaire=i; else { S.sel=[i]; S.primaire=i; }
    }
    // …et le panneau suit la référence de la surface (après le geste, jamais pendant).
    function dveFollow(){
        const S=_dve; if(!S||S.primaire==null||S.primaire===dveShown()) return;
        sideEditKeyer(S.primaire);
    }
    function dveSurfaceHtml(){
        if(!layers.some(isDve)) return '';
        const {W,H}=dveOut();
        return `<div class="mx-dve" role="group" aria-label="${esc(T('plugin.mixer.dve_title','Placement DVE'))}">
            <span class="mx-preset-section-label">${esc(T('plugin.mixer.dve_title','Placement DVE'))}</span>
            <div class="ctl-toolbar mx-dve-tools"></div>
            <canvas class="mx-dve-cv" tabindex="0" style="aspect-ratio:${W}/${H}" aria-label="${esc(T('plugin.mixer.dve_aria','Placement des couches DVE dans l\'image de sortie'))}"></canvas>
            <p class="meta mx-dve-hint">${esc(T('plugin.mixer.dve_hint','Toucher un DVE pour le régler · Maj ou Ctrl+clic (ou « Sélection multiple ») : plusieurs, la référence est le dernier touché · coin bas-droit : taille au format de la source (Alt ou « Depuis le centre » : autour du centre) · flèches : 1 pixel de la sortie (Maj : 10).'))}</p>
        </div>`;
    }
    function dveMount(){
        const S=dveSurf(); if(!S) return;
        const {W,H}=dveOut();
        $all('.mx-dve').forEach(host=>{
            const cv=host.querySelector('.mx-dve-cv');
            if(cv&&!cv._dveBound){
                cv._dveBound=true;
                cv.addEventListener('pointerdown',dveDown); cv.addEventListener('pointermove',dveMove);
                cv.addEventListener('pointerup',dveUp); cv.addEventListener('pointercancel',dveUp);
                const w=Math.max(240,Math.round((cv.clientWidth||360)*((typeof window!=='undefined'&&window.devicePixelRatio)||1)));
                cv.width=w; cv.height=Math.round(w*H/W);
            }
            const tb=host.querySelector('.mx-dve-tools');
            if(tb&&!tb._dveBound){ tb._dveBound=true; dveToolbar(tb); }
        });
        if(!dveUnbind) dveUnbind=dveKeys();
        dveSync(); dveDraw();
    }
    function dveReset(){
        if(dveUnbind){ try{ dveUnbind(); }catch(_){} dveUnbind=null; }
        Object.keys(dveTimers).forEach(i=>{ if(dveTimers[i]) clearTimeout(dveTimers[i]); });
        dveTimers={}; _dve=null; dveDrag=null; dveEditUntil=0;
    }
    // Cadre dans le canevas : marge autour (une image peut sortir de moitié), même format.
    function dveView(cv){
        const {W,H}=dveOut(), FW=cv.width/(1+2*DVE_MARGIN), FH=FW*H/W;
        return {MX:DVE_MARGIN*FW, MY:(cv.height-FH)/2, FW, FH, k:FW/W};
    }
    // Pixels de sortie par pixel de canevas (tolérances de l'aimant et des poignées).
    function dvePxScale(cv){
        cv=cv||(EL&&EL.querySelector('.mx-dve-cv'));
        return (cv&&cv.width)?1/dveView(cv).k:1;
    }
    function dvePos(e){
        const cv=e.currentTarget||e.target, b=cv.getBoundingClientRect(), v=dveView(cv);
        const px=(e.clientX-b.left)*cv.width/(b.width||cv.width), py=(e.clientY-b.top)*cv.height/(b.height||cv.height);
        return {cv, x:(px-v.MX)/v.k, y:(py-v.MY)/v.k};
    }
    // Touché : la couche du dessus d'abord (DSK au-dessus des keyers, keyer N au-dessus de N−1) ;
    // la poignée = coin bas-droit, plus large au doigt.
    function dveHit(x,y,touch,cv){
        const tol=(touch?22:10)*((typeof window!=='undefined'&&window.devicePixelRatio)||1)*dvePxScale(cv);
        // La poignée de la RÉFÉRENCE passe avant tout : elle reste saisissable sous une autre couche.
        const p=_dve&&_dve.primaire;
        if(p!=null&&isDve(layers[p])){ const r=dveRect(layers[p]); if(Math.abs(x-(r.x+r.w))<=tol&&Math.abs(y-(r.y+r.h))<=tol) return {i:p, poignee:true}; }
        for(let i=layers.length-1;i>=0;i--){
            if(!isDve(layers[i])) continue;
            const r=dveRect(layers[i]);
            if(Math.abs(x-(r.x+r.w))<=tol&&Math.abs(y-(r.y+r.h))<=tol) return {i, poignee:true};
            if(x>=r.x&&x<=r.x+r.w&&y>=r.y&&y<=r.y+r.h) return {i, poignee:false};
        }
        return null;
    }
    function dveDown(e){
        if(e.button!==undefined&&e.button!==0) return;
        const S=dveSurf(); if(!S) return;
        const p=dvePos(e), h=dveHit(p.x,p.y,e.pointerType==='touch',p.cv);
        try{ p.cv.focus(); }catch(_){}
        // Poignée d'un DVE qui n'est pas la référence : il le devient (comme un clic).
        if(h&&h.poignee&&S.primaire!==h.i){
            S.selectionner(h.i, S.sel.includes(h.i)?{shiftKey:false}:e);
            if(S.primaire!==h.i) S.selectionner(h.i,{});
        }
        const geste=S.appui({x:p.x,y:p.y}, h, e);
        dveDraw();
        if(!geste){ dveFollow(); return; }
        dveDrag={i:h?h.i:null};
        if(e.preventDefault) e.preventDefault();
        try{ p.cv.setPointerCapture(e.pointerId); }catch(_){}
    }
    function dveMove(e){
        if(!dveDrag){
            const cv=e.currentTarget||e.target;
            if(cv&&cv.style&&cv.getBoundingClientRect){ const p=dvePos(e), h=dveHit(p.x,p.y,e.pointerType==='touch',cv);
                cv.style.cursor=h?(h.poignee?'nwse-resize':'move'):'default'; }
            return;
        }
        const p=dvePos(e);
        dveSurf().glisse({x:p.x,y:p.y}, e);
    }
    function dveUp(){
        if(!dveDrag) return;
        dveDrag=null;
        dveSurf().relache();       // la base valide : lot de POST + historique
        dveDraw(); dveFollow();
    }
    function dveTool(nom,arg){
        const S=dveSurf(); if(!S) return null;
        const r=S.outil(nom,arg);
        if(r===null){
            const n=nom==='distribuer'?3:2;
            if(S.sel.length<n) TOAST(n===3?T('editor.select3','Sélectionnez au moins 3 éléments (Maj ou Ctrl+clic).')
                                         :T('editor.select2','Sélectionnez au moins 2 éléments (Maj ou Ctrl+clic).'),'warning');
        }
        dveDraw();
        return r;
    }
    function dveToolbar(tb){
        const C=window.MXLControls, E=window.BobiEditor, S=dveSurf();
        if(!C||!C.toolGroup||!S) return;
        const SVG=C.ICONS||{};
        tb.appendChild(C.toolGroup(T('plugin.mixer.dve_group_align','Aligner'), [
            ['align_left','dve_align_left','Aligner à gauche','left'], ['align_hcenter','dve_align_hcenter','Centrer horizontalement','hcenter'],
            ['align_right','dve_align_right','Aligner à droite','right'], ['align_top','dve_align_top','Aligner en haut','top'],
            ['align_vcenter','dve_align_vcenter','Centrer verticalement','vcenter'], ['align_bottom','dve_align_bottom','Aligner en bas','bottom'],
        ].map(([ic,k,fb,m])=>[SVG[ic], T('plugin.mixer.'+k,fb), ()=>dveTool('aligner',m)])));
        tb.appendChild(C.toolGroup(T('plugin.mixer.dve_group_size','Taille'), [
            ['size_w','dve_size_w','Même largeur que la référence','w'], ['size_h','dve_size_h','Même hauteur que la référence','h'],
            ['size_both','dve_size_both','Même taille que la référence','both'],
        ].map(([ic,k,fb,m])=>[SVG[ic], T('plugin.mixer.'+k,fb), ()=>dveTool('egaliser',m)])));
        tb.appendChild(C.toolGroup(T('plugin.mixer.dve_group_distribute','Distribuer'), [
            ['distribute_h','dve_distribute_h','Distribuer horizontalement (3 minimum)','h'], ['distribute_v','dve_distribute_v','Distribuer verticalement (3 minimum)','v'],
        ].map(([ic,k,fb,m])=>[SVG[ic], T('plugin.mixer.'+k,fb), ()=>dveTool('distribuer',m)])));
        // Édition : les actions COMMUNES, les mêmes que le clavier — le seul chemin au doigt.
        if(E&&E.actionsEdition) tb.appendChild(C.toolGroup(T('editor.group_edit','Édition'),
            E.actionsEdition().map(a=>[SVG[a.ico], a.label, ()=>{ S.agir(a.action); dveDraw(); }])));
        // Verrous AU DOIGT (équivalents de Maj et d'Alt, portés par la base) et aimant.
        const row=document.createElement('div');
        row.className='mx-dve-locks';
        row.innerHTML=E.verrousHtml(S)
            +`<label class="toggle-inline" title="${esc(T('plugin.mixer.dve_snap_tip','Aimante aux bords et au centre du cadre et aux autres DVE ; les flèches le contournent.'))}">
                <input type="checkbox" class="ctl-switch" data-dvesnap ${dveSnap?'checked':''}><span>${esc(T('plugin.mixer.dve_snap','Aimant'))}</span></label>`;
        E.lierVerrous(row,S);
        const sn=row.querySelector('[data-dvesnap]');
        if(sn) sn.addEventListener('change',()=>{ dveSnap=!!sn.checked; $all('[data-dvesnap]').forEach(x=>{ x.checked=dveSnap; }); });
        tb.appendChild(row);
    }
    // Clavier et historique : ceux de la base (surface.clavier). Actif quand le focus est dans la
    // surface. Copier/coller = les RÉGLAGES du keyer (kind bobi.mixer.keyer, le même que « Copier les
    // réglages ») ; l'historique porte les réglages complets des couches (un collage s'annule aussi).
    function dveKeys(){
        const S=dveSurf(); if(!S||typeof document==='undefined'||!document.addEventListener) return null;
        return S.clavier({
            actif:()=>{ const ae=document.activeElement; return !!(EL&&ae&&ae.closest&&ae.closest('.mx-dve')&&EL.contains(ae)); },
            kind:CLIP_KIND, profondeur:100,
            etat:()=>layers.map(k=>k?keyerSettingsOf(k):null),
            restaurer:st=>dveRestore(st),
            fragment:()=>{ const i=S.primaire; if(i==null||!layers[i]) return null;
                           keyerCopy(i); return {v:1, settings:keyerSettingsOf(layers[i]), from:kName(i)}; },
            poser:d=>dvePaste(d),
            tout:()=>{ S.sel=dveIdx(); S.primaire=S.sel.length?S.sel[S.sel.length-1]:null; },
            apres:()=>{ dveSync(); dveDraw(); dveKnobsSync(); },
            titreAide:T('plugin.mixer.dve_keys','Raccourcis du placement DVE'),
        });
    }
    function dveApply(k,settings){
        KEYER_COPY_FIELDS.forEach(f=>{ if(f in settings) k[f]=settings[f]; });
        if(settings.mask) k.mask=Object.assign({},settings.mask);
    }
    function dveRestore(st){
        let retype=false;
        const lot=[];
        (st||[]).forEach((d,i)=>{
            const k=layers[i]; if(!k||!d) return;
            if(JSON.stringify(keyerSettingsOf(k))===JSON.stringify(d)) return;
            if(d.type!==k.type) retype=true;
            dveApply(k,d);
            lot.push(keyerPostJson('settings',{keyer:i+1, settings:d}));
        });
        if(retype) Promise.all(lot).then(()=>fetchState({fullRebuild:true}));
    }
    function dvePaste(d){
        const clip=clipOf({data:d}); const S=_dve;
        if(!clip||!S||!S.sel.length) return false;
        const settings={}; KEYER_COPY_FIELDS.forEach(f=>{ if(f in clip.settings) settings[f]=clip.settings[f]; });
        if(clip.settings.mask) settings.mask=clip.settings.mask;
        const lot=S.sel.map(i=>{ dveApply(layers[i],settings); return keyerPostJson('settings',{keyer:i+1, settings}); });
        Promise.all(lot).then(()=>fetchState({fullRebuild:true}));
        TOAST(T('plugin.mixer.k_pasted','Réglages collés sur {name}').replace('{name}', S.sel.map(kName).join(', ')));
        return true;
    }
    function dveColor(cv,name,fb){
        try{ const v=getComputedStyle(cv).getPropertyValue(name).trim(); return v||fb; }catch(_){ return fb; }
    }
    function dveDraw(){
        if(!EL) return;
        const S=_dve;
        $all('.mx-dve-cv').forEach(cv=>{
            const g=cv.getContext&&cv.getContext('2d'); if(!g) return;
            const v=dveView(cv), k=v.k;
            const acc=dveColor(cv,'--accent','#4c8dff'), mut=dveColor(cv,'--text-muted','#8a8f98'),
                  bg=dveColor(cv,'--canvas-bg',dveColor(cv,'--bg','#111')), bd=dveColor(cv,'--border','#555'),
                  txt=dveColor(cv,'--text-strong','#fff');
            g.clearRect(0,0,cv.width,cv.height);
            g.fillStyle=bg; g.fillRect(v.MX,v.MY,v.FW,v.FH);
            g.strokeStyle=bd; g.lineWidth=1; g.setLineDash([]); g.strokeRect(v.MX+0.5,v.MY+0.5,v.FW-1,v.FH-1);
            const sel=S?S.sel:[], prim=S&&S.primaire!=null?S.primaire:dveShown();
            const fs=Math.max(11,Math.round(cv.width/40));
            g.font=`${fs}px system-ui, sans-serif`; g.textBaseline='top';
            layers.forEach((L,i)=>{
                if(!isDve(L)) return;
                const r=dveRect(L), x=v.MX+r.x*k, y=v.MY+r.y*k, w=r.w*k, h=r.h*k;
                const on=sel.includes(i), act=i===prim, m=L.mask||{};
                g.globalAlpha=act?0.30:on?0.20:0.10; g.fillStyle=act||on?acc:mut; g.fillRect(x,y,w,h);
                // masque : la part retirée de l'image placée (elle ne bouge pas)
                if(m.left||m.right||m.top||m.bottom){
                    g.globalAlpha=0.45; g.fillStyle=bg;
                    g.fillRect(x,y,w*(m.left||0),h); g.fillRect(x+w*(1-(m.right||0)),y,w*(m.right||0),h);
                    g.fillRect(x,y,w,h*(m.top||0)); g.fillRect(x,y+h*(1-(m.bottom||0)),w,h*(m.bottom||0));
                }
                g.globalAlpha=1;
                g.strokeStyle=act||on?acc:mut; g.lineWidth=act?3:on?2:1; g.setLineDash(act||on?[]:[4,3]);
                g.strokeRect(x,y,w,h); g.setLineDash([]);
                g.fillStyle=act?acc:txt; g.fillText(kName(i)+(L.active?' ●':''),x+4,y+3);
                if(act){ const hs=Math.max(8,Math.round(cv.width/60)); g.fillStyle=acc; g.fillRect(x+w-hs,y+h-hs,hs,hs); }
            });
            const rs=S&&S.rectSel;          // rectangle de sélection en cours (unités de sortie)
            if(rs){ g.save(); g.strokeStyle=acc; g.lineWidth=1; g.setLineDash([4,3]);
                    g.strokeRect(v.MX+rs.x*k+0.5,v.MY+rs.y*k+0.5,rs.w*k,rs.h*k); g.restore(); }
            (S&&S.guides||[]).forEach(gd=>{
                g.strokeStyle=acc; g.lineWidth=1; g.setLineDash([3,3]); g.beginPath();
                if(gd.axe==='v'){ const X=v.MX+gd.pos*k; g.moveTo(X,0); g.lineTo(X,cv.height); }
                else { const Y=v.MY+gd.pos*k; g.moveTo(0,Y); g.lineTo(cv.width,Y); }
                g.stroke(); g.setLineDash([]);
            });
        });
    }

    // ─── Actions ──────────────────────────────────────────
    async function mxCall(path,body,{fullRebuild=true}={}){
        try{ const r=await fetch(`/api/containers/${VMID}/plugin/${path}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})}); if(!r.ok) throw new Error('HTTP '+r.status); clearError(); }
        catch(e){ showError(`${path} : ${e.message}`); }
        await fetchState({fullRebuild});
    }
    // Persistance des réglages d'exploitation (dissolve_ms/overlay_*) : route orchestrateur
    // /control/config (≠ /plugin/*) — persiste en DB + hot-push, survit au redeploy.
    async function configCall(body){ if(VMID==null) return; try{const r=await fetch(`/api/containers/${VMID}/control/config`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); if(!r.ok) throw new Error('HTTP '+r.status); clearError();}catch(e){showError("Config : "+e.message);} }
    function dissolveMsCommit(){ const el=$('#mx-dissolve-ms'); const v=parseDur(el&&el.value); if(el&&frameRate) el.value=fmtDur(v); configCall({dissolve_ms:v}); }
    function clickPgm(i){ mxCall('pgm',{input:i},{fullRebuild:false}); }
    function clickPvw(i){ mxCall('pvw',{input:i},{fullRebuild:false}); }
    async function keyerCut(){ keyer.active=!keyer.active; keyerRefreshActiveUi(); keyerStripRefresh(); if(VMID==null) return; try{const r=await fetch(`/api/containers/${VMID}/plugin/keyer/active`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:keyer.active})}); if(!r.ok) throw new Error('HTTP '+r.status); clearError();}catch(e){showError("Toggle keyer : "+e.message);} }
    function dskToggle(){ dskEnabled=!dskEnabled; dskRefreshUi(); configCall({overlay_enabled:dskEnabled}); }
    function take(forceDurationMs){ let dur; if(forceDurationMs===0){dur=0;}else{const el=$('#mx-dissolve-ms'); dur=parseDur(el&&el.value);} if(dur>0&&!(tbarCtl&&tbarCtl.isDragging())) tbarRunDissolve(dur); mxCall('take',{duration_ms:dur},{fullRebuild:false}); }
    function applyOverlayUrl(){ const el=$('#mx-overlay-url'); const url=(el.value||'').trim(); configCall({overlay_url:url||null}); }

    function mount(el, vmid, ctx){
        if(pollTimer){clearInterval(pollTimer); pollTimer=null;}
        eventsStop();
        dskPreviewStop();
        dveReset();
        EL=el; VMID=vmid; TOAST=(ctx&&ctx.toast)||(()=>{});
        state=null; errorMsg=null; mixSendPending=false; dskEnabled=false;
        preset={mode:"dissolve",wipe:{direction:"horizontal",reverse:false,alternate:false,softness:0.10}};
        keyers=[defaultKeyer()]; keyer=keyers[0];
        dsks=[]; layers=keyers.slice(); dskEngine=false;
        targets={bgd:true,keyers:[false]};
        // Identité du mélangeur pour les préférences de poste : l'instance si le shell la donne
        // (elle survit au recreate), sinon le vmid.
        const inst=(ctx&&ctx.instance)||{};
        SCOPE=inst.instance_uuid||inst.hostname||null;
        rowPrefs=rowPrefsLoad();
        // Fragment statique (control.html) : traduit AVANT le premier rendu, sinon un éclair de
        // français apparaît sur une interface anglaise à chaque ouverture du panneau.
        if(window.t) el.querySelectorAll('[data-i18n]').forEach(n=>{
            const v=window.t(n.dataset.i18n);
            if(v && v!==n.dataset.i18n) n.textContent=v;
        });
        setupPending=false; setupParams=null; setupHost=(inst&&inst.hostname)||''; wizRoot=null; wizApi=null; setupBanner=null;
        fetchState({fullRebuild:true}).then(()=>{ eventsStart(); setupCheck(); });
        pollTimer=setInterval(()=>fetchState({fullRebuild:false}),2000);
    }
    function unmount(){
        if(pollTimer){clearInterval(pollTimer); pollTimer=null;}
        eventsStop();
        Object.keys(tallyTimers).forEach(k=>clearTimeout(tallyTimers[k]));
        dskPreviewStop(); if(tbarCtl){ try{ tbarCtl.destroy(); }catch(_){} tbarCtl=null; }
        if(busRO){ try{ busRO.disconnect(); }catch(_){} busRO=null; }
        Object.values(wipeKnobs).forEach(c=>{ try{ c.destroy(); }catch(_){} }); wipeKnobs={};
        compDestroy(); if(compRaf&&typeof cancelAnimationFrame==='function') cancelAnimationFrame(compRaf); compRaf=0;
        patternKnobs.forEach(c=>{ try{ c.destroy(); }catch(_){} }); patternKnobs=[];
        keyerKnobs.forEach(x=>{ try{ x.c.destroy(); }catch(_){} }); keyerKnobs=[];
        dveReset();
        if(wipePostTimer){ clearTimeout(wipePostTimer); wipePostTimer=null; }
        if(wizApi){ try{ wizApi.close(); }catch(_){} wizApi=null; }
        wizRoot=null; setupBanner=null; syncBanner=null;
        EL=null; VMID=null;
    }

    const exp={mount,unmount,clearError,
        // Assistant de configuration (0.23.56) : ouvrir (⚙ → Assistant, bandeau), actions du ⚙ du shell.
        wizOpen:()=>wizOpen(), settingsActions,targetToggle,modeIconClick,wipeIconClick,reverseToggle,dirModeSet,wipeSymToggle,wipeColorSet,
        // Volets composés (0.23.47) : éditer (nom ou null = nouveau), action (save, recall, delete, close).
        compEdit, compAction, compOp, compShape, compFlag, compOpenFromDesk,
        // Bibliothèque commune (0.23.54) : choisir un volet (id), action (next, copy, delete), verser.
        libPick, libAction, compShare,
        compDraft:()=>compDraft, compPreview:(a,C,W,H)=>compMix(a,C,W||32,H||18),
        // Rotatifs des keyers montés (0.23.65) : gardes et pupitres les pilotent comme un geste.
        keyerKnobs:()=>keyerKnobs.slice(),
        // Rangées de sources paginées (0.23.66) : SHIFT d'écran, et le calcul (gardes).
        busShift, busFit, busLayout, busResize, busState:()=>({cap:busCap, page:busPage}),
        // Surface DVE (0.23.61) : gestes au pointeur, conversions d'unités, surface (gardes, pupitres).
        dveDown,dveMove,dveUp,dveRect,dveParams,dveSurface:()=>dveSurf(),dveLayers:()=>layers,dveTool:(nom,arg)=>dveTool(nom,arg),
        clickPgm,clickPvw,keyerCut,dskToggle,take,applyOverlayUrl,dissolveMsCommit,
        // Geste d'un DSK (0.23.52) : cut, auto, tie — même chemin que les touches de la rangée DSK.
        dskAct:(j,act)=>dskButton(j,act),
        tallySet,tallyMap,setView,sideToggle,sideClose,sideSecSet,sideEditKeyer,keyerRowPick,
        // Sorties programme (0.23.53) : couche × sortie, tally, base du PVW (à chaud) ; forme (↻).
        outLayer,outTally,outPvwBase,outName,outAdd,outRemove,outPvwToggle,outCancel,outApply,
        // Changement d'un champ de keyer (type, input, key_input, ccolor) : même chemin que les champs.
        keyerField:(i,act,v)=>keyerSelect(i,act,v),
        // Geste de keyer par son nom (onair, auto, clean, edit, rowfill, rowkey, copy, paste, psave,
        // recall, pdelete, cswatch, pshape) : même chemin que les boutons — pour un pupitre ou un raccourci.
        keyerAct:(i,act,name,v)=>keyerButton(i,act,{dataset:{name,v},setAttribute(){}})};
    window.__mx=exp;
    return exp;
})();
