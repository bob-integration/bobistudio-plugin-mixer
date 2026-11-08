# Mélangeur

Mélangeur vidéo broadcast : N entrées (1 à 20 ; un pupitre ATEM n'adresse que celles couvertes par ses boutons de bus, 8 sur la plupart des modèles), 1 à 4 sorties programme (**PGM** toujours, **CLEAN** et d'autres au choix) et un **PVW** optionnel, transitions cut/dissolve/wipe via T-bar ou commande directe, 1 à 4 keyers amont et 0 à 2 DSK aval (dve, linear, luma, chroma, motif ou page web). Pilotable depuis la rubrique **Composition** et depuis un pupitre ATEM ou Skaarhoj.

## Assistant de configuration

Un mélangeur **créé** depuis le formulaire de création s'ouvre, la première fois, sur son
**assistant** (pour qui a le droit de déployer ; les autres voient « en attente de configuration »).
On le rouvre ensuite par **⚙ → Assistant de configuration**. Un mélangeur déployé avant cet assistant
compte comme configuré : ni ouverture automatique, ni bandeau.

Le **schéma** est l'interface : entrées → BGD A/B → transition → sorties sur la ligne du bas, keyers et
DSK empilés au-dessus, leurs sources prises directement aux entrées. Touchez un élément pour le régler :
entrées et format, keyers (1 à 4) et DSK (0 à 2) de tout type, page web comprise, sorties
(1 à 4 : couches, tally, base du PVW, PVW oui/non), pupitre ATEM et tally (« Configurer maintenant »
ou « Plus tard » ; touche MACRO de Shift au-delà de 8 entrées). Un fil guidé accompagne la première
fois : les étapes en pastilles sur la ligne du titre (on saute à l'une d'elles d'un toucher), un bandeau
au-dessus du schéma avec les actions de l'étape, et le bloc concerné encadré — rien ne couvre le schéma.
Il se saute. Le câblage des sources reste dans la page **Câbles**.

La **transition** est dessinée sur le schéma, puisque c'est le chemin du signal, mais elle ne se règle
pas dans l'assistant : mix ou wipe, durée et volets relèvent de l'exploitation (rubrique
**Transitions**). Le mélangeur part avec ses défauts.

**Sources synchrones ?** Le panneau des entrées explique ce réglage : une source synchrone (même
référence PTP) arrive à temps pour le **délai par défaut** ; une source qui ne l'est pas (HF, liaison,
traitement extérieur) arrive plus tard et passe avec son image précédente tant qu'elle n'est pas rangée
dans un **groupe de délai**. L'encart montre les groupes et les entrées mesurées en retard, règle le
délai par défaut à chaud (« Fixe, 1 image », le défaut, ou « Au plus rapide ») et renvoie à l'onglet
**Entrées & délais** pour les groupes. En exploitation, un bandeau « Sources non synchrones ? » le
signale aussi dès qu'une entrée sans groupe reste en retard plus de 3 s ; il s'efface après 10 s à
l'heure, ou dès qu'elle est rangée dans un groupe. Mesure en mode tranche seulement. États de macro :
`latency_mode`, `unsync_count`.

Rien ne part avant **Appliquer**. Les réglages marqués **↻** redémarrent le script (entrées, format,
nombre de keyers ou de DSK, genlock, sorties ajoutées, retirées ou qui changent de flux, PVW) : c'est un
**redéploiement**, jamais une recréation, et en service il est **confirmé** (« l'antenne sera coupée
quelques secondes »). Le reste s'applique à chaud. L'assistant n'ajoute aucun réglage : tout ce qu'il
règle reste pilotable par les endpoints et les macros habituels — `POST
/api/containers/<vmid>/control/setup` (droit de déployer) ne fait que poser ensemble ce qui redémarre.

## Opérations disponibles

- **Cut** : bascule instantanée PVW → PGM
- **Auto** : transition automatique de durée configurable (dissolve, additif ou wipe selon le préréglage)
- **T-bar** : fondu manuel PGM ↔ PVW
- **Keyers amont** : 1 à 4 incrustateurs dans le M/E (dve, linear fill + key, luma, chroma, motif), chacun avec ON AIR, AUTO, masque et position
- **DSK** : 0 à 2 incrustateurs aval, au-dessus de tout, avec CUT, AUTO et TIE ; l'ancien habillage HTML est le DSK 1 de type page web

Les **touches** du pupitre ont une **taille fixe** : sur un grand écran elles ne s'étirent pas (la place
en trop reste vide à droite), sur un petit elles ne se serrent pas — la transition suivante, durée |
AUTO | CUT et les keyers passent à la ligne. Les rangées de **sources** (BGD A, BGD B, rangées des
keyers) ne passent pas à la ligne : quand toutes les entrées ne tiennent pas, la rangée en montre ce
qu'elle peut moins une, et une touche **SHIFT** au bout passe aux suivantes. Elle reste enfoncée sur
les pages suivantes, revient à la première après la dernière, et affiche la page montrée (« 9-16 »).
Toutes les rangées sont à la même page, colonnes alignées. Si l'entrée à l'antenne (BGD A) ou en
préparation (BGD B) est sur une page masquée, le SHIFT de sa rangée prend la teinte du bus (rouge,
vert ; ambre pour la source d'un keyer). Ce SHIFT est celui de l'écran : il ne touche ni le moteur, ni
le pupitre ATEM, ni les macros — chaque poste a sa page.

Un **volet** dont la forme vient de changer n'est pas toujours prêt : son champ se calcule en tâche
de fond (jusqu'à 0,26 s en 1080p pour un cœur). L'AUTO, ou le premier geste de la T-bar, **attend** alors
qu'il soit prêt, 0,3 s au plus, pendant que le PGM continue de sortir normalement ; au-delà, la
transition part en **fondu**. Ces attentes sont comptées dans `/state.wipe_waits` et sur `:8080`.

Pendant une transition (AUTO ou T-bar), les bus **BGD A** et **BGD B** restent **chauds** : A remplace
la source qui sort, B celle qui entre, à l'image suivante, sans arrêter la transition ni déplacer la
T-bar. En fin de transition, le PVW reprend la dernière source choisie sur A. Si la transition ne porte
que des keyers, A coupe le fond dessous et la transition des keyers continue.

## Volets (Wipe)

Choisir **Wipe** dans le bloc transition fait apparaître les formes et, dans le panneau
**Transitions**, les réglages du volet.

- **Formes** : linéaire, iris, boîte, losange, étoile, cœur, horloge (balayage autour du centre).
  Horizontal, vertical et diagonal sont des volets **linéaires** à angle prédéfini (0°, 90°, 45°).
- **Angle** (0 à 360°) : direction d'un volet linéaire ; rotation d'une forme.
- **Centre X / Y** : centre des formes et pivot du linéaire.
- **Symétrie** : un linéaire part du centre vers les deux bords (porte coulissante) ; l'horloge
  balaie dans les deux sens à la fois.
- **Répétition** : N bandes balayées ensemble (stores) pour un linéaire, grille de N × N formes
  pour les autres.
- **Bordure** : largeur, **couleur**, et deux douceurs : **interne** (bord côté source qui part)
  et **externe** (bord côté source qui arrive). Avec une bordure, ces deux douceurs remplacent la
  douceur du volet.
- **NORM / N/R / REV** : sens normal, alterné après chaque volet, ou inverse. Une forme en REV se
  referme : la nouvelle source arrive par l'extérieur. En N/R, la lettre du prochain sens est
  allumée dans la touche.

Forme, angle, centre, symétrie, répétition et sens sont **figés au départ** d'un volet ; douceur
et bordure agissent aussi sur le volet en cours. Au début du volet l'image est entièrement la source
qui part, à la fin entièrement celle qui arrive, quels que soient les réglages. Le préréglage de
transition (type, réglages du volet) survit à un redéploiement depuis la version 0.23.44.

### Volets composés

Un volet composé combine deux volets complets (chacun avec tous ses réglages) :

- **union** : la nouvelle source apparaît là où l'un OU l'autre volet l'a découverte ;
- **intersection** : là où les DEUX l'ont découverte ;
- **décalage** : le second volet part plus tard (décalage en fraction de la durée), **ronge** ce
  que le premier a découvert jusqu'à une **profondeur** donnée, puis se retire — la transition
  finit toujours sur la nouvelle source.

Les volets composés sont rangés **sous un nom**, propres à chaque mélangeur, et survivent à un
redéploiement. Une macro les rappelle par leur nom (« Volet composé : rappeler ») ; un keyer motif
peut prendre la forme d'un volet composé. REV et N/R inversent les deux volets à la fois.

**Éditeur** (panneau **Transitions**, en mode Wipe) : la bibliothèque du mélangeur en vignettes
animées — toucher une vignette l'ouvre dans l'éditeur, « Nouveau volet composé » en crée un. Le
panneau s'élargit : **volet 1** à gauche, **résultat** au milieu, **volet 2** à droite, chacun avec
son aperçu, sa forme, symétrie, REV, ses rotatifs et sa couleur de bordure. Sous le résultat, la
**chronologie** montre la course des deux volets sur la durée : en décalage, la poignée du bas règle
le départ du second volet, celle du haut sa profondeur (au doigt, à la souris ou aux flèches).
L'éditeur travaille sur un brouillon : **Enregistrer** le range sous son nom, **Prochaine
transition** en fait le prochain volet, **Supprimer** le retire. Au pupitre, la touche « Composé :
nom » rappelle qu'un volet composé est choisi ; choisir une forme simple le remplace.

**Bibliothèque commune** (0.23.54) : en plus des volets propres à chaque mélangeur, l'installation
tient une bibliothèque de volets composés **partagée par tous les mélangeurs**. Sous les vignettes
« Dans ce mélangeur », le groupe « Bibliothèque commune » les montre ; toucher une vignette la
choisit, puis **Prochaine transition** la copie dans ce mélangeur et en fait le prochain volet,
**Copier dans ce mélangeur** la copie seulement, **Supprimer de la bibliothèque** la retire. Dans
l'éditeur, **Verser dans la bibliothèque** y range le volet **tel qu'enregistré** dans ce mélangeur
(pas le brouillon en cours ; un nom déjà pris se remplace sur confirmation). Un rappel fait une
**copie**, jamais un lien : un volet retiré de la bibliothèque reste dans les mélangeurs qui l'ont
rappelé (sa vignette porte alors « Retiré » ; une copie encore présente porte « Biblio. »). Un volet
local de même nom et différent n'est jamais écrasé : le rappel est refusé. Tout opérateur peut
verser, rappeler et supprimer ; une macro (« Volet composé : rappeler de la bibliothèque commune »)
et Ember+ rappellent aussi. Les volets déjà présents dans un mélangeur ne sont pas versés
automatiquement.

## Sorties

De **1 à 4 sorties programme** (0.23.53), plus un **PVW** optionnel. Toutes partagent le fond, la
transition et l'état des couches (keyers et DSK à l'antenne ou non) ; **chaque sortie choisit ses
couches**, composées dans l'ordre de la pile (keyers, puis DSK). Les couches communes à plusieurs
sorties ne sont calculées qu'une fois.

| Sortie | Flux | Par défaut |
|--------|------|------------|
| PGM (toujours là, slot NMOS 0) | `<hostname>_pgm` | toutes les couches, compte pour le tally |
| CLEAN (slot 1) | `<hostname>_clean` | aucune couche (les keyers réglés « CLEAN » ; un DSK peut désormais y aller) |
| autres (slot 3 et suivants) | `<hostname>_<nom>` | aucune couche |
| PVW (slot 2, optionnel) | `<hostname>_pvw` | état d'après la prochaine transition de **sa sortie de base** |

Réglages, rubrique **Sorties** du panneau ⚙ :

- **à chaud** : la matrice couches × sorties, la ligne **Tally** (la sortie compte-t-elle pour le
  tally du mélangeur ?) et la sortie de base du PVW. Endpoint `POST /outputs` :
  `{"output": id, "layer": "k1" | "d1", "on": bool}`, `{"output": id, "tally": bool}`,
  `{"pvw_base": id}` ; actions de macro « Sortie : une couche… », « Sortie : compte pour le
  tally », « PVW : sortie de base » ; état `/state.outputs`, `/state.pvw_base`.
- **↻ redéploiement** (l'antenne est coupée quelques secondes) : ajouter, renommer ou retirer une
  sortie, ajouter ou retirer le PVW. Le nom donne le flux (`<hostname>_<nom>`, sans accents) ;
  renommer ou retirer une sortie **débranche** ceux qui la lisaient : la page les liste avant de
  confirmer. Route `POST /api/containers/<vmid>/control/outputs` (`confirm: true` exigé).
- **à chaud aussi, le libellé seul** (0.23.55) : un nouveau nom qui garde le même flux (« CLEAN » →
  « Clean », accents, casse) s'applique sans coupure — mélangeur, interface et libellé du sender
  NMOS. Même route, sans confirmation.

Une couche ajoutée au redéploiement (keyer ou DSK de plus) va sur la seule sortie de base du PVW.
Un mélangeur déployé avant 0.23.53 garde ses trois sorties, leurs flux et leurs slots NMOS.

## DSK (keyers aval) et couches web

Le nombre de DSK se choisit au déploiement (**Nombre de DSK**, 0 à 2, défaut 1). Comme sur un ATEM, un
DSK est composé **après** la transition du fond, **au-dessus de tous les keyers** (DSK 1 dessous), et
**jamais** sur la sortie `_clean`, prélevée avant lui. Il a les mêmes types et réglages qu'un keyer
(page web comprise), et ses propres gestes, dans la rangée **DSK** du pupitre :

- **CUT** (ON AIR) : coupe sèche ;
- **AUTO** : fondu (ou volet) sur sa propre durée ;
- **TIE** : lié, il entre ou sort avec la prochaine transition du fond (CUT, AUTO, T-bar).

Le PVW le montre dans son état d'après la prochaine transition : à l'antenne et non lié, il y est ;
lié, il en sort (ou y entre). Ses entrées sont au tally quand il est à l'image. Son état d'antenne et
ses réglages survivent au redéploiement ; le lien (TIE), non.

**Type page web** (keyers et DSK) : une page HTML rendue dans le conteneur sert de fill (sa couleur) et
d'alpha (sa transparence) ; masque, taille, position, découpe et transitions s'y appliquent comme à un
linear. Un seul navigateur, un onglet par couche web ayant une URL ; leur nombre est limité (**2** par
défaut, réglage `web_layers_max`) et une page de trop est refusée avec un message. Seul le rectangle
non transparent de la page est composé.

**Habillage d'avant 0.23.52** : c'est le DSK 1, de type page web. `overlay_url`, `overlay_enabled`,
`POST /overlay`, `POST /config` (overlay_*), `GET /preview.png`, `/state.overlay_*` et l'action de macro
« Habillage (DSK 1) » continuent de marcher, sur lui. Un mélangeur déployé sans `n_dsk` a un DSK 1 web
avec l'URL et l'état de son habillage.

Endpoints : `/dsk/<n>/<geste>` ou `/dsk/<geste>` avec `dsk` (à partir de 1) ou `idx` (à partir de 0)
dans le corps — `cut`, `auto`, `tie`, `settings`, `type`, `input`, `transform`, `mask`, `key`, `rate`,
`chroma`, `pattern`, `web` ; aperçu `GET /dsk/<n>/preview.png`. État : `/state.dsks`.

Pupitre ATEM : une touche DSK **non affectée** à un keyer (Réglages → Pupitres → ATEM) pilote le DSK de
même numéro du mélangeur (CUT, AUTO, TIE, et leurs voyants) ; affectée, elle pilote ce keyer.

## Keyers amont

Le nombre de keyers se choisit au déploiement (**Nombre de keyers**, 1 à 4, défaut 1). Comme les
USK d'un ATEM, ils sont **dans le M/E** : composés sur le fond, au-dessus de la transition du fond,
sous les DSK ; le keyer 1 est en dessous, le keyer 4 au-dessus. Ils n'apparaissent sur `_clean` que
réglés pour (rubrique **Sorties**).

Les **rotatifs** des keyers (seuil, gain, tolérances, masque, X / Y / taille, bordure, durée et douceur
de l'AUTO), comme la douceur du volet, sont ceux de toute l'interface : glisser (au doigt ou à la
souris), **Maj** pour le réglage fin, molette (un cran = un pas), flèches, double-clic ou **↺** pour
la valeur par défaut. La valeur part au moteur au relâcher et à chaque cran.

Chaque keyer a son **type** :

| Type | Alpha | Entrées |
|------|-------|---------|
| **DVE** | uniforme : l'image est réduite et placée, avec bordure, sans découpe | une source |
| **Linear** | la luminance du **key**, pleine échelle (0 = transparent, max = opaque) — la paire `stills` / `stills_key` du plugin Stills | fill + key |
| **Luma** | la luminance du **fill**, échelle studio (16 = transparent, 235 = opaque) | une source |
| **Chroma** | la **couleur** du fill : transparent près de la couleur de clé (vert `#00b140` par défaut) | une source |
| **Motif** | une **forme** générée (barres, diagonale, iris, cœur, boîte) posée sur l'écran | une source |

Découpe (linear, luma) : **inverser**, **seuil** (en dessous : transparent) et **gain** (pente) —
alpha = (key − seuil) / (1 − seuil) × gain, borné à [0, 1]. Le key d'un linear est toujours lu **à
la même image que son fill**, même si les deux entrées sont dans des groupes de délai différents.

Chroma : **couleur de clé** (sélecteur, ou Vert / Bleu d'un appui), **tolérance** (distance à la
clé, dans le plan de chrominance, en dessous de laquelle tout est transparent), **douceur** (largeur de
la rampe jusqu'à l'opaque), **débordement** (retire du fill la part de la couleur de clé : la frange
verte d'une chevelure redevient neutre) et **inverser**. La couleur est convertie avec la colorimétrie
déclarée du conteneur ; si elle ne l'est pas, BT.709 est supposé et l'interface le signale.

Motif : **forme** (les formes du volet), **taille** (0 = rien, 1 = tout l'écran pour un
motif centré), **position X / Y** du centre, **douceur** du bord (rampe intérieure), **angle**,
**répétition** (grille de N × N motifs, N barres pour le linéaire), **inverser** (garder le dehors)
et une **bordure** de couleur autour du motif, posée sous le fill : largeur, couleur, douceur
interne (côté fond) et externe (côté fill, qui remplace alors la douceur du motif). La forme est posée en coordonnées d'**écran** : avec un fly, elle découpe le fill
là où il se trouve.

Coût indicatif en 1080p 4:2:2 8 bits, un keyer plein cadre, un cœur : motif ≈ 3 ms par image (≈ 8 ms
pendant un fondu), chroma ≈ 9 ms (comparable au luma) ; en 10 bits avec débordement, le chroma est plus
cher (≈ 15 ms).

Pour tous les types : **masque** (fractions retirées à gauche, à droite, en haut, en bas) et
**fly** (position X/Y, taille).

Mise à l'antenne, trois façons :
- **ON AIR** : coupe sèche, keyer par keyer ;
- **AUTO** du keyer : fondu sur sa propre **durée AUTO**, indépendant de la transition du fond ;
- **avec la transition** : voir ci-dessous.

Les réglages des keyers (type, entrées, masque, fly, découpe, clé chroma, motif, durée AUTO) survivent à un
redéploiement ; l'état d'antenne, non.

Au pupitre (onglet **Exploitation**), chaque keyer a une ligne compacte : type, ON AIR, AUTO,
Clean, et **Régler**, qui ouvre ses réglages dans le **panneau latéral** (bouton **Réglages** ;
sur iPad, le panneau passe par-dessus le pupitre et se ferme en touchant le voile ou en glissant
vers la droite). Les boutons **Fill** / **Key** de la ligne affichent, pour ce poste, une rangée
de sources du keyer, comme BGD A / B ; elles sont masquées par défaut.

Dans le panneau : **Copier les réglages** / **Coller** recopie un keyer sur un autre (même
d'un autre mélangeur ouvert dans le navigateur), sans jamais toucher à l'antenne ; les
**sauvegardes** rangent un réglage sous un nom, propre à ce mélangeur (32 au plus), et se
rappellent sur n'importe quel keyer — aussi par macro (« Keyer : rappeler une sauvegarde »).

### Placement DVE (surface)

Dès qu'une couche (keyer ou DSK) est de type **DVE**, le panneau affiche **Placement DVE** : un cadre au
format de la sortie où chaque DVE est un rectangle, la couche réglée au panneau en évidence. Ce sont
les gestes de **tous les éditeurs** (mur multiview, PiP, Split…) :

- **toucher** un DVE le sélectionne et l'ouvre au panneau ; **Maj** ou **Ctrl/⌘ + clic** en ajoute ou
  en retire (au doigt : bouton **Sélection multiple**, qui permet aussi de tracer un **rectangle de
  sélection** en glissant dans le vide) — la **référence** est le dernier touché ;
- **glisser** déplace la sélection, bornée **en groupe** ; le centre d'une image reste dans le cadre
  (elle peut en sortir de moitié, comme au moteur) ;
- **coin bas-droit** : taille au **format de la source**, coin haut-gauche fixe, jamais au-delà du plein
  cadre ; **Alt** (au doigt : **Depuis le centre**) agrandit autour du centre ;
- **Aimant** aux bords et au centre du cadre et aux autres DVE ; **Aligner**, **Taille** (même largeur /
  hauteur / taille que la référence), **Distribuer** (3 au moins) ;
- **flèches** : 1 pixel de la sortie (Maj : 10) ; **annuler / rétablir**, **copier / coller** (les
  réglages du keyer, comme « Copier les réglages ») — au clavier ou par la barre **Édition**.

Correspondance exacte avec le moteur : X / Y sont le **centre** de l'image placée et la taille sa
largeur rapportée à la sortie (l'image garde le format de sa source). Pendant un geste, la position est
envoyée au mélangeur à chaud (au plus dix fois par seconde) ; au relâcher, chaque couche déplacée est
validée par le même réglage que les rotatifs **X / Y / Taille**, enregistré : il survit au
redéploiement. Les rotatifs suivent la surface, et la surface suit les rotatifs, les macros et les
autres postes.

## Cible des transitions (BGD / KEY n)

Un **Take**, un **Auto** ou le **T-bar** agissent sur le **fond** (bascule PVW→PGM, « BGD ») et/ou
sur chacun des **keyers** ciblés (« KEY 1 » … « KEY 4 », qui basculent on/off) — tous ensemble,
dans la même transition. Par défaut seul BGD est ciblé ; cibler un keyer permet de faire entrer
une incrustation en même temps qu'un cut de fond, ou de la faire disparaître seule sans toucher
au programme.

Compatibilité : un appel sans numéro de keyer (`/keyer/input`, `/keyer/active`,
`/keyer/transform`, l'action de macro « Keyer : bascule », le bouton KEYER TOGGLE) vise le keyer 1,
et `/state.keyer` le publie.

## Câblage et synchronisation

Câbler les N sources depuis la page **Câbles**. Le changement de source se fait **à chaud**. La cadence de sortie se verrouille 1:1 sur une **entrée de référence** configurable (`sync_ref`, défaut : la 1ʳᵉ entrée) — indépendante de la sélection PGM/PVW, donc stable à la commutation. Les autres entrées sont alignées par horodatage ; si la référence disparaît, le mélangeur passe en cadence libre sans couper le programme.

## Pilotage ATEM

Le mélangeur peut être piloté depuis un pupitre Blackmagic ATEM. Configurer dans **Réglages → Pupitres ATEM** : affecter l'IP du pupitre au conteneur mélangeur.

| Geste pupitre | Action |
|---------------|--------|
| Bus Program | Cut direct sur l'entrée |
| Bus Preview | Prépare l'entrée en preview |
| CUT | Coupe sèche PVW→PGM |
| AUTO | Transition automatique |
| T-bar | Crossfade manuel |

Les touches KEY ON AIR, Next Transition (BKGD, KEY n), MIX / WIPE et DSK (CUT, AUTO, TIE) du pupitre
sont décodées par le service ATEM, et leur état revient au pupitre.

## Tally

Le mélangeur peut **émettre le tally** PGM/PVW (réglage `tally_emit`, panneau ⚙ du conteneur) sur un niveau de Tally, diffusé aux équipements via TSL 5.0 (Réglages → Protocoles → TSL). Les sources sont identifiées par les ports du projet.

Le rouge (« au PGM ») vient des sorties qui **comptent pour le tally** (ligne Tally de la rubrique
Sorties ; par défaut le PGM seul) : une source à l'image sur l'une d'elles est allumée. En
propagation, une sortie à l'antenne en aval remonte SES sources (liste `tally.outputs` du moteur).

## Mode tranche (latence réduite)

Le mélangeur peut composer et publier ses trois sorties **bande par bande** au lieu d'attendre la trame complète (mode tranche MXL). Sur une chaîne dont l'amont et l'aval sont aussi en mode tranche, l'étage mélangeur n'ajoute plus ~1 image de latence de traversée.

- **Opt-in** : paramètre `slice_mode` au déploiement (désactivé par défaut — comportement historique strictement inchangé). `slice_lines` règle la hauteur de bande (défaut 36 lignes).
- Conditions : genlock actif et sources progressives ; en entrelacé, le chemin classique s'applique.
- Le rendu bandé est identique au rendu plein (mêmes images, octet pour octet).
- Garde-fous intégrés : la sortie n'est **jamais en retard** (budget de trame dur ; une entrée trop lente est figée sur sa dernière image complète pour cette trame seulement) ; une source **chroniquement en retard** est signalée dans les métriques, avec le conseil d'insérer un délai d'1 image (plugin Délai) sur cette entrée.

## Diagnostic

- `GET :8082/preview.png` renvoie une image fixe du PGM courant — utile pour vérifier l'état
  d'antenne depuis un outil externe sans ouvrir le monitoring.
- `GET :8082/state` publie l'état complet : entrées PGM/PVW, transition en cours, état de chaque
  keyer (`keyers` : type, entrées, réglages, à l'antenne, en mouvement), chaque DSK (`dsks`, avec `tie`),
  overlay, cibles de
  transition courantes, et niveau de log.
- **Entrée morte** (flux introuvable, ou source dont le producteur n'écrit plus — sans signal) :
  le mélangeur la rouvre à intervalle croissant (2 s, 4 s, 8 s… jusqu'à 30 s) et repart de 2 s dès
  qu'elle écrit de nouveau ou qu'on la recâble. Le journal le dit une fois à la perte et une fois au
  retour, plus à chaque tentative. `dead_inputs` (numéros à partir de 1) et `dead_count` dans
  `/state` ; `dead_count` est un état de macro.

## Notes

- La durée de dissolve par défaut et l'habillage (DSK 1) se règlent aussi à chaud (rubrique Composition) et sont persistés.
- Les états d'antenne (position T-bar, sélection, keyers, DSK, pages web) sont figés par image entière — jamais de trame « moitié cut ».
