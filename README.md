# `mixer` — mélangeur vidéo broadcast sur le bus MXL

Mélangeur de production : N entrées, bus BGD A/B, transition, incrustations, et trois sorties
simultanées. Tout passe par le **bus MXL** en mémoire partagée — ni réseau, ni ré-encodage entre
deux fonctions. Le script Python est le point d'entrée ; le travail lourd est fait en numpy et
dans les noyaux compilés du runtime.

Plugin pour **Bobi.Studio**, sous GPL-3.0-or-later.

## Ce qu'il fait

| | |
|---|---|
| Entrées | **1 à 20**, câblées à chaud. Une entrée qui change de source ne redémarre rien |
| Bus | BGD **A/B**, transition par fondu (durée réglable) ou coupe franche |
| Keyers | **1 à 4**, linéaire, luma, chroma ou motif ; remplissage pris sur une entrée ou une couche web |
| DSK | **0 à 2**, empilés au-dessus du programme, leurs sources prises directement aux entrées |
| Sorties | **PGM** toujours, **CLEAN**, **PVW** optionnel, et d'autres nommées si besoin — chacune publiée sur le bus |
| Tally | émission des niveaux pour les sources à l'image, PGM et PVW distingués |

**Chaque sortie choisit ce qu'elle porte.** Les couches — keyers puis DSK, dans l'ordre de la
pile — se cochent par sortie, dans une matrice modifiable à chaud. Le PGM les prend toutes par
défaut, le CLEAN aucune, et rien n'empêche d'envoyer un DSK sur le CLEAN ou de créer une sortie
de plus pour un diffuseur qui veut son propre habillage. Une couche commune à plusieurs sorties
n'est calculée qu'une fois.

Le PVW, lui, montre l'état d'après la prochaine transition de **sa** sortie de base.

Toutes sont des flux MXL ordinaires : on les câble comme n'importe quelle source.

## Où regarder quoi

| | |
|---|---|
| `script.py` | ce qui tourne dans le conteneur : lecture des entrées, composition, publication des trois sorties, serveur de contrôle |
| `control.html` / `.js` / `.css` | la console de l'opérateur — barre de bus, transition, keyers, DSK |
| `wizard.js` | l'assistant de configuration, ouvert à la première prise en main |
| `hooks.py` | la seule partie qui tourne DANS l'orchestrateur : normalisation au déploiement |
| `i18n/` | les libellés du plugin, FR et EN. Ils voyagent avec lui, jamais dans le cœur |
| `meta.json` | le journal des versions |
| `versions/` | les versions archivées, qu'un conteneur peut encore exécuter |

## Configuration

Ce qui se règle au déploiement :

| clé | |
|---|---|
| `n_inputs` | nombre d'entrées, 1 à 20 |
| `n_keyers`, `n_dsk` | 1 à 4 keyers, 0 à 2 DSK |
| `dissolve_ms` | durée du fondu par défaut |
| `latency_mode` | délai par défaut appliqué aux entrées |
| `tally_*` | ce qui est émis, pour quels niveaux, et avec quelles couleurs |

Les autres clés ne figurent pas dans le formulaire : les couches web (`overlay_*`,
`web_layers_max`), le mode tranche (`slice_*`) dont l'interrupteur qui compte est **global**, et
le comportement sur image manquante. `hidden` ne cache rien — il retire un champ d'un
formulaire, et la clé reste lisible et modifiable par l'API.

## Câblage

**Consomme** N entrées vidéo, déclarées par répétition sur `n_inputs`, en mode **hot-wire** : on
change la source d'une entrée sans redéployer et sans interrompre le programme.

**Produit** `<hostname>_pgm`, `<hostname>_clean` et `<hostname>_pvw`, déclarés au manifeste. Une
sortie supplémentaire publie `<hostname>_<nom>`.

## Contrôle

Une vingtaine d'endpoints derrière le proxy de plugin — `/take`, `/cut`, `/pgm`, `/pvw`,
`/mix`, `/keyer/*`, `/dsk/*`, `/preset`… — et des endpoints de lecture accessibles avec le seul
login : `/state`, `/preview.png`, `/delays`, les aperçus de DSK.

**Tout est pilotable par macro.** Le manifeste publie **40 actions** et un `param_tree` : ce que
l'opérateur peut faire à la main, un déclencheur ou un pupitre peut le faire aussi. C'est une
règle du produit, pas une faveur — une fonction qu'on ne peut pas automatiser est une fonction
à moitié livrée.

## Assistant de configuration

Un mélangeur créé depuis le formulaire s'ouvre la première fois sur son **assistant** : le
schéma est l'interface — entrées → BGD A/B → transition → sorties, keyers et DSK empilés
au-dessus. On le rouvre ensuite par **⚙ → Assistant de configuration**. Un mélangeur déployé
avant l'assistant compte comme configuré : ni ouverture automatique, ni bandeau.

Détail d'exploitation dans `help.md`, rendu dans la page Aide du produit.

## Gardes

Le mélangeur est exercé par **66 vérifications** du dépôt principal — calage, tally, keyers,
transitions, cœurs, rendu de la console. Elles vivent là-bas parce qu'elles ont besoin de
l'orchestrateur, et elles tournent en intégration continue à chaque poussée.

## Prérequis

Un nœud avec la capacité `compute`. Le mélangeur ne demande ni carte ni GPU : il compose en
mémoire partagée, sur les cœurs qu'on lui réserve.

⚠ **Étalonnez avant de passer à l'antenne.** Le coût d'un mélangeur dépend du nombre d'entrées
actives, des keyers allumés et du format — et il n'est pas le même sur votre matériel que sur
celui de l'éditeur. Montez votre dispositif, exercez-le au pire de ce qu'il fera, lancez une
campagne d'étalonnage.

## Journal

`meta.json` porte l'historique des versions, de la plus récente à la plus ancienne.
