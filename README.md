# Mélangeur — mélangeur vidéo de production de Bobi.Studio

*[English version](README.en.md)*

N entrées, bus A/B, transitions, incrustations et plusieurs sorties simultanées. Sous-module de
[bobi.studio](https://github.com/bob-integration/bobistudio), monté sur `plugins/mixer/`.

## Ce que ça fait

- **Entrées** : 1 à 20, câblées à chaud. Changer la source d'une entrée ne redémarre rien et
  n'interrompt pas le programme.
- **Bus** : BGD A/B, transition en **Auto** (durée réglable) ou en **Cut**.
- **Keyers** : 1 à 4, linéaire, luma, chroma ou motif ; remplissage pris sur une entrée ou une
  source HTML5.
- **DSK** : 0 à 2, empilés au-dessus du programme.
- **Sorties** : PGM toujours, CLEAN et PVW au choix, et d'autres sorties nommées si besoin.
- **Tally** : émission des niveaux pour les sources à l'image, PGM et PVW distingués.

## Sorties

Chaque sortie choisit ce qu'elle porte. Les couches — keyers puis DSK, dans l'ordre de la pile —
se cochent par sortie, dans une matrice modifiable à chaud. Le PGM les prend toutes par défaut,
le CLEAN aucune. Une couche commune à plusieurs sorties n'est calculée qu'une fois.

Le PVW montre l'état d'après la prochaine transition de sa sortie de base.

Les sorties sont publiées sur le bus MXL : `<hôte>_pgm`, `<hôte>_clean`, `<hôte>_pvw`, et
`<hôte>_<nom>` pour une sortie supplémentaire.

## Configuration

Au déploiement : le nombre d'entrées, de keyers et de DSK, la durée de l'Auto par défaut, le délai
par défaut des entrées et les réglages du tally. Le reste se règle dans la console.

À la première ouverture, un **assistant** présente le mélangeur sous forme de schéma — entrées,
bus A/B, transition, sorties, keyers et DSK. On le rouvre par **⚙ → Assistant de configuration**.

## Macros et contrôle externe

Quarante actions sont publiées aux macros et aux déclencheurs, plus un `param_tree` pour ce qui
se règle en continu : tout ce que l'opérateur fait à la main peut être automatisé ou piloté depuis
un pupitre. `/state` et l'aperçu du programme sont lisibles avec le seul login.

## Prérequis

Un nœud avec la capacité `compute`. Le mélangeur ne demande ni carte ni GPU.

⚠ **Étalonnez avant de passer à l'antenne.** Le coût d'un mélangeur dépend du nombre d'entrées
actives, des keyers allumés et du format, et il varie d'un matériel à l'autre. Montez votre
dispositif, exercez-le au pire de ce qu'il fera, et lancez une campagne d'étalonnage.

## Licence

GNU GPL v3 ou ultérieure — voir `LICENSE`.
