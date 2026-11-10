# Mixer — Bobi.Studio's production video mixer

*[Version française](README.md)*

N inputs, A/B bus, transitions, keys and several simultaneous outputs. Submodule of
[bobi.studio](https://github.com/bob-integration/bobistudio), mounted at `plugins/mixer/`.

## What it does

- **Inputs**: 1 to 20, hot-wired. Changing an input's source restarts nothing and does not
  interrupt the programme.
- **Bus**: BGD A/B, **Auto** transition (adjustable duration) or **Cut**.
- **Keyers**: 1 to 4, linear, luma, chroma or pattern; fill taken from an input or an HTML5
  source.
- **DSK**: 0 to 2, stacked above the programme.
- **Outputs**: PGM always, CLEAN and PVW optional, and further named outputs if needed.
- **Tally**: levels sent for the sources on air, with PGM and PVW told apart.

## Outputs

Each output chooses what it carries. Layers — keyers then DSKs, in stack order — are ticked per
output, in a matrix that can be changed live. PGM takes them all by default, CLEAN none. A layer
shared by several outputs is computed only once.

PVW shows the state after the next transition of its base output.

Outputs are published on the MXL bus: `<host>_pgm`, `<host>_clean`, `<host>_pvw`, and
`<host>_<name>` for an extra output.

## Configuration

At deployment: the number of inputs, keyers and DSKs, the default Auto duration, the default input
delay and the tally settings. Everything else is set in the console.

On first opening, a **setup assistant** presents the mixer as a diagram — inputs, A/B bus,
transition, outputs, keyers and DSKs. It can be reopened from **⚙ → Setup assistant**.

## Macros and external control

Forty actions are exposed to macros and triggers, plus a `param_tree` for what is adjusted
continuously: anything the operator does by hand can be automated or driven from a panel. `/state`
and the programme preview can be read with a login alone.

## Requirements

A node with the `compute` capability. The mixer needs neither a card nor a GPU.

⚠ **Calibrate before going on air.** A mixer's cost depends on the number of active inputs, the
keyers switched on and the format, and it varies from one machine to another. Set up your rig,
run it at the worst it will ever do, and launch a calibration campaign.

## Licence

GNU GPL v3 or later — see `LICENSE`.
