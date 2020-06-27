# Woql demo for Dublin Port shipping movements

The demo shows how time based events can be modelled in WOQL.

The standard `ipython` (interactive python) tool should be used to run the demo.  The `ipython` tool is a part of `Project Jupyter` and can be downloaded and installed as described [here](https://jupyter.org/install.html).

Each event has a start and end date/time,  and is modelled as an abstract base class (woql document).

While a ship is docked in port,  the duration is modelled as a docking event,  deriving from the abstract base event class.

A ship is underway (to or from port, within the limit of the displayed map) is modelled as a voyage event and again derives from the abstract base event class.

The demo is driven by a slider, which can be manually controlled or allowed to run as an animation.  Each slider setting represents a specific date/time.

TerminusDB is used to store all the (docking and voyage) events.  The demo queries TerminusDB to find the active events as required for a particular slider setting.

The standard library `matplotlib` is used for the animation and plotting.

## Raw data
The raw data for the demo are:
* hard-coded latitude and longitude waypoints and berth positions,  in the source code.
* a .csv file for the docking events
* a .csv file for the voyage events
* a .png file for map of Dublin Port.  The map was built from [OpenStreetMap.org](https://www.openstreetmap.org/export#map=5/51.500/-0.100)

## Log output
The log output from the demo is [here](https://github.com/Chrisjhorn/terminusDB/blob/master/shipping/log).

## Animation
A short .gif of the demo is [here](https://github.com/Chrisjhorn/terminusDB/blob/master/shipping/DublinPort.gif).
