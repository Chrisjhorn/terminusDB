##
##  Woql demo of shipping movements at Dublin port.
##
##  Shows the use of a base class to capture ephemeral events.
##
##  Map handling in python is described in eg https://towardsdatascience.com/easy-steps-to-plot-geographic-data-on-a-map-python-11217859a2db
##
##  Chris Horn
##  May 2020
##

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.widgets import Slider
import matplotlib.dates as mdt
import datetime
import sys
import math
import pandas as pd
import os.path

import woqlDiagnosis as wary
import woqlclient.woqlClient as woql
from woqlclient import WOQLQuery
import woqlclient.errors as woqlError
import woqlclient.woqlDataframe as wdf



########################################################################################################################


VOYAGES_CSV                         = "voyages.csv"
DOCKINGS_CSV                        = "dockings.csv"
                                    # Filenames containing the raw data
                                    # Remember to set your TERMINUS_LOCAL environment variable
                                    # appropriately to reach this as a local file:  see
                                    #    https://medium.com/terminusdb/loading-your-local-files-in-terminusdb-e0b5dfbe59b4

MAP_FILE                        = "./port.png"

server_url                      = "http://localhost:6363"
dbId                            = "shippingDB"
key                             = "root"
dburl                           = server_url + "/" + dbId


DOT_SIZE                = 10                # size of ship icons

ROUTE_WEIGHTS           = (20, 20, 960)     # weight of each three-segment voyage

CLICK_TOL               = 2.0               # tolerance when clicking on/near ships

NR_STEPS                = 1000              # number of steps in a voyage (across the map)

TRANSIT_TIME            = 1                 # assumed number of hours for a voyage (across the map)


START_DATETIME          = datetime.datetime(2020, 4, 28, 15, 0, 0)  # start date/time of raw data
END_DATETIME            = datetime.datetime(2020, 4, 30, 15, 0, 0)  # end date/time of raw data


#
#  Derive some floating number equivalents of various tumes and intervals..
#
Start_DateTime_Num      = mdt.date2num(START_DATETIME)
End_DateTime_Num        = mdt.date2num(END_DATETIME)


Transit_Time_Num = mdt.date2num(datetime.datetime(2020,4,2,12+TRANSIT_TIME,0,0)) - mdt.date2num(datetime.datetime(2020,4,2,12,0,0))
One_Second_Num = mdt.date2num(datetime.datetime(2020,4,2,12,0,1)) - mdt.date2num(datetime.datetime(2020,4,2,12,0,0))


########################################################################################################################
#
#  Locate specific points on the map of Dublin port - hard-coded
#
#  Each point is a latitude, longitude pair
#

#
#  Bounding box for the map
#
BBox = (-6.2177,  -6.1527, 53.3317, 53.3622)
BBoxXDist = BBox[1] - BBox[0]
BBoxYDist = BBox[3] - BBox[2]

#
#  Locations of various berths at the port
#
Berth1 = (-6.195000, 53.3497)           # The Stena Ferries berth
Berth2 = (-6.194800, 53.349400)         # The Irish Ferries berth
Berth3 = (-6.193000, 53.349900)         # Cargo berth
Berth4 = (-6.192600, 53.349900)         # Cargo berth
Berth5 = (-6.196900, 53.349500)         # Cargo berth
Berth6 = (-6.196000, 53.348600)         # Cargo berth

#
#  Locations of various waypoints around the port
#
WP1 = (-6.195000, 53.3487)
WP2 = (-6.194600, 53.3483)
WP3 = (-6.1926, 53.3487)
WP4 = (-6.1920, 53.3485)
WP5 = (-6.196900, 53.3482)
WP6 = (-6.195700, 53.3482)

#
#  Locations of easterly waypoints at the right edge of the map
#  These denote where voyages go off the boundary of the map
#
WPHHIn = (-6.1530, 53.3450)
WPHHOut = (-6.1530, 53.3455)
W4 = (-6.1530, 53.3460)
WPOT = (-6.1530, 53.3465)
W1 = (-6.1530, 53.3470)
WPHH2 = (-6.1530, 53.3475)
WPIOM = (-6.1530, 53.3480)
W2 = (-6.1530, 53.3485)
W3 = (-6.1530, 53.3490)

#
#  Build the routes,  four waypoints and three segments each
#
SegsHH1Out = [Berth1, WP1, WP2, WPHHOut]
SegsHH1In = [WPHHIn, WP2, WP1, Berth1]

SegsHH2Out = [Berth2, WP1, WP2, WPHHOut]
SegsHH2In = [WPHHIn, WP2, WP1, Berth2]

Segs1Out = [Berth3, WP3, WP4, W1]
Segs1In = [W1, WP4, WP3, Berth3]

Segs2Out = [Berth4, WP3, WP4, W2]
Segs2In = [W2, WP4, WP3, Berth4]

Segs3Out = [Berth5, WP5, WP4, W3]
Segs3In = [W3, WP4, WP5, Berth5]

Segs4Out = [Berth6, WP6, WP4, W4]
Segs4In = [W4, WP4, WP6, Berth6]

########################################################################################################################
#
#  Build routes from waypoints
#
def build_Segment(fromWP, toWP, N):
    '''
        Build a segment of N steps from two waypoints

        :param fromWP:      waypoint coordinate pair
        :param toWP:        waypoint coordinate pair
        :param N:           number of steps
        :return:            list of X coordinates, and of Y coordinates for the segment
    '''
    segmentX = [fromWP[0]]
    segmentY = [fromWP[1]]
    xDist = toWP[0] - fromWP[0]
    yDist = toWP[1] - fromWP[1]
    for i in range(1, N):
        x = fromWP[0] + xDist * i / N
        y = fromWP[1] + yDist * i / N
        segmentX.append(x)
        segmentY.append(y)
    return segmentX, segmentY


def build_Route(Segs, Weights, outBound):
    '''
        Build multi-segment route

        :param Segs:            list of waypoints between segments
        :param Weights:         weights associated with each segment
        :param outBound:        whether an inbound or outbound route
        :return:                list of X coordinates, and of Y coordinates for entire route:
                                    from increasing segment points if outbound route
                                    or decreasing segments points if inbound route
    '''
    if len(Segs) != len(Weights) + 1:
        print("build_route inconsistency")
        sys.exit(-1)
    RouteX = []
    RouteY = []
    for i in range(len(Segs)-1):
        j = i if outBound else len(Segs)-2 - i
        xSeg, ySeg = build_Segment(Segs[i], Segs[i+1], Weights[j])
        RouteX.extend(xSeg)
        RouteY.extend(ySeg)
    return RouteX, RouteY

#
#  Build the routes
#
RouteSt_OutX, RouteSt_OutY = build_Route(SegsHH1Out, ROUTE_WEIGHTS, outBound=True)
RouteSt_InX, RouteSt_InY= build_Route(SegsHH1In, ROUTE_WEIGHTS, outBound=False)

RouteIf_OutX, RouteIf_OutY = build_Route(SegsHH2Out, ROUTE_WEIGHTS, outBound=True)
RouteIf_InX, RouteIf_InY = build_Route(SegsHH2In, ROUTE_WEIGHTS, outBound=False)

Route1_OutX, Route1_OutY = build_Route(Segs1Out, ROUTE_WEIGHTS, outBound=True)
Route1_InX, Route1_InY = build_Route(Segs1In, ROUTE_WEIGHTS, outBound=False)

Route2_OutX, Route2_OutY = build_Route(Segs2Out, ROUTE_WEIGHTS, outBound=True)
Route2_InX, Route2_InY = build_Route(Segs2In, ROUTE_WEIGHTS, outBound=False)

Route3_OutX, Route3_OutY = build_Route(Segs3Out, ROUTE_WEIGHTS, outBound=True)
Route3_InX, Route3_InY = build_Route(Segs3In, ROUTE_WEIGHTS, outBound=False)

Route4_OutX, Route4_OutY = build_Route(Segs4Out, ROUTE_WEIGHTS, outBound=True)
Route4_InX, Route4_InY = build_Route(Segs4In, ROUTE_WEIGHTS, outBound=False)


########################################################################################################################
#
#  Tables for the X,Y coordinates of each inbound and outbound route
#
RoutesX = {
    "In1"   : Route1_InX,
    "Out1"  : Route1_OutX,
    "In2"   : Route2_InX,
    "Out2"  : Route2_OutX,
    "In3"   : Route3_InX,
    "Out3"  : Route3_OutX,
    "In4"   : Route4_InX,
    "Out4"  : Route4_OutX,
    "InSt"  : RouteSt_InX,
    "OutSt" : RouteSt_OutX,
    "InIf"  : RouteIf_InX,
    "OutIf" : RouteIf_OutX,
}

RoutesY = {
    "In1"   : Route1_InY,
    "Out1"  : Route1_OutY,
    "In2"   : Route2_InY,
    "Out2"  : Route2_OutY,
    "In3"   : Route3_InY,
    "Out3"  : Route3_OutY,
    "In4"   : Route4_InY,
    "Out4"  : Route4_OutY,
    "InSt"  : RouteSt_InY,
    "OutSt" : RouteSt_OutY,
    "InIf"  : RouteIf_InY,
    "OutIf" : RouteIf_OutY,
}

#
#  Tables of the X,Y coordinates for each berth
#
BerthsX = {
    "B1" : Berth1[0],       # Stena
    "B2" : Berth2[0],       # IrishF
    "B3" : Berth3[0],
    "B4" : Berth4[0],
    "B5" : Berth5[0],
    "B6" : Berth6[0]
}

BerthsY = {
    "B1": Berth1[1],        # Stena
    "B2": Berth2[1],        # IrishF
    "B3": Berth3[1],
    "B4": Berth4[1],
    "B5": Berth5[1],
    "B6": Berth6[1]

}


########################################################################################################################
#
#  Helper functions
#

def sliderToDateTime(x):
    '''
        Convert floating point slider value to corresponding date/time string

        :param x:       slider setting
        :return:        date/time string
    '''
    return str(mdt.num2date(x))[: len("YYYY-MM-DD HH:MM:SS")]


def set_initial_positions():
    '''
        Find the initial positions of ships at the start of the animation

        :return:        list of ships,  and their corresponding X and Y co-ordinates
    '''
    global Start_DateTime_Num, One_Second_Num

    actives, ships = active_Voyages(Start_DateTime_Num + One_Second_Num)
    XposList = [a[0] for a in actives]
    YposList = [a[1] for a in actives]
    return ships, XposList, YposList


def is_empty(q):
    '''
        Test for an empty query result

        :param q:   Woql query result
    '''
    return type(q) is not dict or len(q['bindings']) == 0 or q['bindings'][0] == {}


def literal_string(s):
    '''
        Handle a string value for Woql.

        Woql currently has a bug,  and this function should not really be necessary.
        When bug is fixed,  this function can simply 'return s' - ie become a null function

        :param s:   string value
        :return:    Woql triple for a string literal
    '''
    return {'@type': 'xsd:string', '@value': s}


########################################################################################################################
#
#  Voyage class
#
class Voyage(object):

    def __init__(self, shipName, time, routeX, routeY):
        '''
            Create a new voyage

            :param shipName:        string,  ship name
            :param time:            string, departure time for the voyage
            :param routeX:          list of X (longitude) coordinates for the voyage
            :param routeY:          list of Y (latitude) coordinates for the voyage
        '''
        self.shipName = shipName
        self.ship = None
        self.outBound = routeX[0] < routeX[-1]      # assume outbound easterly routes have increasing longitude..
        if self.outBound:
            self.departure = mdt.date2num(time)
            self.arrival = self.departure + Transit_Time_Num
            if self.departure < Start_DateTime_Num or self.arrival > End_DateTime_Num:
                print("Out of bounds voyage for '{}' departing {}".format(shipName, time))
                sys.exit(-1)
        else:
            self.arrival = mdt.date2num(time)
            self.departure = self.arrival - Transit_Time_Num
            if self.arrival > End_DateTime_Num or self.departure < Start_DateTime_Num:
                print("Out of bounds voyage for '{}' arrving {}".format(shipName, time))
                sys.exit(-1)
        self.routeX = routeX
        self.routeY = routeY


########################################################################################################################
#
#  Annotate ships on the map
#
class AnnotatorClass(object):
    """
        Set up a callback handler for when individual ships on the map are clicked on.

        Each such click enables or disables the associated annotation (ship's name)

        Code is modelled on https://scipy-cookbook.readthedocs.io/items/Matplotlib_Interactive_Plotting.html
    """

    def set_data(self, xdata, ydata, xtol=None, ytol=None):
        '''
            Initialise the annotation data associated with a set of ships:
                for each ship:
                    x and y coordinates
                    previous ('old') x and y coordinates
                    whether the ship is currently annotated
                    the 'phase' of the annotation (to detect stale ships now off the map)

            The coordinate data should match the list of ships already in self.ships

            :param xdata:   X coordinate (longitude) for the ships
            :param ydata:   Y coordinate (latitude) for the ships
            :param xtol:    X tolerance for clicking on ship icon
            :param ytol:    Y tolerance for clicking on ship icon
        '''
        if len(xdata) != len(ydata) or len(xdata) != len(self.ships):
            print("Annotator::coding error on set_data")
            sys.exit(-1)
        for ship, x, y in zip(self.ships, xdata, ydata):
            self.data[ship] = (x, y, x, y, False, self.phase)
        if xtol is None:
            xtol = ((max(xdata) - min(xdata)) / float(len(xdata))) / 2
        if ytol is None:
            ytol = ((max(ydata) - min(ydata)) / float(len(ydata))) / 2
        self.xtol = xtol
        self.ytol = ytol


    def __init__(self, ships, xdata, ydata, ax=None, xtol=None, ytol=None):
        '''
            Create the annotator

            :param ships:       list of ship names
            :param xdata:       list of X coordinates for each of the ships
            :param ydata:       list of Y coordinates for each of the ships
            :param ax:          matplotlib axis
            :param xtol:        X click tolerance factor
            :param ytol:        Y clock tolerance factor
        '''
        self.data = {}
        self.ships = ships
        self.phase = False
        self.set_data(xdata, ydata, xtol, ytol)
        self.ax = plt.gca() if ax is None else ax
        self.activeAnnotations = {}


    def __call__(self, event):
        '''
            Come here when the map is clicked outside of the slider.

            Could be a click on a ship;  or otherwise a general click
            to resume the animation..

            :param event:       Matplotlib click event
        '''
        if event.inaxes:
            clickX = event.xdata
            clickY = event.ydata
            candidates = []
            if (self.ax is None) or (self.ax is event.inaxes):
                for ship, (x, y, oldx, oldy, isActive, _) in self.data.items():
                    #
                    #  Find all ships close to the click co-ordinates
                    #
                    if ((clickX - self.xtol <= x <= clickX + self.xtol) and
                            (clickY - self.ytol <= y <= clickY + self.ytol)):
                        candidates.append((ship, x, y, oldx, oldy))

                if len(candidates) == 0:
                    #
                    #  No click near a ship,  so pass to the slider animation
                    #
                    on_click_slider(event)

                elif len(candidates) == 1:
                    #
                    #  No ambiguity, a click close to a specific ship
                    #
                    self.draw_Annote(*candidates[0])

                else:
                    #
                    #  More than a single ship in the vicinity of the click.
                    #  Take the closest one..
                    #
                    minDist = 10.E20
                    minCand = None
                    for candidate in candidates:
                        dist = self.distance(candidate[1], clickX, candidate[2], clickY)
                        if dist < minDist:
                            minDist = dist
                            minCand = candidate
                    self.draw_Annote(*minCand)


    def distance(self, x1, x2, y1, y2):
        """
            Return the distance between two points
        """
        return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


    def add_Annote(self, ship, x, y, oldX, oldY):
        '''
            Add annotation for a ship

            :param ship:        string,  ship name
            :param x:           X (longitude) coordinate
            :param y:           Y (latitude) coordinate
            :param oldX:        Prior X value
            :param oldY:        Prior Y value
        '''
        if not ship in self.ships:
            self.ships.append(ship)
        text = self.ax.text(x, y, "  {}".format(ship), fontsize='x-small')
        marker = self.ax.scatter([x], [y], marker='d', c='r', zorder=100)
        self.activeAnnotations[(x, y)] = (text, marker)
        self.data[ship] = (x, y, oldX, oldY, True, self.phase)
        self.ax.figure.canvas.draw_idle()


    def remove_Annote(self, ship, x, y, oldX, oldY):
        '''
            Remove the annotation for a ship,  by marking the annotation invsible.

            :param ship:        string,  ship name
            :param x:           X (longitude) coordinate
            :param y:           Y (latitude) coordinate
            :param oldX:        Prior X value
            :param oldY:        Prior Y value
        '''
        markers = self.activeAnnotations[(x, y)]
        for marker in markers:
            marker.set_visible(not marker.get_visible())
        del self.activeAnnotations[(x, y)]
        self.data[ship] = (x, y, oldX, oldY, False, self.phase)
        self.ax.figure.canvas.draw_idle()


    def draw_Annote(self, ship, x, y, oldX, oldY):
        """
            Flip the annotation of a ship on or off

            :param ship:        string,  ship name
            :param x:           X (longitude) coordinate
            :param y:           Y (latitude) coordinate
            :param oldX:        Prior X value
            :param oldY:        Prior Y value
        """
        if (x, y) in self.activeAnnotations:
            self.remove_Annote(ship, x, y, oldX, oldY)
        else:
            self.add_Annote(ship, x, y, oldX, oldY)


    def update_Ship_Position(self, ship, x, y, oldX, oldY):
        '''
            Save the new co-ordinates of a given ship

            :param ship:        string,  ship name
            :param x:           X (longitude) coordinate
            :param y:           Y (latitude) coordinate
            :param oldX:        Prior X value
            :param oldY:        Prior Y value
        '''
        if not ship in self.ships:          # If we have not previously seen this particular ship...
            self.ships.append(ship)         # Add it to the list which was first initialised
            active = False
        else:
            _, _, _, _, active, _ = self.data[ship]
        self.data[ship] = (x, y, oldX, oldY, active, self.phase)


    def get_Coords_Annote(self, ship):
        '''
            Return the annotation data associated with a particular ship.

            :param ship:        string,  ship name
            :return:            x and y coordinates
                                previous ('old') x and y coordinates
                                whether the ship is currently annotated
                                the 'phase' of the annotation (to detect stale ships now off the map)

                                if ship is unknown,  return 0 coordinates and that it is not annotated
        '''
        return self.data.get(ship, (0, 0, 0, 0, False, self.phase))


    def process_ship(self, ship, xPos, yPos):
        '''
            Handle the annotation of a ship for a new step in the animation

            :param ship:        string,  ship name
            :param x:           X (longitude) coordinate
            :param y:           Y (latitude) coordinate
        '''
        x, y, oldx, oldy, isActive, _ = self.get_Coords_Annote(ship)
        newx = xPos if oldx == 0 else x * xPos / oldx       # if oldX/oldY are 0,  ship is not previously known..
        newy = yPos if oldy == 0 else y * yPos / oldy

        if isActive:
            #
            #  Have to remove the old annotation,  and redraw it in correct position
            #
            self.remove_Annote(ship, x, y, xPos, yPos)
            self.add_Annote(ship, newx, newy, xPos, yPos)
        else:
            #
            #  Ship is not currently annotated,  so simply remember its new position
            #
            self.update_Ship_Position(ship, newx, newy, xPos, yPos)


    def start_phase(self):
        '''
            Flip the phase state (to detect stale ships now off the map)
        '''
        self.phase = not self.phase

    def end_phase(self):
        '''
            Conclude a phase.

            Look for ships which were NOT updated during this phase,  and
            assume that they therefore are now off the easterly edge of the map..
        '''
        for ship in self.ships:
            x, y, oldX, oldY, active, phase = self.data[ship]
            if active and phase != self.phase:
                self.remove_Annote(ship, x, y, oldX, oldY)
                self.data[ship] = (0, 0, 0, 0, False, self.phase)

#
#  Declare the Annotator
#
Annotator = None


########################################################################################################################
#
#   Heart of the animation
#
#   Find the locations of ships for a given time/date (as given by the slider),  and update their locations
#
def active_Voyages(currentNumber, af=None):
    '''
        Update locations of ships

        :param currentNumber:   float, date/time value from slider
        :param af:              Annotator object,  or None
        :return:                List of X and Y coordinates of currently operational ships (at this 'currentNumber');
                                List of ships currently operational
    '''
    active = []
    ships = []
    # routes = []               Uncomment if verifying that the raw data is ok
    # berths = []               Uncomment if verifying that the raw data is ok

    if af is not None:
        af.start_phase()

    #
    #  Ask TerminusDB for the current state of the system at this date/time
    #
    df = query_status(currentNumber)
    if len(df) == 0:
        return active, ships

    for ship, shipdf in df.groupby("Ship"):
        #
        #  For each ship currently operating..
        #
        route = shipdf["Route"].values[0]           # There can only be a single operable route for this ship
        if route != "unknown":                      # If 'unknown'  then the ship is berthed, and not underway
            #
            # Uncomment,  to sanity check the raw data
            #
            # if ship in ships:
            #     print("Data error '{}' twice".format(ship))
            #     sys.exit(-1)
            # if route in routes:
            #     print("Data error '{}' twice".format(route))
            #     sys.exit(-1)
            # routes.append(route)
            ships.append(ship)

            #
            #  Derive the X and Y plot positions, based on the 'point' index
            #    value into the Route tables;  and process the ship
            #
            start = shipdf["Start"].values[0]
            point = int(NR_STEPS * ((currentNumber - mdt.date2num(start)) / Transit_Time_Num))
            xPos = RoutesX[route][point]
            yPos = RoutesY[route][point]
            active.append((xPos, yPos))
            if af is not None:
                af.process_ship(ship, xPos, yPos)

        else:
            #
            #  Ship is currently berthed
            #
            berth = shipdf["Berth"].values[0]       # There can only be a single operable berth for this ship
            if berth == "unknown":
                continue                            # Should in fact never come here

            #
            # Uncomment,  to sanity check the raw data
            #
            # if ship in ships:
            #     print("Data error '{}' twice".format(ship))
            #     sys.exit(-1)
            # if berth in berths:
            #     print("Data error '{}' twice".format(berth))
            #     sys.exit(-1)
            # routes.append(route)
            ships.append(ship)

            #
            #  Derive the X and Y plot positions, and process the ship
            #
            xPos = BerthsX[berth]
            yPos = BerthsY[berth]
            active.append((xPos, yPos))
            if af is not None:
                af.process_ship(ship, xPos, yPos)

    if af is not None:
        #
        #  Mark the end of a phase,  and so look for any ships now off the map..
        #
        af.end_phase()

    #
    # Return coordinate lists and names of ships, currently operational
    #
    return active, ships


########################################################################################################################
#
#   Slider and animation controls
#
#   Derived from https://stackoverflow.com/questions/46325447/animated-interactive-plot-using-matplotlib
#

is_manual = False                               # True if user has taken control of the animation
interval = 100                                  # ms, time between animation frames
scale = 0.00166666666666666                     # Controls animation movement across the map


def process_slider(val):
    '''
        Come here when slider is changed (manually, or by the animation)

        :param val:         float,  slider value (corresponding to a specific date/time)
    '''
    global Annotator
    currentDateTime = sliderToDateTime(sfreq.val)       # convert slider value to date/time
    txt.set_text("{}".format(currentDateTime))          # Display the date/time above the slider

    actives, _  = active_Voyages(sfreq.val, Annotator)  # Get the list of X,Y coordinates for currently operational ships
    if actives == []:
        scat.set_visible(False)                         # No ships are currently operational..
        return

    scat.set_visible(True)                              # Update the map with the operational ships..
    scat.set_offsets(actives)
    fig.canvas.draw_idle()


def slider_changed(val):
    '''
        Come here when slider is changed,  either manually or by explicit set_val during animation

        :param val:     float, slider value
    '''
    global is_manual
    is_manual = True            # Assume that the slider was moved manually (or else: reset flag after this call)
    process_slider(val)


def update_plot(num):
    '''
        Come here when animation wants to update

        :param num:         ignored
    '''
    global is_manual
    if is_manual:           # If operating in manual mode,  then do not update animation
        return

    #
    #   Update slider, in animation (moving it towards its maximum (right-hand) setting
    #
    val = (sfreq.val + scale) % sfreq.valmax
    sfreq.set_val(val)      # Will call back out to slider_changed above,  via mahplotlib
    is_manual = False       # was set by slider_changed,  so need to reset this again
    return


def on_click_slider(event):
    '''
        Click handler for the slider

        :param event:       matplotlib event
    '''
    (xm,ym),(xM,yM) = sfreq.label.clipbox.get_points()
    if xm < event.x < xM and ym < event.y < yM:
        #
        # Click was on the slider,
        # but can ignore click since matplotlib will also call update_slider
        #
        return
    else:
        #
        # Click was elsewhere on the map (and not near any ship)
        # Set manual mode,  and so disable animated updates
        global is_manual
        is_manual = False


#######################################################################################################################
#
#   Initialisation of the TerminusDB graph from raw data in .csv files
#

def apply_query_to_url(woqlGet, url):
    '''
        Use either a local file or remote http resource,  to execute a woql get query.
        In the case of a local file,  it should be the file path relative to the value of the
        TERMINUS_LOCAL environment variable set when the TerminusDB server was started...

        :param woqlGet:         a woql get query
        :param url:             string,  eiher a local file name or http-style url
        :return:                return value from executing the woql get
    '''
    if url.startswith("http"):
        return woqlGet.remote(url)
    if not url.startswith("/app/local_files/"):
        url = "/app/local_files/" + url
    return woqlGet.file(url)


def create_schema(client):
    '''
        Build the schema.

        For this demo,  there is a base document to capture ephemeral events.

        There is then a derived document for Voyage events, and Berth events.

        Note especially that ANY property common to all the derived documents MUST be put in the
        base:
            Here, the 'ship' property is common to both 'Voyage' and 'Berth' and so must be in the
            'Ship_Event' document.  If instead it is placed in 'Voyage' and also in 'Base',  then
            TerminusDB will complain with a "class subsumption" error...

        :param client:      TerminusDB server handle
    '''

    base = WOQLQuery().doctype("Ship_Event").label("Ship Event").description("An ephemeral")
    base.property("ship", "string").label("Ship Name")
    base.property("start", "dateTime").label("Existed From")    # try "dateTime rather than string?
    base.property("end", "dateTime").label("Existed To")

    voyage = WOQLQuery().add_class("Voyage").label("Voyage").description("Ship movement").parent("Ship_Event")
    voyage.property("route", "string").label("Route")

    docking = WOQLQuery().add_class("Docking").label("Docking").description("A ship docked at a berth").parent("Ship_Event")
    docking.property("berth", "string").label("Berth")

    schema = WOQLQuery().when(True).woql_and(
        base,
        docking,
        voyage
    )
    try:
        print("[Building schema..]")
        with wary.suppress_Terminus_diagnostics():
            schema.execute(client)
    except Exception as e:
        wary.diagnose(e)



def get_csv_variables(url, voyages):
    '''
        Read a .csv file,  and use some or all of its columns to initialise
        the doctypes established in the schema.

        :param url:         string,  either local file name (relative to TERMINUS_LOCAL env. var.) or remote URL
        :param voyages:     boolean,  whether a Voyage or Berth document set are to be created
        :return:            result of executing a woql get query on the .csv file
    '''
    #
    #  The first parameter in each woql_as must be a column name from the .csv
    #
    wq = WOQLQuery().get(
                WOQLQuery().woql_as("voyage", "v:Voyage").
                            woql_as("start", "v:Start").
                            woql_as("end", "v:End").
                            woql_as("ship", "v:Ship").
                            woql_as("route", "v:Route")
            if voyages else
                WOQLQuery().woql_as("docking", "v:Docking").
                            woql_as("start", "v:Start").
                            woql_as("end", "v:End").
                            woql_as("berth", "v:Berth").
                            woql_as("ship", "v:Ship")
        )
    return apply_query_to_url(wq, url)


def get_wrangles(voyages):
    '''
        Assign TerminusDB unique identifiers for each instance of the schema doctypes,  using the
        lists of .csv column data (one instance for each row of each column;  one column per doctype)

        :return:        list of woql queries,  each of which is an idgen
    '''
    if voyages:
        return [WOQLQuery().idgen("doc:Voyage", ["v:Voyage"], "v:Voyage_ID"),
                WOQLQuery().cast("v:Start", "xsd:dateTime", "v:Start_Time"),
                WOQLQuery().cast("v:End", "xsd:dateTime", "v:End_Time")
                ]
    else:
        return [WOQLQuery().idgen("doc:Docking", ["v:Docking"], "v:Docking_ID"),
                WOQLQuery().cast("v:Start", "xsd:dateTime", "v:Start_Time"),
                WOQLQuery().cast("v:End", "xsd:dateTime", "v:End_Time")
                ]


def get_inserts(voyages):
    '''
        Build a query to initialise each instance of each doctype with its corresponding
        properties,  using the raw data previously read in from the .csv file

        :return:    woql query for all the insertions
    '''
    if voyages:
        return WOQLQuery().insert("v:Voyage_ID", "Voyage").label("v:Start").\
                    property("start", "v:Start_Time").\
                    property("end", "v:End_Time").\
                    property("route", "v:Route").\
                    property("ship", "v:Ship")
    else:
        return WOQLQuery().insert("v:Docking_ID", "Docking").label("v:Start").\
                    property("start", "v:Start_Time").\
                    property("end", "v:End_Time").\
                    property("berth", "v:Berth").\
                    property("ship", "v:Ship")



def load_csv(client, url, voyages):
    '''
        Read a .csv file and use its raw data to initialise a graph in the TerminusDB server.
        In the case of a local file,  it should be the file path relative to the value of the
        TERMINUS_LOCAL environment variable set when the TerminusDB server was started...

        :param client:      handle on the TerminusDB server
        :param url:         string,  eiher a local file name or http-style url
        :param voyages:     boolean,  whether a Voyage or Berth document set are to be created
        :return:            None
    '''
    csv = get_csv_variables(url, voyages)
    wrangles = get_wrangles(voyages)
    inputs = WOQLQuery().woql_and(csv, *wrangles)
    inserts = get_inserts(voyages)
    answer = WOQLQuery().when(inputs, inserts)
    try:
        print("[Loading raw data from '{}'..]".format(url))
        with wary.suppress_Terminus_diagnostics():
            answer.execute(client)
    except woqlError.APIError as e:
        wary.diagnose(e)


#######################################################################################################################

def query_status(time):
    '''
        Query TerminusDB about the state of the system,  at a particular date/time

        :param time:        string, date/time
        :return:
    '''
    selects = ["v:Ship", "v:Start", "v:End", "v:Route", "v:Berth"]         # so we can return an empty dataframe if no data

    q = WOQLQuery().select(*selects).woql_and(

            #
            #  Look for Events with a start and end times
            #
            WOQLQuery().triple("v:Event", "start", "v:Start"),
            WOQLQuery().triple("v:Event", "end", "v:End"),

            #
            #  Make v:Time the current date/time in which we're interested
            #
            WOQLQuery().cast(literal_string(mdt.num2date(time).strftime('%Y-%m-%d %H:%M:%S')), "xsd:dateTime", "v:Time"),

            #
            #  Want the start time before the current time,  and end time after the current time
            #
            WOQLQuery().less("v:Start", "v:Time"),
            WOQLQuery().greater("v:End", "v:Time"),

            #
            #  Now,  pick up the data which we actually want from the query.
            #  Note that the v:Event will either be a Voyage or a Berth document,  so
            #  use optional query triples to pick up alternative properties..
            #
            WOQLQuery().triple("v:Event", "ship", "v:Ship"),
            WOQLQuery().opt().triple("v:Event", "route", "v:Route"),
            WOQLQuery().opt().triple("v:Event", "berth", "v:Berth")
    )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


#######################################################################################################################
#######################################################################################################################
if __name__ == "__main__":

    #
    #  Connect to TerminusDB, clean out any previous version of the charities database
    #  and build a new version using the raw .csv data
    #
    client = woql.WOQLClient()
    try:
        print("[Connecting to the TerminusDB server..]")
        with wary.suppress_Terminus_diagnostics():
            client.connect(server_url, key)
    except Exception as e:
        wary.diagnose(e)
    try:
        print("[Removing prior version of the database,  if it exists..]")
        with wary.suppress_Terminus_diagnostics():
            client.deleteDatabase(dbId)
    except Exception as e:
        print("[No prior database to delete]")
    try:
        print("[Creating new database..]")
        with wary.suppress_Terminus_diagnostics():
            client.createDatabase(dbId, "Shipping", key=None, comment="Shipping graphbase")
    except Exception as e:
        wary.diagnose(e)
    create_schema(client)

    #
    #  Read the two raw data sets into TerminusDB.
    #
    load_csv(client, VOYAGES_CSV, True)
    load_csv(client, DOCKINGS_CSV, False)

    #
    #  Build the basic plot map
    #
    fig, ax = plt.subplots(figsize=(8,4))
    plt.subplots_adjust(left=0.09, bottom=0.0, right=0.96, top=0.98)
    plt.title('Dublin Port Movements', fontsize=12)

    plt.xlim(BBox[0], BBox[1])                                      # set bounding box on the plot..
    plt.ylim(BBox[2], BBox[3])

    if not os.path.isfile(MAP_FILE):
        print("Cannot find the map file {}".format(MAP_FILE))
        sys.exit(-1)
    zz = plt.imread(MAP_FILE)                                       # load the map file
    ax.imshow(zz, zorder=0,  extent=BBox, aspect= 'equal')          # display the map as the canvas

    textstr = '\n'.join(["Click outside slider for animation",      # set up the explanation text box..
                        "Click on slider to stop, and run manually",
                        "  Then click on ships to see their names"])
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    ax.text(0.02, 0.20, textstr, transform=ax.transAxes, fontsize=8,
            verticalalignment='top', bbox=props)


    ax.margins(x=0)                                                 # move plot so that is touches the y-axis (ie at x=0)
    plt.axis('off')                                                 # disable the longitude/latitude axes

    axcolor = 'lightgoldenrodyellow'                                # set up the slider
    axfreq = plt.axes([0.25, 0.1, 0.65, 0.03], facecolor=axcolor)
    initVal = Start_DateTime_Num
    stepSize = (End_DateTime_Num - Start_DateTime_Num) / NR_STEPS
    sfreq = Slider(axfreq, 'Time', Start_DateTime_Num, End_DateTime_Num, valinit=initVal, valstep=stepSize)
    sfreq.valtext.set_visible(False)
    txt = ax.text(-6.190000, 53.33500, sliderToDateTime(initVal))
    sfreq.on_changed(slider_changed)                                # attach slider click handler

    #
    #  Get the starting positions, and put them into the map plot
    #
    ships, initPosnsX, initPosnsY = set_initial_positions()
    scat = ax.scatter(initPosnsX, initPosnsY, s = DOT_SIZE)

    #
    #  Build the Annotator object
    #
    Annotator = AnnotatorClass(ships,
                                  initPosnsX,
                                  initPosnsY,
                                  ax=ax,
                                  xtol=BBoxXDist * CLICK_TOL / 100.,
                                  ytol=BBoxYDist * CLICK_TOL / 100.)
    fig.canvas.mpl_connect('button_press_event', Annotator)         # attach the click handler

    #
    #  Run the animation
    #
    ani = animation.FuncAnimation(fig, update_plot, interval=interval)
    plt.show()