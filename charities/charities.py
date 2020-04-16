##
##  Woql demo using Irish charities
##
##  Basically a M:N relationship example: each of M charities can have N trustees,  each of whom was
##  appointed on a specific date.
##
##  Base data is publicly available at:
##     https://www.charitiesregulator.ie/en/information-for-the-public/search-the-register-of-charities
##
##  Although the individual names of Irish charity trustees are published by the Irish Charities Regulator at the
##  this web site,  for the demo these trustee names have been obfuscated in the form "T<number".
##
##  Chris Horn
##  April 2020
##

import requests
import pandas as pd
import os
import sys

import networkx as nx
import matplotlib as mplt

import woqlclient.woqlClient as woql
from woqlclient import WOQLQuery
import woqlclient.errors as woqlError

import woqlclient.woqlDataframe as wdf

#######################################################################################################################

SUPPRESS_TERMINUS_DIAGNOSICS    = True              # whether to hide TerminusDB connection messages

CSV                             = "quads.csv"       # Filename containing the raw data
                                                    # Remember to set your TERMINUS_LOCAL environment variable
                                                    # appropriately to reach this as a local file:  see
                                                    #    https://medium.com/terminusdb/loading-your-local-files-in-terminusdb-e0b5dfbe59b4

PLOT_FILE                       = "charities.png"   # Where to place the plot produced by the networkx module

server_url                      = "http://localhost:6363"
dbId                            = "charitiesDB"
key                             = "root"
dburl                           = server_url + "/" + dbId


#######################################################################################################################

def diagnose(e):
    '''
        Try and provide some help in diagnosing an unexpected exception...and then exit.
    '''
    print(e)
    if type(e) == requests.exceptions.ConnectionError:
        print("Is your TerminusDB server running?..")
    elif type(e) == woqlError.APIError:
        if e.errorObj["terminus:message"][:len("Error: existence_error")] == "Error: existence_error":
            print("Did you forget to correctly set the 'TERMINUS_LOCAL' for the TerminusDB server?..")
        elif e.errorObj["terminus:message"][:len("The variables: ")] == "The variables: ":
            print("Coding error - Possibly one of your schema doctypes is uninitialised from your .csv file?")
    sys.exit(-1)


class suppress_Terminus_diagnostics:
    '''
        Suppress information messages from the TerminusDB libraries.

        At some point,  the woqlclient library will probably have an explicit setting to do this.

        In the meantime,  cf https://stackoverflow.com/questions/8391411
    '''

    def __enter__(self):
        if SUPPRESS_TERMINUS_DIAGNOSICS:
            self._original_stdout = sys.stdout
            sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        if SUPPRESS_TERMINUS_DIAGNOSICS:
            sys.stdout.close()
            sys.stdout = self._original_stdout


def execute_query(q):
    '''
        Carefully do a woql query
        :param q:        a woql query
        :return:         the result of the woql query
    '''
    try:
        with suppress_Terminus_diagnostics():
            result = q.execute(client)
    except woqlError.APIError as e:
        diagnose(e)
    return result

#######################################################################################################################
#
#   Initialisation of the TerminusDB graph from raw data in a .csv file
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
        Build the schema
        :param client:      TerminusDB server handle
    '''
    schema = WOQLQuery().when(True).woql_and(

        #
        # A Charity has a name,  and a registration number
        #
        WOQLQuery().doctype("Charity").
            label("Charity").description("Registered Charity").
            property("charity_name", "string").
            property("charity_number", "decimal").label("Charity Number"),

        #
        #  A Trustee just has an (obfucsated) name
        #
        WOQLQuery().doctype("Trustee").
            label("Trustee").description("A trustee of the charity").
            property("trustee_name", "string"),

        #
        #  An appointment links a specific Trustee to a specific Charity, on a specific date
        #
        WOQLQuery().doctype("Appointed").
            label("Appointed").description("The appointment of a trustee to a charity").
            property("trustee", "Trustee").label("Trustee").
            property("trustee_of", "Charity").label("Appointed to").
            property("date_appointed", "string").label("appointment date")
    )
    try:
        print("[Building schema..]")
        with suppress_Terminus_diagnostics():
            schema.execute(client)
    except Exception as e:
        diagnose(e)


def get_csv_variables(url):
    '''
        Read a .csv file,  and use some or all of its columns to initialise
        the doctypes established in the schema.
        :param url:         string,  either local file name (relative to TERMINUS_LOCAL env. var.) or remote URL
        :return:            result of executing a woql get query on the .csv file
    '''
    #
    #  The first parameter in each woql_as must be a column name from the .csv
    #
    wq = WOQLQuery().get(
            WOQLQuery().woql_as("Appt", "v:Appt").
                        woql_as("Name", "v:Trustee").
                        woql_as("Charity", "v:Charity").
                        woql_as("Registered Number", "v:charity_number").
                        woql_as("Date", "v:Date")
        )
    return apply_query_to_url(wq, url)


def get_wrangles():
    '''
        Assign TerminusDB unique identifiers for each instance of the schema doctypes,  using the
        lists of .csv column data (one instance for each row of each column;  one column per doctype)
        :return:        list of woql queries,  each of which is an idgen
    '''
    return [
         WOQLQuery().idgen("doc:Charity", ["v:Charity"], "v:Charity_ID"),
         WOQLQuery().idgen("doc:Trustee", ["v:Trustee"], "v:Trustee_ID"),
         WOQLQuery().idgen("doc:Appointed", ["v:Appt"], "v:Appointed_ID")
    ]


def get_inserts():
    '''
        Build a query to initialise each instance of each doctype with its corresponding
        properties,  using the raw data previously read in from the .csv file
        :return:    woql query for all the insertions
    '''
    return WOQLQuery().woql_and(
        WOQLQuery().insert("v:Charity_ID", "Charity").label("v:Charity").
            property("charity_name", "v:Charity").
            property("charity_number", "v:charity_number"),

        WOQLQuery().insert("v:Trustee_ID", "Trustee").label("v:Trustee").
            property("trustee_name", "v:Trustee"),

        WOQLQuery().insert("v:Appointed_ID", "Appointed").label("v:Appt").
            property("trustee", "v:Trustee_ID").                    # Important to use Trustee_ID here,  not Trustee
            property("trustee_of", "v:Charity_ID").                 # ditto for Charity_ID - otherwise type subsumption error
            property("date_appointed", "v:Date")
      )


def load_csv(client, url):
    '''
        Read a .csv file and use its raw data to initialise a graph in the TerminusDB server.
        In the case of a local file,  it should be the file path relative to the value of the
        TERMINUS_LOCAL environment variable set when the TerminusDB server was started...
        :param client:      handle on the TerminusDB server
        :param url:         string,  eiher a local file name or http-style url
    :return:
    '''
    csv = get_csv_variables(url)
    wrangles = get_wrangles()
    inputs = WOQLQuery().woql_and(csv, *wrangles)
    inserts = get_inserts()
    answer = WOQLQuery().when(inputs, inserts)
    try:
        print("[Loading raw data from '{}'..]".format(url))
        with suppress_Terminus_diagnostics():
            answer.execute(client)
    except woqlError.APIError as e:
        diagnose(e)

#######################################################################################################################
#
#   Some illustrative woql queries
#

def is_empty(q):
    '''
        Test for an empty query result
        :param q:   Woql query result
    '''
    return len(q['bindings']) == 0


def list_all_charities():
    '''
        Return a dataframe with the registration number and name of each charity
    '''
    q = WOQLQuery().select("v:number", "v:Charity_Name").woql_and(
            WOQLQuery().triple("v:Charity", "charity_name", "v:Charity_Name"),
            WOQLQuery().triple("v:Charity", "charity_number", "v:number"))
    result = execute_query(q)
    return pd.DataFrame(columns=["number", "Charity_Name"]) if is_empty(result) else wdf.query_to_df(result)


def list_all_trustees():
    '''
        Return a dataframe with the (obfuscated) name of each trustee
    '''
    q = WOQLQuery().select("v:Trustee_Name").woql_and(
            WOQLQuery().triple("v:Trustee", "trustee_name", "v:Trustee_Name")
    )
    result = execute_query(q)
    return pd.DataFrame(columns=["Trustee_Name"]) if is_empty(result) else wdf.query_to_df(result)


def list__all_appointments():
    '''
        Return a dataframe with dates on which each trustee was appointed to each charity
    '''
    q = WOQLQuery().select("v:Trustee_Name", "v:Charity_Name", "v:date_appointed").woql_and(
            WOQLQuery().triple("v:Appointment", "trustee", "v:Trustee"),
            WOQLQuery().triple("v:Appointment", "trustee_of", "v:Charity"),
            WOQLQuery().triple("v:Appointment", "date_appointed", "v:date_appointed"),
            WOQLQuery().triple("v:Trustee", "trustee_name", "v:Trustee_Name"),
            WOQLQuery().triple("v:Charity", "charity_name", "v:Charity_Name")
    )
    result = execute_query(q)
    return pd.DataFrame(columns=["Trustee_Name", "Charity_Name", "date_appointed"]) if is_empty(result) else wdf.query_to_df(result)


def lookup_registration(charity):
    '''
        Lookup the registration number for a given charity
        :param charity:     string, charity name
        :return:            integer,  registration number or None if unknown
    '''

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Charity", "charity_name", charity) here but instead use @type..
    #
    q = WOQLQuery().select("v:number").woql_and(
                WOQLQuery().triple("v:Charity", "charity_name", {"@type": "xsd:string", "@value": charity}),
                WOQLQuery().triple("v:Charity", "charity_number", "v:number")
        )
    result = execute_query(q)

    #
    # Could walk the result binding to extract the (sole) decimal value - but easier just to use a dataframe
    #
    return None if is_empty(result) else int(wdf.query_to_df(result)['number'])


def reverse_lookup_registration(regNumber):
    '''
        Lookup the charity with the given registration number
        :param regNumber:       integer, registration number
        :return:                string,  charity name or None if unknown
    '''
    q = WOQLQuery().select("v:Charity_Name").woql_and(
              WOQLQuery().triple("v:Charity", "charity_name", "v:Charity_Name"),
              WOQLQuery().triple("v:Charity", "charity_number", regNumber))
    result = execute_query(q)

    #
    # Could walk the result binding to extract the (sole) string value - but easier just to use a dataframe
    #
    return None if is_empty(result) else wdf.query_to_df(result)["Charity_Name"][0]


def list_charities_for(trustee__name):
    '''
        Find all the charities to which a trustee is appointed
        :param trustee__name:       string, a trustee
        :return:                    dataframe with charities and appointment dates
    '''

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Trustee", "trustee_name", trustee_name) here but instead use @type..
    #
    q = WOQLQuery().select("v:Charity_Name", "v:date_appointed").woql_and(
            WOQLQuery().triple("v:Appointment", "trustee", "v:Trustee"),
            WOQLQuery().triple("v:Appointment", "trustee_of", "v:Charity"),
            WOQLQuery().triple("v:Appointment", "date_appointed", "v:date_appointed"),
            WOQLQuery().triple("v:Trustee", "trustee_name", {"@type": "xsd:string", "@value": trustee__name}),
            WOQLQuery().triple("v:Charity", "charity_name", "v:Charity_Name")
    )
    result = execute_query(q)
    return pd.DataFrame(columns=["Charity_Name", "date_appointed"]) if is_empty(result) else wdf.query_to_df(result)


def query_trustees_for(charity__name):
    '''
        Find all the trustees appointed to a given charity
        :param charity__name:       string, a charity
        :return:                    dataframe with trustees and appointment dates
    '''

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Charity", "charity_name", charity) here but instead use @type..
    #
    q = WOQLQuery().select("v:Trustee_Name", "v:date_appointed").woql_and(
            WOQLQuery().triple("v:Appointment", "trustee", "v:Trustee"),
            WOQLQuery().triple("v:Appointment", "trustee_of", "v:Charity"),
            WOQLQuery().triple("v:Appointment", "date_appointed", "v:date_appointed"),
            WOQLQuery().triple("v:Trustee", "trustee_name", "v:Trustee_Name"),
            WOQLQuery().triple("v:Charity", "charity_name", {"@type": "xsd:string", "@value": charity__name})
    )
    result = execute_query(q)
    return pd.DataFrame(columns=["Trustee_Name", "date_appointed"]) if is_empty(result) else wdf.query_to_df(result)


def sub_query_appointment(N):
    '''
        A utility function to aid the busy_trustees function later below.

        Build a list of queries relating to the appointment to a charity for each trustee.  Each search
        for an appointment requires 5 sub-queries.

        Since the same trustee may be appointed to several charities,  this function builds a query combination
        for N different charities,  all with the same trustee, including 5*N sub-queries.

        There are two steps:  first find a match for what we want -- a set of charities with a common trustee.
        Second step is then simply to extract the various properties from the match,  that we want as the answer..

        :param N:       integer,  the minumum number of charities which each trustee must have, to satisfy the overall query
        :return:        a woql and with a variable set of sub-queries,  depending on the value of N
    '''

    #
    #  First part:  fixed sub-query to pick out the first appointment and charity for some Trustee
    #
    sq1 = [WOQLQuery().triple("v:Appointment1", "trustee", "v:Trustee"),
           WOQLQuery().triple("v:Appointment1", "trustee_of", "v:Charity1")]

    #
    #  Second part: set of sub-queries,  in which each further appointment and charity
    #  are found;  but also ensure they differ from the prior set.  Use WOQLquery().greater here
    #  rather than "not equal",  so we don't end up with repeated pairs of matches.
    #
    #  Note that if the query succeeds beyond the second part,  then we've found a match for what
    #  we're seeking (a set of charities with a common trustee);  and the remaining parts just simply
    #  pick up the various properties we want as the answer.
    #
    sq2 = [(WOQLQuery().triple("v:Appointment{}".format(i), "trustee", "v:Trustee"),
            WOQLQuery().triple("v:Appointment{}".format(i), "trustee_of", "v:Charity{}".format(i)),
            WOQLQuery().greater("v:Charity{}".format(i-1), "v:Charity{}".format(i))
            ) for i in range(2, N+1)]

    #
    #  Third part:  fixed sub-query,  just to pick up the name of the Trustee which we're dealing with
    #
    sq3 = [WOQLQuery().triple("v:Trustee", "label", "v:Trustee_Name")]

    #
    #  Fourth part: set of sub-queries,  picking out the additional properties we want
    #
    sq4 = [(WOQLQuery().triple("v:Charity{}".format(i), "charity_name", "v:Charity_Name{}".format(i)),
            WOQLQuery().triple("v:Appointment{}".format(i), "date_appointed", "v:date_appointed{}".format(i))
            ) for i in range(1, N+1)]

    #
    #  Bring all the sub-queries together as a single list
    #
    sq = sq1 + [item for sublist in sq2 for item in sublist] + \
            sq3 + [item for sublist in sq4 for item in sublist]

    #
    #  Unpack the list of queries into a woql_and,  and give this all back to our caller
    #
    return WOQLQuery().woql_and(*sq)


def busy_trustees(N):
    '''
        Search for trustees appointed to at least N different charities.

        Uses the sub_query_appointment function above,  to build a search pattern in which the appointment of a
        trustee to each of N charities is found;  each search for an appointment requires 5 sub-queries.
        :return:          dataframe with the N charities associated with various trustees,  or None
    '''

    selectList = ["v:Trustee_Name"]
    selectList.extend(["v:Charity_Name{}".format(i) for i in range(1,N+1)])
    selectList.extend(["v:date_appointed{}".format(i) for i in range(1,N+1)])

    q = WOQLQuery().select(*selectList).woql_and(               # NB: in general a list of queries could be given here
                        sub_query_appointment(N))               # with sub_query_appointment(N) as just one component
    result = execute_query(q)
    return None if is_empty(result) else wdf.query_to_df(result)


def query_network(charity_name, trustees=[], charities=[]):
    '''
        Given a specific charity as a "seed",  find all trustees and all charities reachable from
        that specific charity,  via common trustees

        :param charity_name:    string, the seed charity
        :param trustees:        a list of trustees reachable from the seed
        :param charities:       a list of charities reachable from the seed
        :return:                a list of all trustees,  and of all charities,  reachable from the seed charity
    '''
    if charities == []:
        charities = [charity_name]
    trustee_list = query_trustees_for(charity_name)["Trustee_Name"]
    for trustee in trustee_list:
        if trustee in trustees:
            continue
        trustees.append(trustee)
        charities_list = list_charities_for(trustee)["Charity_Name"]
        for charity in charities_list:
            if charity in charities:
                continue
            charities.append(charity)
            trustees, charities = query_network(charity, trustees, charities)
    return trustees, charities


#######################################################################################################################
#
#  Use the standard networx library to plot a network graph
#
def plot_charity(target_charity, trustees):
    '''
        Produce a graph plot showing the charities and trustees reachable from a given charity
        :param target_charity:  string,  a given charity
        :param trustees:        the list of trustees of that charity
    '''
    G = nx.Graph()
    G.add_node(target_charity, font_size=24)
    for trustee in trustees:
        # print("getting charities for {}".format(trustee))                         # uncomment this if you wish...
        charities = list_charities_for(trustee)
        for _, row in charities.iterrows():
            # print("adding edge for {} to {}".format(trustee,row["Charity_Name"])) # uncomment this if you wish...
            G.add_edge(trustee, row["Charity_Name"], date=row["date_appointed"])

    colour_map = []
    for node in G:
        if isinstance(node, str) and len(node) > 2 and node[0] == 'T' and node[1:].isdigit():
            colour_map.append('red')
        else:
            colour_map.append('blue')

    # fig = \
    mplt.pyplot.figure(figsize=(11, 8))
    mplt.pyplot.title(target_charity)
    pos = nx.spring_layout(G)
    nx.draw(G, pos=pos, node_color=colour_map, with_labels=True, font_size=8, node_size=50)
    nx.draw(G.subgraph(target_charity), pos=pos, node_color='green', with_labels=True, font_size=8, node_size=100)
    mplt.pyplot.savefig(PLOT_FILE)
    mplt.pyplot.show()

#######################################################################################################################

if __name__ == "__main__":

    #
    #  Connect to TerminusDB, clean out any previous version of the charities database
    #  and build a new version using the raw .csv data
    #
    client = woql.WOQLClient()
    try:
        print("[Connecting to the TerminusDB server..]")
        with suppress_Terminus_diagnostics():
            client.connect(server_url, key)
    except Exception as e:
        print("[TerminusDB server is apparently not running?]")
        diagnose(e)
    try:
        print("[Removing prior version of the database,  if it exists..]")
        with suppress_Terminus_diagnostics():
            client.deleteDatabase(dbId)
    except Exception as e:
        print("[No prior database to delete]")
    try:
        print("[Creating new database..]")
        with suppress_Terminus_diagnostics():
            client.createDatabase(dbId, "Charities", key=None, comment="Irish Charities graphbase")
    except Exception as e:
        diagnose(e)
    create_schema(client)
    load_csv(client, CSV)

    #
    #  Some sample queries..
    #

    print("\nList all charities....")
    df = list_all_charities()
    print("{:,} charities found".format(df.shape[0]))

    print("\nList all trustees....")
    df = list_all_trustees()
    print("{:,} trustees found".format(df.shape[0]))

    print("\nList all appointments...")
    df = list__all_appointments()
    print("{:,} appointments found".format(df.shape[0]))

    print("\nLookup registration...")
    charity = "Irish Scouting Fellowship"
    nr = lookup_registration(charity)
    print("'{}' has registered number {}".format(charity, 'unknown' if nr is None else nr))

    print("\nReverse lookup of registration number...")
    nr = 20080846
    charity = reverse_lookup_registration(nr)
    print("Registered number {} is '{}'".format(nr, 'unknown' if charity is None else charity))

    print("\nFind charities for a given trustee...")
    trustee = "T1796596693580697126"
    df = list_charities_for(trustee)
    print("Trustee {} is appointed to the following charities".format(trustee))
    print(df)

    print("\nList the trustees of a given charity...")
    df = query_trustees_for("Irish Scouting Fellowship")
    print("The following trustees are appointed to '{}'".format("Irish Scouting Fellowship"))
    print(df)

    print("\nFind sets of charities with a common trustee..")
    N = 3
    df = busy_trustees(N)
    print("There are {:,} sets of {} charities linked by a common trustee".format(0 if df is None else df.shape[0], N))
    cap = min(df.shape[0], 5)
    print("{} of them are:".format(cap))
    pd.set_option('display.max_columns', None)                          # so that we print out all of the columns...
    print(df.head(cap))

    print("\nExtract a subgraph..(may take a few seconds..)")
    target_charity = "Daingean Community Childcare Services Limited"
    trustees, charities = query_network(target_charity)
    print("Plotting subgraph for '{}'...{:,} trustees in subgraph".format(target_charity, len(trustees)))
    plot_charity(target_charity, trustees)






