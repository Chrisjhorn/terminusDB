##
##  Woql demo using a (fictitious) family tree
##
##  Shows the composition of more complex queries from re-using simpler subqueries.
##  Also shows how to build queries as Python functions,  for query composition.
##
##
##  Chris Horn
##  April 2020
##

import pandas as pd

from random import randint                          # used to overcome current lack of a scoping mechanism in woql
                                                    # Scoped queries will soon be added by the core team..

import woqlclient.woqlClient as woql
from woqlclient import WOQLQuery
import woqlclient.errors as woqlError
import woqlclient.woqlDataframe as wdf

import woqlDiagnosis as wary


#######################################################################################################################

CSV                             = "https://raw.githubusercontent.com/Chrisjhorn/terminusDB/master/family-tree/people.csv"

                               # = "people.csv"     # Filename containing the raw data
                                                    # Remember to set your TERMINUS_LOCAL environment variable
                                                    # appropriately to reach this as a local file:  see
                                                    #    https://medium.com/terminusdb/loading-your-local-files-in-terminusdb-e0b5dfbe59b4

server_url                      = "http://localhost:6363"
dbId                            = "peopleDB"
key                             = "root"
dburl                           = server_url + "/" + dbId

#######################################################################################################################
#
#  Utility functions
#

def local_variable(v):
    '''
        Append a random suffix to a woql variable,  thus making it unique.

        Currently there is no nested scoping mechanism in woql.  Thus the same woql variable used in two
        different subquery clauses can interfere with each other.  Eg, the two "v:X" variables in example here
        collide with each other (they bind to the same query result).

        Future version of woql may have a scoping mechanism like WOQLQuery().select("v:A", "v:B").woql_scope(.....)

                WOQLQuery().woql_or(
                    WOQLQuery().woql_and(
                        WOQLQuery().triple(l"v:X", "property1", "v:Value1"),
                        ....
                    ),
                    WOQLQuery().woql_and(
                        WOQLQuery().triple(l"v:X", "property2", "v:Value2"),
                        ....
                    )
                )

        :param v:       string, woql variable name
        :return:        string, variable plus suffix
    '''
    return v + str(randint(0,1000000))


def is_empty(q):
    '''
        Test for an empty query result

        :param q:   Woql query result
    '''
    return len(q['bindings']) == 0

#######################################################################################################################
#
#   Composable queries
#

def parents_of(child, parent1, parent2):
    '''
        Query to return the parent (doctypes) of a child (doctype).
        Of course, in real life, a child might have a single parent, or be an orphan...

        :param child:   string: woql variable
        :param parent1: string: woql variable
        :param parent2: string: woql variable
        :return:        Woql query result
    '''
    local_P1_Name = local_variable("v:P1_Name")             # Demarcate, because query can be used multiple times
    local_P2_Name = local_variable("v:P2_Name")             # Demarcate, because query can be used multiple times
    return WOQLQuery().woql_and(
        WOQLQuery().triple(child, "Parent1", local_P1_Name),
        WOQLQuery().triple(parent1, "Name", local_P1_Name),
        WOQLQuery().triple(child, "Parent2", local_P2_Name),
        WOQLQuery().triple(parent2, "Name", local_P2_Name),
    )

def parent_of(child, parent, want_mother=True):
    '''
        Query to return the mother or father (doctype) of a child (doctype).
        Of course, in real life, a child might be an orphan...

        :param child:       string: woql variable or literal
        :param parent:      string: woql variable
        :param want_mother: boolean: whether mother or father sought
        :return:            Woql query result
    '''
    local_P1 = local_variable("v:P1")                       # Demarcate, because query can be used multiple times
    local_P2 = local_variable("v:P2")                       # Demarcate, because query can be used multiple times
    sex = "F" if want_mother else "M"

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple(local_P1, "Sex", sex) here,  but instead have to use @type..
    #
    return WOQLQuery().woql_and(
                parents_of(child, local_P1, local_P2),
                WOQLQuery().woql_or(
                    WOQLQuery().woql_and(
                        WOQLQuery().triple(local_P1, "Sex", {"@type": "xsd:string", "@value": sex}),
                        WOQLQuery().eq(local_P1, parent)
                    ),
                    WOQLQuery().woql_and(
                        WOQLQuery().triple(local_P2, "Sex", {"@type": "xsd:string", "@value": sex}),
                        WOQLQuery().eq(local_P2, parent)
                    )
                )
    )


def grandmothers_of(child, grandM1, grandM2):
    '''
        Query to return the two grandmothers (doctype) of a child (doctype).
        Of course, in real life, a child might have 0, 1 or 2 grandmothers

        :param child:       string: woql variable or literal
        :param grandM1:     string: woql variable for first grandmother
        :param grandM2:     string: woql variable for second grandmother
        :return:            Woql query result
    '''
    local_P1 = local_variable("v:P1")             # Demarcate, because query can be used multiple times
    local_P2 = local_variable("v:P2")             # Demarcate, because query can be used multiple times
    return WOQLQuery().woql_and(
                parents_of(child, local_P1, local_P2),
                parent_of(local_P1, grandM1),
                parent_of(local_P2, grandM2)
    )


def grandfathers_of(child, grandF1, grandF2):
    '''
        Query to return the two grandfathers (doctype) of a child (doctype).
        Of course, in real life, a child might have 0, 1 or 2 grandfathers

        :param child:       string: woql variable or literal
        :param grandF1:     string: woql variable for first grandmother
        :param grandF2:     string: woql variable for second grandmother
        :return:            Woql query result
    '''
    local_P1 = local_variable("v:P1")             # Demarcate, because query can be used multiple times
    local_P2 = local_variable("v:P2")             # Demarcate, because query can be used multiple times
    return WOQLQuery().woql_and(
                parents_of(child, local_P1, local_P2),
                parent_of(local_P1, grandF1, want_mother=False),
                parent_of(local_P2, grandF2, want_mother=False)
    )

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
        Build the schema.

        For this example,  it is very simple:
        just a doctype for a Person with various attributes


        :param client:      TerminusDB server handle
    '''
    schema = WOQLQuery().when(True).woql_and(
        WOQLQuery().doctype("Person").
            label("Person").description("Somebody").
            property("Name", "string").
            property("Sex", "string").
            property("Parent1", "string").
            property("Parent2", "string")
    )
    try:
        print("[Building schema..]")
        with wary.suppress_Terminus_diagnostics():
            schema.execute(client)
    except Exception as e:
        wary.diagnose(e)


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
            WOQLQuery().woql_as("Nr", "v:Nr").
                        woql_as("Name", "v:Person").
                        woql_as("Sex", "v:Sex").
                        woql_as("Parent1", "v:Parent1").
                        woql_as("Parent2", "v:Parent2")
        )
    return apply_query_to_url(wq, url)


def get_wrangles():
    '''
        Assign TerminusDB unique identifiers for each instance of the schema doctypes,  using the
        lists of .csv column data (one instance for each row of each column;  one column per doctype)

        :return:        list of woql queries,  each of which is an idgen
    '''
    return [
         WOQLQuery().idgen("doc:Person", ["v:Person"], "v:Person_ID"),
    ]


def get_inserts():
    '''
        Build a query to initialise each instance of each doctype with its corresponding
        properties,  using the raw data previously read in from the .csv file

        :return:    woql query for all the insertions
    '''
    return WOQLQuery().woql_and(

        WOQLQuery().insert("v:Person_ID", "Person").label("v:Person").
            property("Name", "v:Person").
            property("Sex", "v:Sex").
            property("Parent1", "v:Parent1").
            property("Parent2", "v:Parent2")
      )


def load_csv(client, url):
    '''
        Read a .csv file and use its raw data to initialise a graph in the TerminusDB server.
        In the case of a local file,  it should be the file path relative to the value of the
        TERMINUS_LOCAL environment variable set when the TerminusDB server was started...

        :param client:      handle on the TerminusDB server
        :param url:         string,  eiher a local file name or http-style url
        :return:            None
    '''
    csv = get_csv_variables(url)
    wrangles = get_wrangles()
    inputs = WOQLQuery().woql_and(csv, *wrangles)
    inserts = get_inserts()
    answer = WOQLQuery().when(inputs, inserts)
    try:
        print("[Loading raw data from '{}'..]".format(url))
        with wary.suppress_Terminus_diagnostics():
            answer.execute(client)
    except woqlError.APIError as e:
        wary.diagnose(e)


#######################################################################################################################
#
#   Some illustrative woql queries
#

def list_people():
    '''
        Return a dataframe with the name of each person
    '''
    selects = ["v:Name", "v:Sex", "v:Parent1", "v:Parent2"]         # so we can return an empty dataframe if no data
    q = WOQLQuery().select(*selects).woql_and(
            WOQLQuery().triple("v:Person", "Name", "v:Name"),
            WOQLQuery().triple("v:Person", "Sex", "v:Sex"),
            WOQLQuery().triple("v:Person", "Parent1", "v:Parent1"),
            WOQLQuery().triple("v:Person", "Parent2", "v:Parent2")
    )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


def list_parents_of(child=None):
    '''
        Return a dataframe with the name of both parents of a child.
        Of course, in real life, a child might have just a single parent, or be an orphan...

        :param child:       string: woql variable or literal
        :return:            If child is None, then a dataframe with all children and their respective parents.
                            Otherwise, a dataframe with the two parents of the given child
    '''
    selects = ["v:Child_Name", "v:P1_Name", "v:P2_Name"] if child is None else ["v:P1_Name", "v:P2_Name"]

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Child", "Name", child) here if child is a literal,  but instead have to use @type..
    #
    child =  "v:Child_Name" if child is None else {'@type' : 'xsd:string', '@value': child}
    q = WOQLQuery().select(*selects).woql_and(
            parents_of("v:Child", "v:Parent1", "v:Parent2"),
            WOQLQuery().triple("v:Child", "Name", child),
            WOQLQuery().triple("v:Parent1", "Name", "v:P1_Name"),
            WOQLQuery().triple("v:Parent2", "Name", "v:P2_Name")
        )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


def list_father_of(child=None):
    '''
        Return a dataframe with the name of the father of a child.
        Of course, in real life, a child might be an orphan...

        :param child:       string: woql variable or literal
        :return:            If child is None, then a dataframe with all children and their respective fathers.
                            Otherwise, a dataframe with the father of the given child
    '''
    selects = ["v:Child_Name", "v:Father_Name"] if child is None else ["v:Father_Name"]

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Child", "Name", child) here if child is a literal,  but instead have to use @type..
    #
    child =  "v:Child_Name" if child is None else {'@type' : 'xsd:string', '@value': child}
    q = WOQLQuery().select(*selects).woql_and(
            parent_of("v:Child", "v:Father", want_mother=False),
            WOQLQuery().triple("v:Child", "Name", child),
            WOQLQuery().triple("v:Father", "Name", "v:Father_Name")
        )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


def list_mother_of(child=None):
    '''
        Return a dataframe with the name of the mother of a child.
        Of course, in real life, a child might be an orphan...

        :param child:       string: woql variable or literal
        :return:            If child is None, then a dataframe with all children and their respective mothers.
                            Otherwise, a dataframe with the mother of the given child
    '''
    selects = ["v:Child_Name", "v:Mother_Name"] if child is None else ["v:Mother_Name"]

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Child", "Name", child) here if child is a literal,  but instead have to use @type..
    #
    child =  "v:Child_Name" if child is None else {'@type' : 'xsd:string', '@value': child}
    q = WOQLQuery().select(*selects).woql_and(
            parent_of("v:Child", "v:Mother"),
            WOQLQuery().triple("v:Child", "Name", child),
            WOQLQuery().triple("v:Mother", "Name", "v:Mother_Name")
        )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


def list_grandmothers_of(child=None):
    '''
        Return a dataframe with the names of the grandmothers of a child.
        Of course, in real life, a child might have 0,1 or 2 grandmothers

        :param child:       string: woql variable or literal
        :return:            If child is None, then a dataframe with all children and their respective grandmothers.
                            Otherwise, a dataframe with the grandmothers of the given child
    '''
    selects = ["v:Child_Name", "v:GMother1_Name", "v:GMother2_Name"] if child is None else ["v:GMother1_Name", "v:GMother2_Name"]

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Child", "Name", child) here if child is a literal,  but instead have to use @type..
    #
    child =  "v:Child_Name" if child is None else {'@type' : 'xsd:string', '@value': child}
    q = WOQLQuery().select(*selects).woql_and(
            grandmothers_of("v:Child", "v:GMother1", "v:GMother2"),
            WOQLQuery().triple("v:Child", "Name", child),
            WOQLQuery().triple("v:GMother1", "Name", "v:GMother1_Name"),
            WOQLQuery().triple("v:GMother2", "Name", "v:GMother2_Name")
        )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


def list_grandfathers_of(child=None):
    '''
        Return a dataframe with the names of the grandfathers of a child.
        Of course, in real life, a child might have 0,1 or 2 grandfathers.

        :param child:       string: woql variable or literal
        :return:            If child is None, then a dataframe with all children and their respective grandfather.
                            Otherwise, a dataframe with the grandfathers of the given child.
    '''
    selects = ["v:Child_Name", "v:GFather1_Name", "v:GFather2_Name"] if child is None else ["v:GFather1_Name", "v:GFather2_Name"]

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Child", "Name", child) here if child is a literal,  but instead have to use @type..
    #
    child =  "v:Child_Name" if child is None else {'@type' : 'xsd:string', '@value': child}
    q = WOQLQuery().select(*selects).woql_and(
            grandfathers_of("v:Child", "v:GFather1", "v:GFather2"),
            WOQLQuery().triple("v:Child", "Name", child),
            WOQLQuery().triple("v:GFather1", "Name", "v:GFather1_Name"),
            WOQLQuery().triple("v:GFather2", "Name", "v:GFather2_Name")
        )
    result = wary.execute_query(q, client)
    return pd.DataFrame(columns=selects) if is_empty(result) else wdf.query_to_df(result)


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
        print("[TerminusDB server is apparently not running?]")
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
            client.createDatabase(dbId, "People", key=None, comment="People graphbase")
    except Exception as e:
        wary.diagnose(e)
    create_schema(client)
    load_csv(client, CSV)


    #
    #  Some sample queries..
    #

    print("\nList people....")
    df = list_people()
    print("{:,} people found".format(df.shape[0]))

    print("\nList all parents....")
    df = list_parents_of()
    print(df.to_string(index=False))

    print("\nList Mary's parents....")
    df = list_parents_of("Mary")
    print(df.to_string(index=False))

    print("\nList all fathers....")
    df = list_father_of()
    print(df.to_string(index=False))

    print("\nList Mary's father....")
    df = list_father_of("Mary")
    print(df.to_string(index=False))

    print("\nList all mothers....")
    df = list_mother_of()
    print(df.to_string(index=False))

    print("\nList Mary's mother....")
    df = list_mother_of("Mary")
    print(df.to_string(index=False))

    print("\nList all grandmothers....")
    df = list_grandmothers_of()
    print(df.to_string(index=False))

    print("\nList Joe's grandmothers....")
    df = list_grandmothers_of("Joe")
    print(df.to_string(index=False))

    print("\nList all grandfathers....")
    df = list_grandfathers_of()
    print(df.to_string(index=False))

    print("\nList Joe's grandfathers....")
    df = list_grandfathers_of("Joe")
    print(df.to_string(index=False))