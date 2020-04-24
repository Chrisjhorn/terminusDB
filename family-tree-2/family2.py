##
##  Woql demo-2 using a (fictitious) family tree.
##
##  This differs from the "vanilla" family tree example in that the database is
##  built using program created data (Python objects),  rather than reading in an external .csv file
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

server_url                      = "http://localhost:6363"
dbId                            = "peopleDB_2"
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
    return type(q) is not dict or len(q['bindings']) == 0 or q['bindings'][0] == {}


#######################################################################################################################
#
#  Define some sample data,  but in-memory (rather than an external .csv file)
#
class Person:

    def __init__(self, sex, father, mother):
        self.sex = sex
        self.father = father
        self.mother = mother

    #
    #  We could add further methods here,  and in particular methods which
    #  deduce and return derived properties for a Person.  An example might be
    #  the grandmothers and grandparents of a Person.  These could then
    #  be implemented using the example woql queries below
    #
    #  For you to try sometime....   :-)
    #


#
#  Build an in-memory dataset...
#
Family = {
    'Joe': Person("M", "Seamus", "Mary"),
    'Siobhan': Person("F", "Mary", "Seamus"),
    'Seamus': Person("M", "Pat", "Cliona"),
    'Mary': Person("F", "Padraig", "Roisin"),
    'Pat': Person("M", "unknown", "unknown"),
    'Cliona': Person("F", "unknown", "unknown"),
    'Padraig': Person("M", "unknown", "unknown"),
    'Roisin': Person("F", "unknown", "unknown")
}



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


def children_of(parent, childVariable="v:Child", childVariableName="v:Child_Name"):
    '''
        Return the names of the children of a parent.

        The optional arguments allow a caller (eg a higher level query,  such as grandchildren_of) to
        control the woql query names of the bindings,  resulting as output.

        :param parent:              string: name of a parent
        :param childVariable:       string: if given,  the woql variable to use for the results
        :param childVariableName:   string: if given,  the woql variable to use for the resulting "Name" property
        :return:                    a woql query for the children
    '''
    local_P1 = local_variable("v:P1")                       # Demarcate, because query can be used multiple times
    local_P2 = local_variable("v:P2")                       # Demarcate, because query can be used multiple times

    #
    #  Terminus currently has a bug with literal values in queries.  Should be able to do:
    #     WOQLQuery().triple("v:Parent1", "Name", parent) but instead have to use @type..
    #
    parentName =  {'@type' : 'xsd:string', '@value': parent} if parent[:2] != "v:" else parent
    return WOQLQuery().woql_and(
        parents_of(childVariable, local_P1, local_P2),
        WOQLQuery().woql_or(
            WOQLQuery().triple(local_P1, "Name", parentName),
            WOQLQuery().triple(local_P2, "Name", parentName),
        ),
        WOQLQuery().triple(childVariable, "Name", childVariableName)
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


def grandchildren_of(gParent):
    '''
        Return the names of the grandchildren of a grandparent.

        Note in this case,  two consecutive calls to the same sub-query "children_of" are made.  The resulting
        bindings from the first call (in effect the children of the grandparent/parents of the grandchildren) are
        passed to the second call.  To avoid scope clashes of "v:Child" in the subquery,  a new local variable
        is introduced here,  and used to control the woql variable name used in the first call.

        :param gParent:     string: name of a gParent
        :return:            A dataframe with the grandchildren
    '''
    local_P = local_variable("v:P")     # introduce a local woql variable here,  which represents the intermediate bindings
    local_PName = local_P + "_Name"     # the associated "Name" property

    return WOQLQuery().woql_and(
                children_of(gParent, local_P, local_PName), # control the names of the bindings used in the first call
                children_of(local_PName)
        )

#######################################################################################################################
#
#   Initialise the schema
#

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


def list_children_of(parent):
    '''
        Return a dataframe with the names of the children of a parent.

        :param parent:      string: name of a parent
        :return:            If child is None, then a dataframe with all children and their respective mothers.
                            Otherwise, a dataframe with the mother of the given child
    '''
    selects = ["v:Child_Name"]
    q = WOQLQuery().select(*selects).woql_and(
            children_of(parent)
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


def list_grandchildren_of(gParent):
    '''
        Return a dataframe with the names of the grandchildren of a grandparent.

        :param gParent:     string: name of a grandparent
        :return:            a dataframe with all the grandchildren
    '''

    selects = ["v:Child_Name"]
    q = WOQLQuery().select(*selects).woql_and(
            grandchildren_of(gParent)
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
        wary.diagnose(e)
    try:
        print("[Removing prior version of the database,  if it exists..]")
        with wary.suppress_Terminus_diagnostics():
            client.deleteDatabase(dbId)
    except Exception as e:
        print("[No prior database to delete]")
    try:
        print("[Creating new schema..]")
        with wary.suppress_Terminus_diagnostics():
            client.createDatabase(dbId, "People", key=None, comment="People graphbase")
    except Exception as e:
        wary.diagnose(e)
    create_schema(client)

    #
    #  Use the in-memory dataset to initialise the database
    #
    print("[Inserting data into the database..]")
    for person, value in Family.items():

        #
        #  Build a woql query to insert each new instance of the Person document
        #  into the database
        #
        #  The 'when' clause here wraps a transaction-write into the database..
        #
        answer = WOQLQuery().when(

                    #
                    #  Create a new TerminusDB identifier for the new document
                    #
                    WOQLQuery().woql_and(
                                WOQLQuery().idgen("doc:Person", [person], "v:Person_ID"),
                            ),

                    #
                    #  Insert the new document into the database..
                    #
                    #  Note the use of the @types because Terminus currently has a bug with literal values in queries
                    #
                    WOQLQuery().woql_and(
                                  WOQLQuery().insert("v:Person_ID", "Person").label(person).
                                      property("Name", {'@type' : 'xsd:string', '@value': person}).
                                      property("Sex", {'@type' : 'xsd:string', '@value': value.sex}).
                                      property("Parent1", {'@type' : 'xsd:string', '@value': value.mother}).
                                      property("Parent2", {'@type' : 'xsd:string', '@value': value.father})
                        )
            )

        #
        #  We are inserting each Person one at a time into Terminus:  each insertion
        #  involves a call out to the database,  and would thus be slow if there were a lot of data..
        #
        #  We could instead change the woql when clause and both each woql_ands to bundle a number of
        #  idgens and insertions for different documents together.  This then would be more efficient.
        #
        #  For you to try if you wish.. :-)
        #
        try:
            print("[Inserting {}..]".format(person))
            with wary.suppress_Terminus_diagnostics():
                answer.execute(client)
        except Exception as e:
            wary.diagnose(e)



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

    print("\nList Seamus's children....")
    df = list_children_of("Seamus")
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

    print("\nList Roisin's grandchildren....")
    df = list_grandchildren_of("Roisin")
    print(df.to_string(index=False))
