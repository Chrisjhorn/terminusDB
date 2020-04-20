##
##  Helper module to provide support for making and diagnosing
##  woqlClient calls.
##
##  Attempts to diagnose unexpected exceptions,  and suggest
##  a possible reason for why things do not work..
##
##  Chris Horn
##  April 2020
##

import os
import sys
import requests

import woqlclient.errors as woqlError



SUPPRESS_TERMINUS_DIAGNOSICS    = True              # whether to hide TerminusDB connection messages


#######################################################################################################################
#
#  Some utility functions
#
def diagnose_api_error(e, terminusMsg, errorMsg, diagnosis):
    '''
        Try and diagnose an API error by giving a hint as to what is wrong

        :param e:               Exception object
        :param terminusMsg:     string, the TerminusDB error message category
        :param errorMsg:        string, the TerminusDB error message header
        :param diagnosis:       string, hint as to perhaps what is wrong
        :return:                boolean, True if the Exception matches the terminusMsg and errorMsg
    '''
    eVal = e.errorObj.get(terminusMsg, None)
    if eVal is None:
        return False
    if eVal[:len(errorMsg)] != errorMsg:
        return False
    print(diagnosis)
    return True


def diagnose_api_witness_error(e, violation, errorMsg, diagnosis):
    '''
        Try and diagnose an API witness error by giving a hint as to what is wrong

        :param e:               Exception object
        :param violation:       string, the TerminusDB violation message
        :param errorMsg:        string, the TerminusDB error message header
        :param diagnosis:       string, hint as to perhaps what is wrong
        :return:                boolean, True if the Exception matches the violation and errorMsg
    '''
    eVal = e.errorObj.get("terminus:status", None)
    if eVal is None:
        return False
    if eVal != "terminus:failure":
        return False
    wDict= e.errorObj["terminus:witnesses"][0]
    if wDict["@type"] != violation:
        return False
    if wDict["vio:literal"][1:len(errorMsg)+1] != errorMsg:
        return False
    print(diagnosis)
    return True


def diagnose_api_query_error(e, errorMsg, diagnosis):
    '''
        Try and diagnose an API query parsing error by giving a hint as to what is wrong

        :param e:               Exception object
        :param errorMsg:        string, the TerminusDB error message header
        :param diagnosis:       string, hint as to perhaps what is wrong
        :return:                boolean, True if the Exception matches the violation and errorMsg
    '''
    eTyp = e.errorObj.get("@type", None)
    if eTyp is None:
        return False
    if eTyp != "vio:WOQLSyntaxError":
        return False
    eMsg = e.errorObj.get("terminus:message", None)
    if eMsg is None:
        return False
    if eMsg != errorMsg:
        return False
    print(diagnosis)
    return True


#######################################################################################################################
#
#   Chief functions
#

def diagnose(e):
    '''
        Try and provide some help in diagnosing an unexpected exception e...and then exit.
    '''
    print(e)
    if type(e) == requests.exceptions.ConnectionError:
        print("Is your TerminusDB server running?..")
    elif type(e) == woqlError.APIError:
        if diagnose_api_error(e, "terminus:message", "Error: existence_error",
                "Did you forget to correctly set the 'TERMINUS_LOCAL' for the TerminusDB server?.."):
            pass
        elif diagnose_api_error(e, "terminus:message", "The variables: ",
                "Coding error:\n" +
                " Possibly one of your schema doctypes is uninitialised from your .csv file?\n"):
            pass
        elif diagnose_api_error(e, "terminus:status", "The variables: ",
                 "Coding error: Possibly one of your schema doctypes is uninitialised from your .csv file?"):
            pass
        elif diagnose_api_witness_error(e, "vio:ViolationWithDatatypeObject", "No such indexed name in get:",
                "Coding error:\n" +
                "   Did you read a .csv column,  but then not use it?\n" +
                "   Or try to read a variable which does not appear in your .csv columns?"):
            pass
        elif diagnose_api_witness_error(e, "vio:ViolationWithDatatypeObject", "Too few values in get:",
                "Data error:\n" +
                "   Is at least one of your rows of your .csv file missing a value?"):
            pass
        elif diagnose_api_query_error(e, "Un-parsable Query",
                "Coding error (malformed query):\n" +
                "   Perhaps you selected a V: variable which is not in the query?\n" +
                "   Or a malformed/mistyped V: variable?\n"):
            pass
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


def execute_query(q, client):
    '''
        Carefully do a woql query
        :param q:        a woql query
        :param client:   TerminusDB server connection
        :return:         the result of the woql query
    '''
    try:
        with suppress_Terminus_diagnostics():
            result = q.execute(client)
    except woqlError.APIError as e:
        diagnose(e)
    return result


