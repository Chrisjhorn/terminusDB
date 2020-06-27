# Swoql - Swipl and WOQL
This is an Alpha Release for supporting WOQL (Web Oriented Query Language) in Swipl for the [TerminusDB](https://terminusdb.com/) open source graph base.

## Installation
Install [Swipl](https://www.swi-prolog.org/download/stable).

Download the modules and folders here.

## Brief Summary
Swoql consist of three modules:
* woql.pl - contains swipl support for the asking WOQL queries,  and analysing the results
* client.pl - handles connections to the TerminusDB server in http
* logging.pl - utility for logging activity, reminiscent of the Python [logging facility](https://docs.python.org/3/library/logging.html).

### Introduction
Interactions with the TerminusDB server are handled via the `client.pl` module.  This uses swipl's [user-defined functions on dicts](https://www.swi-prolog.org/pldoc/man?section=ext-dict-user-functions). Thus,  every call to a `client` dict returns a new `client` dict in a style reminscent of function calls in other languages.  An example is:
```
Client1 = client{}.create(Server, Account, User, Key)  % construct new Client
Client2 = Client1.create_database(DB, 'Swoql', 'My first swipl DB!', Result) % create a new database```

WOQL queries are submitted by swoql's ask/3 primitive.  This takes a suitably initialised `client`,  and the query itself,  and returns a result representing the payload returned from the server.  

