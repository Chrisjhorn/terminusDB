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

### Quick Start
Interactions with the TerminusDB server are handled via the `client.pl` module.  This uses swipl's [user-defined functions on dicts](https://www.swi-prolog.org/pldoc/man?section=ext-dict-user-functions). Thus,  every call to a `client` dict returns a new `client` dict in a style reminscent of function calls in other languages.  An example is:
```
Client1 = client{}.create(Server, Account, User, Key)                        % construct new Client
Client2 = Client1.create_database(DB, 'Swoql', 'My first swipl DB!', Result) % create a new database
```

WOQL queries are submitted by swoql's `ask/3` primitive.  This takes a suitably initialised `client`,  and the query itself,  and returns a result representing the payload returned from the server.   An example is:
```
 woql:ask(Client,
            triple('v:Person', 'Who', 'v:Value'),
            Result)
```

Swoql uses nested calls,  rather than '.' style chaining (as is used eg in the Python WOQL library). Swipl lists are used for some calls (eg `and` and `select`).  The '^^' operator is used to bind a variable to a type.  The '<<' operator is used to introduce labels and descriptions.  An example of all this is:
```
woql:ask(Client,
         when(true,
              doctype('PersonType',  and([
                                      property('Name'^^'string') << label('first_name'),
                                      property('Age'^^'integer') << label('years')
              ])) << label('Some Person') << description('Somebody')
          ),
         Result_schema)
```
This creates a new schema for `PersonType` documents,  each with two properties.  The `Name` property is a string,  and has an associated label `first_name`.  Likewise the `Age` property is an integer,  and has a label `years`.  Finally each `PersonType` document itself has a label `Some Person` and a description `Somebody`.

WOQL variables can be in the same `v:<identifier>` format as is used in both Javascript and Python WOQL:  for example `'v:person'`.  However,  an alternative format can optionally be used, using a `v()` wrapper:  for example `v(person)` or even `v('person')`.

Swoql has various facilities for analysing the result of a query.  For example:
```
 (woql:empty_response(Result)
 -> format('Empty Result!!..~n')
 ;  woql:process_result(Result, PResult),
    woql:pretty_print(PResult))
```
This tests for an empty result returned by the server.  If the result is non-empty,  `process_result` converts the result into a swipl dict,  which is then pretty printed.

Finally, the logging facility can be used to log all calls and responses (with associated payloads) to the server.  It can also be used directly by a swoql application to place entries into the log.  The log is usually to a file, but it can also be set to `current_output`.  Each log entry has an aassociated date and timestamp.  Log entries can be informational, warnings or errors.  The severity of the three different categories can be filtered out from the log.  Fatal errors can optionally abort the application.

By default, Swoql logs all three categories of entries,  and aborts on a fatal error.  Logging is initialised as (usually at the start of a swoql script):
```
logging:log('logfile.log')
```
This places the log output into `logfile.log` in the current working directory.  An example of an entry in the logfile is:
```
2020-06-27 06:31:01 INFO Sending http:post to http://localhost:6363/woql/admin/Swipl/local/branch/master with payload: {
  "query": {
    "@context": {
      "_":"_:",
      "doc":"terminus:///terminus/document/",
      "layer":"http://terminusdb.com/schema/layer#",
      "owl":"http://www.w3.org/2002/07/owl#",
      "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
      "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
      "ref":"http://terminusdb.com/schema/ref#",
      "repo":"http://terminusdb.com/schema/repository#",
      "terminus":"http://terminusdb.com/schema/terminus#",
      "vio":"http://terminusdb.com/schema/vio#",
      "woql":"http://terminusdb.com/schema/woql#",
      "xdd":"http://terminusdb.com/schema/xdd#",
      "xsd":"http://www.w3.org/2001/XMLSchema#"
    },
    "@type":"woql:Triple",
    "woql:object": {
      "@type":"woql:Variable",
      "woql:variable_name": {"@type":"xsd:string", "@value":"Value"}
    },
    "woql:predicate": {"@type":"woql:Node", "woql:node":"rdfs:label"},
    "woql:subject": {
      "@type":"woql:Variable",
      "woql:variable_name": {"@type":"xsd:string", "@value":"Person"}
    }
  }
}
```
