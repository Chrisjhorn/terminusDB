# Swoql - Swipl and WOQL
This is an Alpha Release for supporting WOQL (Web Oriented Query Language) in Swipl for the [TerminusDB](https://terminusdb.com/) open source graph base.

Currently,  the entire set of WOQL primitives available in the Javascript and Python is not yet supported for swipl.  The subset of primitives supported so far is listed [below](#swoql).

I'ld be grateful if you would report an errors,  or suggestions, to me via the TerminusDb [discord](https://discord.com/) server.

## Index:
* [Installation](#installation)
* [Brief Summary](#brief-summary)
* [Quick Start](#quick-start)
* [Short Introduction to swoql (and WOQL)](#short-introduction-to-swoql-and-woql)
* [Client API](#client-api)
* [Woql API](#woql-api)
* [Swoql Verbs](#swoql_verbs)
* [Logging API](#logging-api)

***

## Installation
Install [Swipl](https://www.swi-prolog.org/download/stable).

Download the modules and folders here.

***

## Brief Summary
Swoql consist of three modules:
* swoql.pl - contains swipl support for the asking WOQL queries,  and analysing the results
* client.pl - handles connections to the TerminusDB server in http
* logging.pl - utility for logging activity, reminiscent of the Python [logging facility](https://docs.python.org/3/library/logging.html).

***

## Quick Start
If you are already familiar with swipl,  and either the Javascript or Python binding to WOQL, then here's a short summary of how WOQL is used from swipl.

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

By default, swoql logs all three categories of entries,  and aborts on a fatal error.  Logging is initialised as (usually at the start of a swoql script):
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

***

## Short introduction to swoql (and WOQL)
If you have not used WOQL before,  then here are the essential concepts.

TerminusDB stores "documents",  which are akin to objects or entities in other systems.  Each document can have an arbitrary set of associated properties (such as "name", "height", "age", "width",  etc).  In principle every document in a particular document class has the same set of properties,  but it is also possible to add or delete properties to individual documents without changing its peers in its class.  By the way, document classes can also form inheritance hierarchies.

Each document can form 1:1 or 1:M or M:N relationships with other documents.  Relationships are also documents,  and can have their own set of properties.  For example a marine shipping application may have "ships", "berths", and "routes".  Ships and berths may form a relationship relating to the duration of a docking.  Ships and routes may form a relationship relating to the transit times of particular ships on particular routes.

WOQL queries relate a subject via a predicate to an object,  in a `triple`. The subject of a query may be specified to be a particular value (eg a particular document),  or be represented as WOQL variable.  In the latter case,  all triples which match the predicate and object parts of the triple,  are valid responses to the query.  The responses will have a set of subject documents,  whose predicate and object parts match the query.

Equally,  the predicate and object parts may also be fixed to particular values,  or be represented as WOQL variables.

In swoql, a WOQL variable can be written as `'v:<identifier>'` eg `'v:person'`,  following the same syntax as the Javascript and Python WOQL bindings.  In swoql (but not in Javascript or Python), a WOQL variable can alternatively be written as `v(<identifier>)`,  eg `v(person)`.

Based on the above, the contents of an entire database can be matched as (all subjects, all predicates, all objects):
```
triple(v(alpha), v(beta), v(gamma))
```

More realistically,  the following triple might be used to query all documents which have a `'Name'` property:
```
triple(v(person), 'Name', v(name))
```
In this case, all possible subject and object values are returned,  where the subject and object are related by `'Name'`

In the following query, the triples which have a `'Name'` predicate linking a subject to an object, and the triples which have the very same subjects but with an `'Age'` predicate linking to another object, are returned.  However the `select` verb then indicates only the two sets of objects should be returned:
```
select([v(name), v(age)],
                  where([triple(v(person), 'Name', v(name)),
                         triple(v(person, 'Age', v(age))
```

To actually ask a query in swoql,  the `ask/3` predicate is used:
``` 
swoql:ask(Client,
            select([v(name), v(age)],
                  where([triple(v(person), 'Name', v(name)),
                         triple(v(person, 'Age', v(age))
            Result)
```
Here `Client` should be already bound to a `client` dict from the `client` module, and suitably initialised and connected to a TerminusDB database.  The `Result` will be the (complex structure) result returned by TerminusDB.

Finally, WOQL uses schemas to be able to safely navigate and infer document structures.  A swoql schema for documents which match the above query might be:
```
doctype('personType',  and([
                             property('Name'^^'string'),
                             property('Age'^^'integer')]))
```

I suggest looking at the tutorial examples as a next step.

***

## Client API

### connect(-Result) := client{}
Connect to the server, as given in the client dict. Eg:
```
Client2 = Client.connect(Result)
(swoql:result_success(Result)
-> true
;  logging:fatal('Could not connect to the server!')),
```

### create(+Server, +Account, +User, +Key) := client{} 
Construct a new client dict,  using the four string arguments passed.  Eg:
```
Client = client{}.create('http://localhost:6363', 'admin', 'admin', Key)
```

### create_database(+DB, +Label, +Description, +Include_Schema, -Result) := client{}
Create a new database with the given name,  given label and given description.  If the `Include_Schema` flag is set,  implicitly allow new schemas to be created in the database (otherwise,  schemas need to be created with a special server endpoint for that database).  Eg:
```
 Client2 = Client.create_database(DB, 'swoql', 'My first swoql DB!', Result),
 (swoql:result_success(Result)
 -> true
 ;  logging:fatal('Could not create database!'))
 ```

### create_database(+DB, +Label, +Description, -Result) := client{}
Calls create_database/5 with the `Include_Schema` flag set to true.


### create_graph(+DB, +GraphType, +GraphId, -Result) := client{}
Creates a graph for the named database.  `GraphType` is one of `schema, instance` or `inference`. Eg:
```
Client2 = Client.create_graph('swoql', 'inference', 'my graph', Result),
(swoql:result_success(Result)
-> true
;  format('Could not create graph!~n')),
```

### create_graph(+GraphType, +GraphId, -Result) := client{}
Creates a graph for the database known in the `client` dict.  Eg:
```
Client2 = Client.create_graph('inference', 'my graph', Result),
(swoql:result_success(Result)
-> true
;  format('Could not create graph!~n')),
```


### delete_database(+DB, -Result) := client{}
Delete the named database. Eg:
```
Client2 = Client.delete_database('GDPR data', Result),
(swoql:result_success(Result)
-> true
;  format('Database could not be deleted!~n')),
```
***

## WOQL API

### ask(+Client, +Query, -Reply)
Ask the given query of the database and server as represented by the client,  and return the result.

The query should be any legitimate composition of the [swoql](#swoql) verbs.

### empty_response(+Reply)
True if a reply is empty (i.e. has no bindings).

### get_bindingsget_bindings(+Reply, -BindingsList)
Extract the bindings list from a reply.

### pretty_print(+RepliesList)
Print the contents of a list of results to `current_output`.

### process_result(+Reply, -RepliesList)
Converts a reply into a list of results.  Each result is a dict.

### result_check_statistic(+Category, +Target, +Reply)
True if a reply has the target value for one of its statistical counters.  `Category` should be any of `bindings, deletes inserts` or `transaction_retry_count`.

### result_success(+Result)
True if the result was successful.


***

## Swoql Verbs

### add_quad(subject, predicate, object, graph)
Create a new `quad`.

### add_triple(subject, predicate, object)
Create a new `triple`.

### and([list of swoql verbs])
Form a swoql query by `and-ing` together all the subqueries in the given list.

### as(alias, swoql variable)
Use the `alias` -- a .csv file column name -- to obtain values for the swoql variable.

### cast(swoql variable,  swoql variable^^Type)
Type-cast the first swoql variable into the second swoql variable and associated type.

### concat(string, swoql variable)
Concatenate the given WOQL variable value into the given string.

### delete_triple(subject, predicate, object)
Delete the specified triple.

### delete_quad(subject, predicate, object, graph)
Delete the specified quad.

### description(string)
Introduce a description to a `doctype`.   The label is then associated with the doctype using the `<<` operator: eg
```
doctype('person') << description('details of a person')
```

### doctype(document name, swoql primitive)
Create a new document (category) with given name, and associated fields.  The `swoql primitive` is either a single `property`,  or a list of properties. 

### doctype(document name)
Create a new document (category) with given name (and no associated properties).

### eq(left term, right term)
Test whether the left and right terms (swoql verbs, or compositions of verbs) are equal.

### file(file path, Swoql get)
Read the contents of the local (.csv) file and use it to match against the given swoql `get`. 

### get([list of swoql `as` verbs])
Get and match a list of (typically, .csv data rows) values against the swoql variables given in the associated `as` verbs.

### get_csv(URI or file path, swoql get)
Do a `file` or `remote`,  depending on whether the first parameter is a file path or web URI.

### greater (left term, right term)
Test whether the left term (swoql verb, or compositions of verbs) is greater than the right term (ditto).

### idgen(document name, list of value keys, swoql variable)
Use the list of key values, and document name,  to generate unique keys for the given swoql variable.

### insert(Swoql variable^^Type, swoql qualifier)
Create a new value for the given swoql variable of the specified type,  with associated values (typically properties) given by the swoql verb qualifier.

### insert(Swoql variable^^Type)
Create a new value for the given swoql variable of the specified type.

### label(string)
Introduce a label to a `doctype` or `property` entity.   The label is then associated with the entity using the `<<` operator: eg
```
doctype('person') << label('an individual')
```

### less (left term, right term)
Test whether the left term (swoql verb, or compositions of verbs) is less than the right term (ditto).

### not (swoql primitive)
Test the opposite of the given swoql primitive.

### opt(Swoql primitive)
Specify that the given swoql primitive is optional.

### or([list of swoql verbs])
Form a swoql query by `or-ing` together all the subqueries in the given list.

### property(swoql variable^^Type)
Declare a property into a schema with the given swoql variable name,  and associated type.

### property(Name, Variable or type)
Insert a property with the given name, using the given variable or type.

### quad(subject, predicate, object, graph)
Query whether there are any quads matching the given parameters.

### remote(URL, swoql get)
Read the contents of the remote (.csv) file specified by the URL,  and use it to match against the given swoql `get`. 

### select([list of swoql variables], 'and'/'where' primitive)
Select values only for those swoql variables given in the list, from the query given by the associated `and` or `where`.

### triple(subject, predicate, object, graph)
Query whether there are any triples matching the given parameters.

### when(Swoql query,  Swoql update)
Make the updates specified only for those values which match the given query.

### where([list of swoql verbs])
Synonym for swoql `and`.


***

## Logging API

### error(+Msg, +List)
Make an ERROR log entry with the given message (in swipl standard [format](https://www.swi-prolog.org/pldoc/man?predicate=format/2),  and list of arguments.

### error(+Msg)
Same as `error(Msg, [])`.

### fatal(+Str, +List)
Report an ERROR with message `Str`(in swipl standard [format](https://www.swi-prolog.org/pldoc/man?predicate=format/2) and its parameters in `List`.  Conditionally abort the swoql application.

### fatal(+Str)
Call fatal/2 with an empty `List`.

### get_level(-Level)
Get the current threshold for making log entries.  Only log entry categories equal to or above this theshold will be made.

INFOs messages are level 0.  WARNINGs are level 1. ERRORs are level 2.

### get_stream(+Stream)
Get the current logging stream.

### info(+Msg, +List)
Make an INFO log entry with the given message (in swipl standard [format](https://www.swi-prolog.org/pldoc/man?predicate=format/2),  and list of arguments.

### info(+Msg)
Same as `info(Msg, [])`.

### log(+File, +Level)
Create a log.  

`File` is either `current_output`,  or an absolute or relative (to the current working directory) file path.

`Level` is the threshold level.  Log entries below this level will be suppressed.  INFOs messages are level 0.  WARNINGs are level 1. ERRORs are level 2.

### log()
Same as `log(current_output, 0).`

### log(+File)
Same as `log(File, 0)`.

### set_level(+Level)
Set the threshold for making log entries.  Only log entry categories equal to or above this theshold will be made.

INFOs messages are level 0.  WARNINGs are level 1. ERRORs are level 2.

### set_stream(+Stream)
Set the current stream for logging.

### warning(+Msg, +List)
Make a WARNING log entry with the given message (in swipl standard [format](https://www.swi-prolog.org/pldoc/man?predicate=format/2)),  and list of arguments.

### warning(+Msg)
Same as `warning(Msg, [])`.


