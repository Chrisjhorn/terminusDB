/** <module> Client
 *
 * Client library support for Swipl Woql.
 *
 *
 * * * * * * * * * * * * * COPYRIGHT NOTICE  * * * * * * * * * * * * * * *
 *                                                                       *
 *  This file is a contributed part of TerminusDB.                       *
 *                                                                       *
 *  TerminusDB is free software: you can redistribute it and/or modify   *
 *  it under the terms of the GNU General Public License as published by *
 *  the Free Software Foundation, under version 3 of the License.        *
 *                                                                       *
 *                                                                       *
 *  TerminusDB is distributed in the hope that it will be useful,        *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 *  GNU General Public License for more details.                         *
 *                                                                       *
 *  You should have received a copy of the GNU General Public License    *
 *  along with TerminusDB.  If not, see <https://www.gnu.org/licenses/>. *
 *                                                                       *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
:- module(client, [
                    % _{}.create/4,
                    % client{}.create_database/5,
                    % client{}.create_database/4,
                    % client{}.create_graph/4,
                    % client{}.connect/1,
                    % client{}.delete_database/2,
                    % update_database/4      % for other swoql modules only..
                  ]).
version('1.0').

:- use_module(library(http/json)).
:- use_module(library(http/http_open)).
:- use_module(library(http/http_client)).
:- use_module(logging).



/******************************************************************************/
/*
 * Utility predicates for rest of the code..
 *
 */

% ends_with(+String, +Ch)
ends_with(String, Ch) :-
  atom(String)
  ->  atomic_concat(Ch, '$', End),
      re_match(End, String, [])
  ;   logging:maybeAbort().


% non_null(+Msg, +Value)
non_null(Msg, Value) :-
  Value == ''
  -> logging:fatal('Have empty value for \'~w\' value when building server endpoint URL', [Msg])
  ;  true.


% get_key_with_default(+Key, +Dict, +Default, -X)
get_key_with_default(Key, Dict, Default, X) :-
  get_dict(Key, Dict, X);
  X = Default.


% build_DB_URI(+Server, +User, +DB, -DB_URI)
build_DB_URI(Server, User, DB, DB_URI) :-
  (ends_with(Server, '/')
  -> atomic_list_concat([Server, 'db/', User, '/', DB], DB_URI)
  ;  atomic_list_concat([Server, '/db/', User, '/', DB], DB_URI)).


% build_DB_URI2(+Server, +DB, +EndPoint, -DB_URI)
build_DB_URI2(Server, DB, EndPoint, DB_URI) :-
  atomic_list_concat([DB, '/', EndPoint], Path),
  build_DB_URI(Server, Path, DB_URI).


% build_prefix(+Postfix, +Account, +DB, -Result)
build_prefix(Postfix, Account, DB, Result) :-
  atomic_list_concat(['terminusdb://', Account, '/', DB, '/', Postfix], Result).


% add_comment(+Template, +Language, +Comment, -Result)
add_comment(Template, Language, Comment, Result) :-
  Comment == ""
  -> Result = Template
  ;  Result = Template.put(_{'rdfs:comment': _{'@language': Language, '@value': Comment}}).


% build_query_url(+Server, +User, +DB, -Query_url)
build_query_url(Server, User, DB, Query_url) :-        % Needs recoding to handle commit branch paths - see python woql's query_url()
  non_null('Server', Server),
  non_null('User', User),
  non_null('Database', DB),
  (ends_with(Server, '/')
  -> atomic_list_concat([Server, 'woql/', User, '/', DB, '/local/branch/master'], Query_url)
  ;  atomic_list_concat([Server, '/woql/', User, '/', DB, '/local/branch/master'], Query_url)).


% build_graph_URL(+Server, +User, +DB, +GType, +GId, -Graph_URI)
build_graph_URL(Server, User, DB, GType, GId, Graph_URI) :-
  non_null('Server', Server),
  non_null('User', User),
  non_null('Database', DB),
  non_null('Graph type', GType),
  non_null('Graph ID', GId),
  (ends_with(Server, '/')
  -> atomic_list_concat([Server, 'graph/', User, '/', DB, '/local/branch/master/schema/main'], Graph_URI)
  ;  atomic_list_concat([Server, '/graph/', User, '/', DB, '/local/branch/master/schema/main'], Graph_URI)).



/**********************************************************************************/
/*
 * Woql Context management
 *
 */
standard_urls(_{
    'doc'   :'terminus:///terminus/document/',
    'layer' :'http://terminusdb.com/schema/layer#',
    'owl'   :'http://www.w3.org/2002/07/owl#',
    'rdf'   :'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs'  :'http://www.w3.org/2000/01/rdf-schema#',
    'ref'   :'http://terminusdb.com/schema/ref#',
    'repo'  :'http://terminusdb.com/schema/repository#',
    'terminus':'http://terminusdb.com/schema/terminus#',
    'vio'   :'http://terminusdb.com/schema/vio#',
    'woql'  :'http://terminusdb.com/schema/woql#',
    'xdd'   :'http://terminusdb.com/schema/xdd#',
    'xsd'   :'http://www.w3.org/2001/XMLSchema#'
}).


% retrieve_context(+Client_Context, -Context)
retrieve_context(Client_Context, Context) :-
  (Client_Context == _{}
   -> standard_urls(Context1)
    ; Context1 = Client_Context),
  Context = Context1.put(['_' : '_:']).


% extract_context(+Result, -Context)
extract_context(Result, Context) :-
   open_string(Result, InStream),
   json_read_dict(InStream, Dict),
   (is_dict(Dict)
   -> (get_dict('@context', Dict, Value)
       -> Context = Value
       ;  Context = _{})
    ; Context = _{}).



/**************************************************************************************************************/
/*
 * Client dicts.
 *
 * API here uses Swipl style of user-defined functions on dicts.
 *     Thus every call should take the form:
 *         Swipl_Unbound = Swipl_bound:function(<parameters>)
 *         e.g. Client2 = Client1.create_database(<parameters)
 *
 * Many of the Client functions update the associated Client dict.
 * Thus,  be wary of using '_'
 *         e.g. _ = Client.function(<parameters>) is unsafe if
 *                     the function updates the Client dict.
 *
 * Each Client dict has:
 *         ac:       the user account
 *         ctxt:     Context returned by the server,  from a connect
 *         db:       Database name at the server
 *         url:      Url for the server
 *         user:     User account name
 *         key:      Authorisation key
 *
 */


/*
 * Construct a new Client record
 *   client{}.create(+Server, +Account, +User, +Key) := client{}
 */
_.create(Server, Account, User, Key) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
  ((nonvar(Server), nonvar(Account), nonvar(User), nonvar(Key))
    -> true
    ;  logging:fatal('\'create\' requires bound arguments..')),
  A = Account,
  C = _{},
  D = '',
  X = Server,
  Y = User,
  Z = Key.


/*
 * connect to a server, as given by the url.
 *
 *  client{}.connect(-Result) := client{}
 *
 * Updates Context record in the Client dict.
 * Result is the dict returned by the http call.
 *
 */
Cli.connect(Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
    (var(Result)
    -> true
    ;  logging:fatal('\'connect\' requires an unbound argument..')),
    dispatch(Cli, Cli.url, 'connect', {}, Result, ''),
    extract_context(Result, C),
    A = Cli.ac, D = Cli.db, X = Cli.url, Y = Cli.user, Z = Cli.key.


/*
 * Get metadata.  Currently unimplemented.
 *
 */
Cli.get_metadata(_, _) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
    logging:fatal('\`get_metadata\` not yet implemented'),
    A = Cli.ac, C = Cli.ctxt, D = Cli.db, X = Cli.url, Y = Cli.user, Z = Cli.key.


/*
 * Delete a database
 *
 *  client{}.delete_database(+DB, -Result) := client{}
 *
 *  Updates db field in Client dict.
 *  Result is the dict returned by the http call.
 */
Cli.delete_database(DB, Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
    ((nonvar(DB), var(Result))
      -> true
      ;  logging:fatal('\'delete_database\' called with incorrect arguments..')),
    build_DB_URI(Cli.url, Cli.user, DB, DB_URI),
    dispatch(Cli, DB_URI, 'delete_database', {}, Result, ''),
    A = Cli.ac, C = Cli.ctxt, D = DB, X = Cli.url, Y = Cli.user, Z = Cli.key.


/*
 * Create a database
 *
 *  client{}.create_database(+DB, +Label, +Description, +Include_Schema, -Result) := client{}
 *
 *  Updates db field in Client dict.
 *  Include_Schema is boolean flag on whether new database should implicitly accept new schemas
 *  Result is the dict returned by the http call.
 *
 */
Cli.create_database(DB, Label, Description, Include_Schema, Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
    ((nonvar(DB), nonvar(Label), nonvar(Description), nonvar(Include_Schema), var(Result))
     -> true
     ;  logging:fatal('\'create_database\' called with incorrect arguments..')),
    Details = _{},
    (Label == ''
    -> Details1 = Details
    ;  Details1 = Details.put(['label':Label])),
    (Description == ''
    -> Details2 = Details1
    ;  Details2 = Details1.put(['comment':Description])),
    build_prefix('schema#', Cli.ac, DB, SCM),
    build_prefix('data', Cli.ac, DB, Doc),
    Details3 = Details2.put(['prefixes':_{scm:SCM, doc: Doc}]),
    atom_json_dict(Atom, Details3, []),
    atom_string(Atom, Payload),
    build_DB_URI(Cli.url, Cli.user, DB, DB_URI),
    dispatch(Cli, DB_URI, 'create_database', Payload, Result, Payload),
    (Include_Schema
    -> Cli2 = Cli.create_graph(DB, 'schema', 'main', _)
    ;  Cli2 = Cli),
    A = Cli2.ac, C = Cli2.ctxt, D = DB, X = Cli2.url, Y = Cli2.user, Z = Cli2.key.


%  client{}.create_database(+DB, +Label, +Description, -Result) := client{}
Cli.create_database(DB, Label, Description, Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
  ((nonvar(DB), nonvar(Label), nonvar(Description), var(Result))
   -> true
   ;  logging:fatal('\'create_database\' called with incorrect arguments..')),
  Client = Cli.create_database(DB, Label, Description, true, Result),
  A = Client.ac, C = Client.ctxt, D = Client.db, X = Client.url, Y = Client.user, Z = Client.key.


/*
 * Create a graph
 *
 *  client{}.create_graph(+DB, +GraphType, +GraphId, -Result) := client{}
 *
 *  Updates db field in Client dict.
 *  Result is the dict returned by the http call.
 *
 */
Cli.create_graph(DB, GType, GId, Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
  ((nonvar(DB), nonvar(GType), nonvar(GId), var(Result))
   -> true
   ;  logging:fatal('\'create_graph\' called with incorrect arguments..')),
  (GType == 'schema'; GType == 'instance'; GType == 'inference')
  -> version(Version),
     atomic_concat('Schema graph created by Swipl Client v', Version, CommitMsg),
     CommitDict = _{'commit_info': _{'author': Cli.user, 'message': CommitMsg}},
     build_graph_URL(Cli.url, Cli.user, DB, GType, GId, Graph_URI),
     atom_json_dict(Atom, CommitDict, []),
     atom_string(Atom, Payload),
     dispatch(Cli, Graph_URI, 'create_graph', Payload, Result, Payload),
     A = Cli.ac, C = Cli.ctxt, D = DB, X = Cli.url, Y = Cli.user, Z = Cli.key
  ; logging.fatal('Invalid graph category \'~w\' for \'create_graph\'', [GType]).


% client{}.create_graph(+GraphType, +GraphId, -Result) := client{}
Cli.create_graph(GType, GId, Result) := client{ac: A, ctxt: C, db: D, url:X, user:Y, key:Z} :-
  ((nonvar(GType), nonvar(GId), var(Result))
   -> true
   ;  logging:fatal('\'create_graph\' called with incorrect arguments..')),
  Cli2 = Cli.create_graph(Cli.db, GType, GId, Result),
  A = Cli2.ac, C = Cli2.ctxt, D = Cli2.db, X = Cli2.url, Y = Cli2.user, Z = Cli2.key.



/*********************************************************************************/
/*
 * Update a database
 *
 *  update_database(+Client, +DB, +Query, +CommitRecord, -Result)
 *
 *  Because the Client dict is not updated,  the user-defined function approach
 *  in the Client API would be unnecessary:  a simple predicate suffices.
 *
 *  Calls the server with Query as the payload,  augmented by the current Context
 *  Result is the dict returned by the http call.
 *
 */
update_database(Client, DB, Query, Commit, Result) :-
    retrieve_context(Client.ctxt, Context),
    Query1 = Query.put(['@context' : Context]),
    Query_obj = Commit.put(['query': Query1]),
    atom_json_dict(LogPayload, Query_obj, []),
    atom_string(LogPayload, Payload),
    build_query_url(Client.url, Client.user, DB, Query_url),
    dispatch(Client, Query_url, 'woql_update', Payload, Result, LogPayload).



/*********************************************************************************/
/*
 *  Build log records of incoming and outgoing traffic
 */

% log_outgoing(+Verb, +URL, +LogPayload)
log_outgoing(Verb, URL, LogPayload) :-
 LogPayload == ''
 -> logging:info('Sending ~w to ~w (no payload)', [Verb, URL])
 ;  logging:info('Sending ~w to ~w with payload: ~w', [Verb, URL, LogPayload]).


% log_incoming(+Verb, +Code, +Reply)
log_incoming(Verb, Code, Reply) :-
  logging:info('~w code: ~w reply: ~w', [Verb, Code, Reply]).



/*********************************************************************************/
/*
 *  Interpret some http codes in a reply.
 *
 *  Could usefully be extended to give better hints to the user..
 *
 *  do_result(+URL, +Code)
 */
do_result(URL, Code) :-
    Code == 200
    ->  true
    ;  Code == 404
       ->  write("[Error: No such resource '"), write(URL), write("']"), nl
       ;   write("[Error: Code="), write(Code), write("]"), nl.



/*********************************************************************************/
/*
 *  Dispatcher
 *
 *  Use http get, post, or deletes as appropriate to each action
 *
 *  dispatch(+Client, +URL, +Action, +Payload, -Response, +LogPayload)
 */
dispatch(Client, URL, Action, Payload, Response, LogPayload) :-
    Action == 'connect'
    ->  Payload == {},                                      % empty payload
        dispatch_get(URL, Client, Response, LogPayload)
    ; (Action == 'create_database'; Action == 'create_graph')
      ->  dispatch_post(URL, Client, Payload, Response, LogPayload)
      ;   Action == 'delete_database'
          ->  Payload == {},                               % empty payload
              dispatch_delete(URL, Client, Response, LogPayload)
        ;  Action == 'woql_update'
           ->  dispatch_post(URL, Client, Payload, Response, LogPayload)
           ;   logging:fatal('Unknown dispatch action \'~w\'', [Action]).


% dispatch_get(+URL, +Client, -Reply, +LogPayload)
dispatch_get(URL, Client, Reply, LogPayload):-
  log_outgoing('http:get', URL, LogPayload),
  http_get(URL, Reply, [authorization(basic(Client.user, Client.key)),
                     request_header('Accept'='application/json'),
                     status_code(Code)]),
  log_incoming('http:get', Code, Reply),
  do_result(URL, Code).


% dispatch_get(+URL, +Client, +Payload, -Reply, +LogPayload)
dispatch_get(URL, Client, Payload, Reply, LogPayload):-
  log_outgoing('http:get', URL, LogPayload),
  http_get(URL, atom(Payload), Reply, [authorization(basic(Client.user, Client.key)),
                                        request_header('Accept'='application/json'),
                                        request_header('content-type'='application/json'),
                                        status_code(Code),
                                        to(atom)]),
  log_incoming('http:get', Code, Reply),
  do_result(URL, Code).


% dispatch_delete(+URL, +Client, -Reply, +LogPayload)
dispatch_delete(URL, Client, Reply, LogPayload):-
  log_outgoing('http:delete', URL, LogPayload),
  http_delete(URL, Reply, [authorization(basic(Client.user, Client.key)),
                          request_header('Accept'='application/json'),
                          status_code(Code)]),
  log_incoming('http:delete', Code, Reply),
  do_result(URL, Code).


% dispatch_post(+URL, +Client, +Payload, -Reply, +LogPayload)
dispatch_post(URL, Client, Payload, Reply, LogPayload):-
    log_outgoing('http:post', URL, LogPayload),
    http_post(URL, atom(Payload), Reply, [authorization(basic(Client.user, Client.key)),
                                          request_header('content-type'='application/json'),
                                          status_code(Code),
                                          to(atom)]),
    log_incoming('http:post', Code, Reply),
    do_result(URL, Code).
