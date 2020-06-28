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
:- module(swoql, [
                  ask/3,
                  empty_response/1,
                  get_bindings/2,
                  pretty_print/1,
                  process_result/2,
                  result_check_statistic/3,
                  result_success/1,
                  op(2, xfx, ^^),         % used for typed identifiers: identifier^^type
                  op(2, yfx, <<)          % used for labels and descriptions: eg doctype << label
                    ]).
version('1.0').

:- use_module(library(dicts)).
:- use_module(library(http/json)).
:- use_module(logging).
:- use_module(client).


:- op(2, xfx, ^^).
:- op(2, yfx, <<).


/*******************************************************************************/
/*
 * Default vocabulary.
 *
 */
vocabulary(_{
    'type': 'rdf:type',
    'label': 'rdfs:label',
    'Class': 'owl:Class',
    'DatatypeProperty': 'owl:DatatypeProperty',
    'ObjectProperty': 'owl:ObjectProperty',
    'Entity': 'tcs:Entity',
    'Document': 'tcs:Document',
    'Relationship': 'tcs:Relationship',
    'temporality': 'tcs:temporality',
    'geotemporality': 'tcs:geotemporality',
    'geography': 'tcs:geography',
    'abstract': 'tcs:abstract',
    'comment': 'rdfs:comment',
    'range': 'rdfs:range',
    'domain': 'rdfs:domain',
    'subClassOf': 'rdfs:subClassOf',
    'string': 'xsd:string',
    'integer': 'xsd:integer',
    'decimal': 'xsd:decimal',
    'email': 'xdd:email',
    'json': 'xdd:json',
    'dateTime': 'xsd:dateTime',
    'date': 'xsd:date',
    'coordinate': 'xdd:coordinate',
    'line': 'xdd:coordinatePolyline',
    'polygon': 'xdd:coordinatePolygon'
}).

/*******************************************************************************/

/*
 * Utilities for use by rest of the module
 */

% begins_with(+String, +Pat)
begins_with(String, Pat) :-
  atomic_list_concat(['^', Pat], Pat2),
  re_match(Pat2, String, []).


% begins_with_pattern_colon(+String, +Pat)
begins_with_pattern_colon(String, Pat) :-
  (compound(String)
  -> String = Pat:_
   ; atomic_list_concat(['^', Pat, ':'], Pat2),
     re_match(Pat2, String, [])).


% begins_with_reserved_colon(+S)
begins_with_reserved_colon(S) :-
   (begins_with_pattern_colon(S, rdfs);
    begins_with_pattern_colon(S, rdf);
    begins_with_pattern_colon(S, owl);
    begins_with_pattern_colon(S, tcs);
    begins_with_pattern_colon(S, terminus);
    begins_with_pattern_colon(S, xsd);
    begins_with_pattern_colon(S, xdd)).


% begins_with_reserved_or_scm_colon(+S)
begins_with_reserved_or_scm_colon(S) :-
  number(S)
  -> false
  ;  (begins_with_pattern_colon(S, scm);
        begins_with_reserved_colon(S)).


% reserved base types
valid_base_type('integer').
valid_base_type('string').
valid_base_type('decimal').
valid_base_type('float').
valid_base_type('dateTime').


% remove_prefix(+String, +Pat, -Result)
remove_prefix(String, Pat, Result) :-
  atom(String)
  -> string_length(Pat, Len1),
     string_length(String, Len2),
     Len3 is Len2 - Len1,
     sub_string(String, Len1, Len3, _, Result)
  ;  false.




/*******************************************************************************/
/*
 * Decompose argument lists in WOQL query
 *
 * Name, Argument list, and the length of the argument list,  are returned
 *
 */

% decompose(+Query, -Name, -Len, -ArgList)
decompose(Query, Name, Len, ArgList) :-
    compound_name_arguments(Query, Name, ArgList),        % swipl library call
    length(ArgList, Len).


% decompose(+Query, -Name, +ExpectedLen, -Len, -ArgList)
%  check argument list length matches the expected length
decompose(Query, Name, ExpectedLen, Len, ArgList) :-
    decompose(Query, Name, Len, ArgList),
    Len == ExpectedLen
    -> true
    ;  logging:fatal('Incorrect number of arguments:: ~w (expected ~w) ~n', [Len, ExpectedLen]).


% decomposeList(+Query, -Name, -ExpectedLen, +Len, +ArgList, +Head, +Tail)
%   Check argument list has expected length,  and expected Head and
decomposeList(Query, Name, ExpectedLen, Len, ArgList, Head, Tail) :-
    decompose(Query, Name, ExpectedLen, Len, ArgList),
    [Head|Tail] = ArgList.


/*******************************************************************************/
/*
 * Check for variable pattern.
 *
 * Swipl WOQL supports variables of the form:
 *    'v:name'      -- akin to Python and Javascript Woql libraries
 *    v(name)
 *    v('Name')
 *
 * Note that because of Prolog rules on naming of identifiers,  a WOQL
 * variable beginning with an uppercase letter has to be quoted:
 *     v('Age')     -- legimate
 *     v(Age)       -- Invalid
 *     v(age)       -- valid
 */

% wvarstr(+S)
wvarstr(S) :-
  number(S)
  -> false
  ;  begins_with_pattern_colon(S, v).

% wvar(+)
wvar(v(I)) :-
  atom(I).
wvar(S) :-
  wvarstr(S).

% var_string(+Arg, -Variable)
%  Extract Variable identifier from Arg
var_string(Arg, Variable) :-
  (wvarstr(Arg)
  -> remove_prefix(Arg, 'v:', Variable)
  ; (wvar(Arg)
     -> decomposeList(Arg, _, 1, _, _, Variable, _)
     ;  false)).



/*******************************************************************************/
/*
 *  Basic JSON support for WOQL
 *
 */

% jsonise_var(+Arg, -Variable)
jsonise_var(Arg, Variable) :-
  (wvarstr(Arg)
  -> Variable = Arg
  ;  (wvar(Arg)
     -> decomposeList(Arg, _, 1, _, _, Var, _),
        atomic_concat('v:', Var, Variable)
     ;  logging:fatal('\'~w\' is not a variable~n', [Arg]))).


% jsonise_var_list(+ArgList, -VarList)
jsonise_var_list(ArgList, VarList) :-
  maplist(jsonise_var, ArgList, VarList).


% jsonise_doc(+Doc, -J)
jsonise_doc(Doc, J) :-
  atomic_list_concat(['doc:', Doc], J).


% jsonise_scm(+Val, -J)
jsonise_scm(Val, J) :-
  atomic_concat('scm:', Val, J).


% literal_dict(+Literal, -Dict)
literal_dict(Literal, Dict) :-
  Dict = _{'@value': Literal, '@language': 'en'}.


%split_variables(+String, -List)
split_variables(String, List) :-
  re_split('v:[0-9A-Za-z_]+', String, L1),
  length(L1, Length),
  (Length == 1
  -> re_split('v\\([0-9A-Za-z_]+\\)', String, List)
  ;  List = L1).



/*******************************************************************************/
/*
 *  Support for WOQL concat
 *
 */

% map_concat_variables(+Var, -Result)
map_concat_variables(Var, Result) :-
  (wvar(Var)
   -> jsonise_var(Var, Var1)
   ;  Var1 = Var),
  Result = _{'@type' : 'xsd:string', '@value' : Var1}.


% process_concat(+String, -Result)
process_concat(String, Result) :-
  split_variables(String, List),
  delete(List, "", Filtered1),
  delete(Filtered1, "", Filtered2),
  maplist(map_concat_variables, Filtered2, Result).



/*******************************************************************************/
/*
 *  maplist variants
 *
 *  maplist_with_param - pass parameter into support function
 *  maplist_with_counter - pass incremental counter value into support function
 *
 */

% maplist_with_param(+F, +Param, +[A|As], -[B|Bs])
maplist_with_param(_, _, [], []).
maplist_with_param(F, Param, [A|As], [B|Bs]) :-
   call(F, Param, A, B),
   maplist_with_param(F, Param, As, Bs).


% maplist_with_counter(+F, +Cnt, +[A|As], -[B|Bs])
maplist_with_counter(_, _, [], []).
maplist_with_counter(F, Cnt, [A|As], [B|Bs]) :-
   call(F, Cnt, A, B),
   Cnt1 is Cnt +1,
   maplist_with_counter(F, Cnt1, As, Bs).



/*******************************************************************************/
/*
 *  Support for lists of asks (queries)
 *
 *    collect_ask_list - ?
 *    do_ask_list - apply ask across a list
 */

% get_ask_dict(+ScopeList, +Element, -DictOut)
get_ask_dict(ScopeList, Element, DictOut) :-
  set_scopes(ScopeList, _{}, Dict1),
  do_ask(Element, _, Dict1, Dict2),
  purge_scopes(Dict2, DictOut).


% collect_ask_list(+List, +DictIn, -DictList)
collect_ask_list(List, DictIn, DictList) :-
  get_scopes(DictIn, ScopeList),
  maplist_with_param(get_ask_dict, ScopeList, List, DictList).


% do_ask_list(+[Verb|Tail], +DictIn, -DictOut)
do_ask_list([], DictIn, DictOut) :-
  DictOut = DictIn.
do_ask_list([Verb|Tail], DictIn, DictOut) :-
  do_ask(Verb, _, DictIn, Dict),
  do_ask_list(Tail, Dict, DictOut).



/*******************************************************************************/
/*
 *  Support for JSON-LD Query Elements
 *
 *    build_query_list - Construct Query List from a list of dicts
 *    append_to_query_list - Append a dict onto an existing Query List
 *    merge_query_lists - Merge two query lists,  adjusting the
 *                            QueryListElement @value counters as needed
 */

% build_query_element(+Cnt, +DictEl, -QueryEl)
build_query_element(Cnt, DictEl, QueryEl) :-
  QueryEl = _{
              '@type'       : 'woql:QueryListElement',
              'woql:index'  : _{'@type': 'xsd:nonNegativeInteger', '@value': Cnt},
              'woql:query'  : DictEl}.


% build_query_list(+DictList, -QueryList)
build_query_list(DictList, QueryList) :-
  maplist_with_counter(build_query_element, 0, DictList, QueryList).


% append_to_query_list(+QListDict, +Dict, +Msg, +Arg, -DictOut)
append_to_query_list(QListDict, Dict, Msg, Arg, DictOut) :-
  (get_dict('woql:query_list', QListDict, QList)
   -> length(QList, QLen),
      build_query_element(QLen, Dict, QueryEl),
      append(QList, [QueryEl], ListOut),
      DictOut = QListDict.put(['woql:query_list' : ListOut])
   ;  logging:fatal('Cannot ~w \'~w\', no \'woql:query_list\'', [Msg, Arg])).


% change_query_index(+Cnt, +QEl1, -QEl2)
change_query_index(Cnt, QEl1, QEl2) :-
  get_dict('woql:query', QEl1, QDict1),
  build_query_element(Cnt, QDict1, QEl2).


% merge_query_lists(+Dict1, +Dict2, -DictOut)
merge_query_lists(Dict1, Dict2, DictOut) :-
  (get_dict('woql:query_list', Dict1, QList1)
   -> length(QList1, QLen1),
      get_dict('woql:query_list', Dict2, QList2),
      maplist_with_counter(change_query_index, QLen1, QList2, QList3),
      append(QList1, QList3, QListOut),
      DictOut = Dict1.put(['woql:query_list' : QListOut])
   ;  get_dict('woql:query_list', Dict2, QList2),
      DictOut = Dict1.put(['woql:query_list' : QList2])).



/*******************************************************************************/
/*
 *  Support for JSON-LD select - build VariableListElement dict
 *
 */

% map_select_variable(+Cnt, +Arg, -Var)
map_select_variable(Cnt, Arg, Var) :-
  var_string(Arg, Arg1),
  Var = _{'@type'               : 'woql:VariableListElement',
          'woql:variable_name'  : _{
                                    '@value'      : Arg1,
                                    '@type'       : 'xsd:string'
                                    },
          'woql:index'          : _{
                                    '@type'       : 'xsd:nonNegativeInteger',
                                    '@value'      : Cnt
                                    }
          }.



/*******************************************************************************/
/*
 *  Support for JSON-LD arrays - build ArrayElement dict,  and list
 *
 */

%map_array_key(+Cnt, +ListEl, -ArrayEl)
map_array_key(Cnt, ListEl, ArrayEl) :-
  (wvar(ListEl)
   -> var_string(ListEl, Variable),
      ArrayEl = _{
                  '@type'               : 'woql:ArrayElement',
                  'woql:variable_name'  : _{'@value': Variable, '@type': 'xsd:string'},
                  'woql:index'          : _{'@type' : 'xsd:nonNegativeInteger', '@value' : Cnt}
                 }
   ; triple_literal(ListEl, DataType),
     ArrayEl = _{
                 '@type'               : 'woql:ArrayElement',
                 'woql:datatype'       : DataType,
                 'woql:index'          : _{'@type' : 'xsd:nonNegativeInteger', '@value' : Cnt}
               }).


% key_list(+List, -ListDict)
key_list(List, ListDict) :-
  maplist_with_counter(map_array_key, 0, List, ArrayList),
  ListDict = _{
                '@type'               : 'woql:Array',
                'woql:array_element'  : ArrayList
                }.



/*******************************************************************************/
/*
 *  Support for JSON-LD triple components
 *
 *  triple_var - WOQL Variable
 *  triple_literal - WOQL literal value
 *  triple_datatype - JSON-LD Datatype for a literal
 *  triple_var_or_datatype - Choose triple_var or triple_datatype as appropriate
 *  triple_subject - JSON-LD for a triple subject
 *  triple_predicate - JSON-LD for a triple predicate
 *  triple_object - JSON-LD for a triple object
 */

% triple_var(+S, -Dict)
triple_var(S, Dict) :-
  (wvar(S)
  -> var_string(S, Var),
     Dict = _{'@type': 'woql:Variable', 'woql:variable_name': _{'@value': Var, '@type': 'xsd:string'}}
  ;  false).


% triple_literal(+Val, -J)
triple_literal(Val, J) :-
  (integer(Val)
  -> J = _{'@type': 'xsd:integer', '@value': Val}
      ;  (float(Val)
         -> J = _{'@type': 'xsd:float', '@value': Val}
         ;  (wvar(Val)
             -> var_string(Val, J)
             ;  ((atom(Val); string(Val))
                -> J = _{'@type': 'xsd:string', '@value': Val, '@language' : 'en'}
                ;  (is_dict(Val)
                   -> J = Val
                   ;  J = _{'@value': Val, '@language' : 'en'}))))).


% triple_datatype(+Data, -Dict)
triple_datatype(Data, Dict) :-
  triple_literal(Data, J),
  Dict = _{'@type': 'woql:Datatype', 'woql:datatype': J}.


% triple_var_or_datatype(+Item, -Dict)
triple_var_or_datatype(Item, Dict) :-
  triple_var(Item, Dict)
   -> true
    ; triple_datatype(Item, Dict).


% triple_subject(+S, -Dict)
triple_subject(S, Dict) :-
  (triple_var(S, Dict)
  -> true
  ;  (begins_with_pattern_colon(S, scm)
      -> J = S
       ; jsonise_doc(S, J)),
     Dict = _{'@type': 'woql:Node', 'woql:node': J}).


% triple_predicate(+S, -Dict)
triple_predicate(S, Dict) :-
  triple_var(S, Dict)
  -> true
  ;  ((begins_with_reserved_colon(S)
     -> J = S
     ;  vocabulary(Vocab),
        (get_dict(S, Vocab, Xtype)
         -> J = Xtype
         ;  jsonise_scm(S, J))),
     Dict = _{'@type': 'woql:Node', 'woql:node': J}).


% triple_object(+S, -Dict)
triple_object(S, Dict) :-
  is_dict(S)
  -> Dict = S
  ;  triple_var(S, Dict)
     -> true
     ;  (begins_with_reserved_or_scm_colon(S)
         -> Dict = _{'@type': 'woql:Node', 'woql:node': S}
         ;  triple_datatype(S, Dict)).



/*******************************************************************************/
/*
 *  Support for WOQL (type)cast
 *
 */
% typecast_type(+T, -Dict)
typecast_type(T, Dict) :-
  (begins_with_reserved_or_scm_colon(T)
   -> Type = T
   ;  (valid_base_type(T)
       -> atomic_concat('xsd:', T, Type)
       ;  logging:fatal('\'~w\' is not a valid base type', [T]))),
  Dict = _{'@type'      : 'woql:Node',
           'woql:node'  : Type}.


/*******************************************************************************/
/*
 *  Support for JSON-LD graph parameter
 *
 */
% quad_graph(+Graph, -Dict)
quad_graph(Graph, Dict) :-
  Dict = _{'@type': 'xsd:string', '@value': Graph}.



/*******************************************************************************/
/*
 *  JSON-LD support for the type of a property
 *
 */

%property_type(+IsSchema, +T, -S, -OwlProperty)
property_type(IsSchema, T, S, OwlProperty) :-
  vocabulary(Vocab),
  (get_dict(T, Vocab, Xtype)
  -> S = Xtype,
     OwlProperty = 'owl:DatatypeProperty'
  ;  (integer(T)
      -> S = _{'@type': 'xsd:integer', '@value': T},
         OwlProperty = 'owl:DatatypeProperty'
      ;  (float(T)
         -> S = _{'@type': 'xsd:float', '@value': T},
            OwlProperty = 'owl:DatatypeProperty'
         ;  ((atom(T); string(T))
             -> (IsSchema
                 -> jsonise_scm(T, S),
                    OwlProperty = 'owl:ObjectProperty'
                 ;  S = _{'@type': 'xsd:string', '@value': T},
                    OwlProperty = 'owl:DatatypeProperty')
             ;  (is_dict(T)
                 -> S = T,
                    OwlProperty = 'owl:DatatypeProperty'
                ;  logging:fatal('Unrecognised type \'~w\'', [T])))))).



/*******************************************************************************/
/*
 *  Scope Management
 *
 *  Scopes are managed as additional dict key-value pairs,  where the key
 * is one of "doc_scope", "insert_scope" or "property-scope".  The value
 * is an identifier particular to the scope (eg doctype name).
 *
 * Scopes are not transmitted in JSON-LD, are so are purged from their dicts
 * once terminated.
 *
 *   start_scope - start a new scope with designated key and value
 *   del_scopes  - delete all listed keys (and their values) in a dict
 *   purge_scopes - remove all possible scopes from a dict
 *   scope_has - return value associated with Expected key, if key exists
 *   get_scopes - return list of scope key-value pairs in a dict
 *   set_scopes - register a list of scopes (key-value) pairs in a dict
 */

scope(doc, doc_scope).
scope(insert, insert_scope).
scope(property, property_scope).


% start_scope(+Kind, +Value, +DictIn, -DictOut)
start_scope(Kind, Value, DictIn, DictOut) :-
  scope(Kind, Key),
  put_dict([Key-Value], DictIn, DictOut).


% del_scopes(+[Key|Keys], +DictIn, -DictOut)
del_scopes([], DictIn, DictOut) :-
  DictOut = DictIn.
del_scopes([Key|Keys], DictIn, DictOut) :-
  (del_dict(Key, DictIn, _, Dict)
  -> del_scopes(Keys, Dict, DictOut)
  ;  del_scopes(Keys, DictIn, DictOut)).


% purge_scopes(+DictIn, -DictOut)
purge_scopes(DictIn, DictOut) :-
  findall(X, scope(_, X), Scopes),
  del_scopes(Scopes, DictIn, DictOut).


% scope_has(+Expected, -Value, -Dict)
scope_has(Expected, Value, Dict) :-
  scope(Expected, Key),
  get_dict(Key, Dict, Value).


% get_scopes(+DictIn, -ScopeList)
get_scopes(DictIn, ScopeList) :-
  findall(X, scope(_, X), Scopes),
  get_scopes(Scopes, DictIn, [], ScopeList).


% get_scopes(+[Scope|Scopes], +DictIn, +ScopeListIn, -ScopeListOut)
get_scopes([], _, ScopeListIn, ScopeListOut) :-
  ScopeListOut = ScopeListIn.
get_scopes([Scope|Scopes], DictIn, ScopeListIn, ScopeListOut) :-
  (get_dict(Scope, DictIn, Value)
  -> scope(Key, Scope),
     append([Key-Value], ScopeListIn, ScopeList)
  ;  ScopeList = ScopeListIn),
  get_scopes(Scopes, DictIn, ScopeList, ScopeListOut).


% set_scopes(+[Scope|ScopeList], +DictIn, -DictOut)
set_scopes([], DictIn, DictOut) :-
  DictOut = DictIn.
set_scopes([Scope|ScopeList], DictIn, DictOut) :-
  Kind-Value = Scope,
  start_scope(Kind, Value, DictIn, Dict),
  set_scopes(ScopeList, Dict, DictOut).



/*******************************************************************************/
/*
 *  Commit Record support
 *
 *  Recursive search over a (possibly complex) query to determine whether
 *  any part of intends to update the target database.
 *
 *  contains_update_check - do_update_check,  and then if resulting boolean is unbound, bind it
 *  do_update_check_for_at_type - succeed if given dict has an @type and update operator
 *  do_update_check - return boolean flag set to true,  if given dict will update
 */

update_operator('woql:AddTriple').
update_operator('woql:DeleteTriple').
update_operator('woql:AddQuad').
update_operator('woql:DeleteQuad').
update_operator('woql:DeleteObject').
update_operator('woql:AddTriple').
update_operator('woql:When').


% contains_update_check(+Dict, -Is_Update)
contains_update_check(Dict, Is_Update) :-
  do_update_check(Dict, Is_Update),
  (var(Is_Update)
  -> Is_Update = false
  ;  true).

% do_update_check_for_at_type(+Dict)
do_update_check_for_at_type(Dict) :-
  get_dict('@type', Dict, Operator),
  update_operator(Operator).


% do_update_check(+Dict, -Is_Update)
do_update_check(Dict, Is_Update) :-
  nonvar(Is_Update)                                       % is the overall result already known?
  -> true                                                 % if so,  just return
   ; (do_update_check_for_at_type(Dict)                   % if not, check the current Dict
     -> Is_Update = true                                  % if the dict directly updates, set the flag
     ;  (get_dict('woql:query', Dict, Dict1)              % if not, look inside the dict and recurse..
         -> do_update_check(Dict1, Is_Update)
         ;  (get_dict('woql:query_list', Dict, List)
             -> foreach(member(X, List), do_update_check(X, Is_Update))
             ;  true))).



/*******************************************************************************/
/*
 *  WOQL file support - insert docker prefix if needed
 *
 */

% docker_prefix(+Resource, -LocalPath)
docker_prefix(Resource, LocalPath) :-
  begins_with(Resource, '/app/local_files/')
   -> LocalPath = Resource
   ;  atomic_concat('/app/local_files/', Resource, LocalPath).


/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*
 *  Swoql Primitives........
 *
 *  Every primitive has the same Swipl signature:
 *      primitive(+ArgumentList, +DictIn, -DictOut)
 */


/*******************************************************************************/
/*
 *  add_quad(subject, predicate, object, graph)
 *
 */
ask_add_quad(ArgList, DictIn, DictOut) :-
  [Subj|[Pred|[Obj|[Graph|_]]]] = ArgList,
  triple_subject(Subj, SubjectDict),
  triple_predicate(Pred, PredicateDict),
  triple_object(Obj, ObjectDict),
  quad_graph(Graph, GraphDict),
  DictOut = DictIn.put(['@type'             : 'woql:AddQuad',
                        'woql:subject'      : SubjectDict,
                        'woql:predicate'    : PredicateDict,
                        'woql:object'       : ObjectDict,
                        'woql:graph'        : GraphDict]).


/*******************************************************************************/
/*
 *  add_triple(subject, predicate, object)
 *
 */
ask_add_triple(ArgList, DictIn, DictOut) :-
  [Subj|[Pred|[Obj|_]]] = ArgList,
  triple_subject(Subj, SubjectDict),
  triple_predicate(Pred, PredicateDict),
  triple_object(Obj, ObjectDict),
  DictOut = DictIn.put(['@type'         : 'woql:AddTriple',
                        'woql:subject'  : SubjectDict,
                        'woql:predicate': PredicateDict,
                        'woql:object'   : ObjectDict]).


/*******************************************************************************/
/*
 *  and([list of Swoql primitives])
 *
 */
ask_and(ArgList, DictIn, DictOut) :-
  [List|_] = ArgList,
  is_list(List)
  -> collect_ask_list(List, DictIn, DictList),
     build_query_list(DictList, AndList),
     Dict2 = DictIn.put(['@type'          : 'woql:And',
                           'woql:query_list' : AndList]),

     ((get_dict('@type', DictIn, Type), Type == 'woql:And')
      -> merge_query_lists(DictIn, Dict2, DictOut)
      ;  DictOut = Dict2)
  ;  logging:fatal('\'and\' requires a list parameter').


/*******************************************************************************/
/*
 *  as(alias, Swoql variable)
 *
 */
ask_as(ArgList, _, DictOut) :-
 [Alias|[Variable|_]] = ArgList,
 (wvar(Variable)
 -> var_string(Variable, Var),
    DictOut = _{
                  '@type'             : 'woql:NamedAsVar',
                  'woql:identifier'   : _{'@type'     : 'xsd:string',
                                          '@value'    : Alias
                                          },
                  'woql:variable_name': _{'@type'     : 'xsd:string',
                                          '@value'    : Var
                                          }
                }
  ; logging:fatal('\'as\' called without a woql variable..')).


/*******************************************************************************/
/*
 *  concat(string, Swoql variable)
 *
 */
ask_concat(ArgList, DictIn, DictOut) :-
 [Input|[Variable|_]] = ArgList,
 (wvar(Variable)
 -> triple_var(Variable, VarDict),
    process_concat(Input, InputList),
    key_list(InputList, ArrayList),
    DictOut = DictIn.put([
                          '@type'             : 'woql:Concatenate',
                          'woql:concat_list'  : ArrayList,
                          'woql:concatenated' : VarDict
    ])
  ; logging:fatal('\'concat\' called without a swoql variable..')).


/*******************************************************************************/
/*
 *  delete_triple(subject, predicate, object)
 *
 */
ask_delete_triple(ArgList, DictIn, DictOut) :-
  [Subj|[Pred|[Obj|_]]] = ArgList,
   triple_subject(Subj, SubjectDict),
   triple_predicate(Pred, PredicateDict),
   triple_object(Obj, ObjectDict),
   DictOut = DictIn.put(['@type'         : 'woql:DeleteTriple',
                         'woql:subject'  : SubjectDict,
                         'woql:predicate': PredicateDict,
                         'woql:object'   : ObjectDict]).


 /*******************************************************************************/
 /*
  *  delete_quad(subject, predicate, object, graph)
  *
  */
ask_delete_quad(ArgList, DictIn, DictOut) :-
   [Subj|[Pred|[Obj|[Graph|_]]]] = ArgList,
   triple_subject(Subj, SubjectDict),
   triple_predicate(Pred, PredicateDict),
   triple_object(Obj, ObjectDict),
   quad_graph(Graph, GraphDict),
   DictOut = DictIn.put(['@type'             : 'woql:DeleteQuad',
                         'woql:subject'      : SubjectDict,
                         'woql:predicate'    : PredicateDict,
                         'woql:object'       : ObjectDict,
                         'woql:graph_filter' : GraphDict]).


/*******************************************************************************/
/*
 *  doctype(document name, Swoql primitive)
 *
 */
ask_doctype(ArgList, DictIn, DictOut) :-
  [DocName|[Qualifier|_]] = ArgList,
  start_scope(doc, DocName, DictIn, Dict1),
  jsonise_scm(DocName, JDocName),
  do_ask(and([add_quad(JDocName, 'rdf:type', 'owl:Class', 'schema/main'),
                add_quad(JDocName, 'rdfs:subClassOf', 'terminus:Document', 'schema/main')]),
           _, Dict1, Dict2),
  do_ask(Qualifier, _, Dict2, DictOut).


/*******************************************************************************/
/*
 *  doctype(document name)
 *
 */
ask_doctype1(ArgList, DictIn, DictOut) :-
  [DocName|_] = ArgList,
  ask_doctype([DocName|[_|_]], DictIn, DictOut).


/*******************************************************************************/
/*
 *  eq(left term, right term)
 *
 */
ask_eq(ArgList, DictIn, DictOut) :-
  [Left|[Right|_]] = ArgList,
  triple_var_or_datatype(Left, LeftDict),
  triple_var_or_datatype(Right, RightDict),
  DictOut = DictIn.put([
                        '@type'       : 'woql:Equals',
                        'woql:left'   : LeftDict,
                        'woql:right'  : RightDict]).


/*******************************************************************************/
/*
 *  file(file path, Swoql primitive)
 *
 */
ask_file(ArgList, DictIn, DictOut) :-
  [Path|[Query|_]] = ArgList,
   do_ask(Query, Verb, DictIn, Dict1),
   Verb == get
   -> DictOut = Dict1.put([
                            'woql:query_resource' :
                                    _{'@type'             : 'woql:FileResource',
                                      'woql:file'         : Path
                                      }])
   ;  logging:fatal('\'file\' has no associated \'get\'').



/*******************************************************************************/
/*
 *  get([list of 'as' primitives])
 *
 */
ask_get(ArgList, DictIn, DictOut) :-
  [AsList|_] = ArgList,
  is_list(AsList)
  -> collect_ask_list(AsList, DictIn, DictList),
     DictOut = DictIn.put([
                           '@type'          : 'woql:Get',
                           'woql:as_vars'   : DictList])
  ;  logging:fatal('\'get\' requires a list parameter').



/*******************************************************************************/
/*
 *  get_csv(URI or file path, Swoql primitive)
 *
 */
ask_get_csv(ArgList, DictIn, DictOut) :-
 [Resource|[Query|_]] = ArgList,
 (begins_with(Resource, 'http')
 -> do_ask(remote(Resource, Query), _, DictIn, DictOut)
 ;  docker_prefix(Resource, LocalPath),
    do_ask(file(LocalPath, Query), _, DictIn, DictOut)).


/*******************************************************************************/
/*
 *  greater(left term, right term)
 *
 */
ask_greater(ArgList, DictIn, DictOut) :-
  [Left|[Right|_]] = ArgList,
  triple_var_or_datatype(Left, LeftDict),
  triple_var_or_datatype(Right, RightDict),
  DictOut = DictIn.put([
                        '@type'       : 'woql:Greater',
                        'woql:left'   : LeftDict,
                        'woql:right'  : RightDict]).


/*******************************************************************************/
/*
 *  idgen(document name, list of value keys, Swoql variable)
 *
 */
ask_idgen(ArgList, DictIn, DictOut) :-
  [DocName|[List|[Variable|_]]] = ArgList,
  is_list(List)
  -> (wvar(Variable)
     -> triple_datatype(DocName, DocDict),
        key_list(List, ListDict),
        triple_var(Variable, VarDict),
        DictOut = DictIn.put([
                              '@type'           : 'woql:IDGenerator',
                              'woql:base'       : DocDict,
                              'woql:key_list'   : ListDict,
                              'woql:uri'        : VarDict
                              ])
      ; logging:fatal('Third argument to \'idgen\' should be swoql variable..'))
  ; logging:fatal('Second argument to \'idgen\' should be a list..').


/*******************************************************************************/
/*
 *  insert(Swoql variable^^Type, Swoql primitive)
 *
 */
ask_insert(ArgList, DictIn, DictOut) :-
  [TypedID|[Qualifier|_]] = ArgList,
  Var^^Type = TypedID
  -> (wvar(Var)
    -> jsonise_var(Var, JVar),
       jsonise_scm(Type, JType),
       start_scope(insert, JVar, DictIn, Dict1),
       do_ask(and([add_triple(JVar, 'rdf:type', JType)]), _, Dict1, Dict2),
       do_ask(Qualifier, _, Dict1, Dict3),
       purge_scopes(Dict3, Dict4),
       append_to_query_list(Dict2, Dict4, 'insert second parameter', '', DictOut)
     ; logging:fatal('\'insert\' requires a typed swoql variable..'))
  ; logging:fatal('\'insert\' requires a typed identifer in the form \'id^^type\'..').


/*******************************************************************************/
/*
 *  insert(Swoql variable^^Type)
 *
 */
ask_insert1(ArgList, DictIn, DictOut) :-
  [TypedID|_] = ArgList,
  Var^^Type = TypedID
  -> (wvar(Var)
    -> jsonise_var(Var, JVar),
       jsonise_scm(Type, JType),
       start_scope(insert, JVar, DictIn, Dict1),
       do_ask(and([add_triple(JVar, 'rdf:type', JType)]), _, Dict1, DictOut)
     ; logging:fatal('\'insert\' requires a typed swoql variable..'))
  ; logging:fatal('\'insert\' requires a typed identifer in the form \'id^^type\'..').


/*******************************************************************************/
/*
 *  less(left term, right term)
 *
 */
ask_less(ArgList, DictIn, DictOut) :-
  [Left|[Right|_]] = ArgList,
  triple_var_or_datatype(Left, LeftDict),
  triple_var_or_datatype(Right, RightDict),
  DictOut = DictIn.put([
                        '@type'       : 'woql:Less',
                        'woql:left'   : LeftDict,
                        'woql:right'  : RightDict]).


/*******************************************************************************/
/*
 *  not(Swoql primitive)
 *
 */
ask_not(ArgList, DictIn, DictOut) :-
 [Query|_] = ArgList,
 do_ask(Query, _, DictIn, Dict1),
 DictOut = DictIn.put(['@type'          : 'woql:Not',
                      'woql:query'      : Dict1]).


/*******************************************************************************/
/*
 *  opt(Swoql primitive)
 *
 */
ask_opt(ArgList, DictIn, DictOut) :-
  [Query|_] = ArgList,
  do_ask(Query, _, DictIn, Dict1),
  DictOut = DictIn.put(['@type'          : 'woql:Optional',
                      'woql:query'       : Dict1]).


/*******************************************************************************/
/*
 *  or([list of Swoql primitives])
 *
 */
ask_or(ArgList, DictIn, DictOut) :-
 [List|_] = ArgList,
 is_list(List)
 -> collect_ask_list(List, DictIn, DictList),
    build_query_list(DictList, OrList),
    Dict2 = DictIn.put(['@type'          : 'woql:Or',
                          'woql:query_list' : OrList]),
    ((get_dict('@type', DictIn, Type), Type == 'woql:Or')
    -> merge_query_lists(DictIn, Dict2, DictOut)
    ;  DictOut = Dict2)
 ;  logging:fatal('\'or\' requires a list parameter').


/*******************************************************************************/
/*
 *  property(Swoql variable^^Type)
 *
 */
ask_property_1(ArgList, DictIn, DictOut) :-
  [Arg|_] = ArgList,
  (Name^^Type_or_Var = Arg
  -> ((scope_has(doc, ScopeValue, DictIn); scope_has(property, ScopeValue, DictIn))
     -> start_scope(property, ScopeValue, DictIn, Dict1),             % Make new the scope have the same docname as the predecessor
        jsonise_scm(ScopeValue, ScmDoc),
        jsonise_scm(Name, ScmName),
        (wvar(Type_or_Var)
         -> XsdType = Type_or_Var,
            OwlProperty = 'owl:DatatypeProperty'
        ;   property_type(true, Type_or_Var, XsdType, OwlProperty)),
        do_ask(and([ add_quad(ScmName, 'rdf:type', OwlProperty, 'schema/main'),
                     add_quad(ScmName, 'rdfs:range', XsdType, 'schema/main'),
                     add_quad(ScmName, 'rdfs:domain', ScmDoc, 'schema/main')]), _, Dict1, Dict2),
        DictOut = Dict2
      ; logging:fatal('property \'~w\' has no enclosing context', [Name]))
  ; logging:fatal('Incorrect type operator for property \'~w\' (should use \'property_name^^property_type\')', [Arg])).


/*******************************************************************************/
/*
 *  property(Name, Type or Variable)
 *
 */
ask_property_2(ArgList, DictIn, DictOut) :-
  [Name|[Type_or_Var|_]] = ArgList,
  (scope_has(insert, ScopeValue, DictIn)
  -> do_ask(add_triple(ScopeValue, Name, Type_or_Var), _, DictIn, DictOut)
  ; logging:fatal('property \'~w\' has no enclosing \'insert\' context', [Name])).



/*******************************************************************************/
/*
 *  quad(subject, predicate, object, graph)
 *
 */
ask_quad(ArgList, DictIn, DictOut) :-
  [Subj|[Pred|[Obj|[Graph|_]]]] = ArgList,
  triple_subject(Subj, SubjectDict),
  triple_predicate(Pred, PredicateDict),
  triple_object(Obj, ObjectDict),
  quad_graph(Graph, GraphDict),
  DictOut = DictIn.put(['@type'             : 'woql:Quad',
                        'woql:subject'      : SubjectDict,
                        'woql:predicate'    : PredicateDict,
                        'woql:object'       : ObjectDict,
                        'woql:graph_filter' : GraphDict]).


/*******************************************************************************/
/*
 *  remote(URL, Swoql primitive)
 *
 */
ask_remote(ArgList, DictIn, DictOut) :-
 [URL|[Query|_]] = ArgList,
 do_ask(Query, Verb, DictIn, Dict1),
 Verb == get
 -> DictOut = Dict1.put([
                          'woql:query_resource' :
                                  _{'@type'             : 'woql:RemoteResource',
                                    'woql:remote_uri'   : _{'@type'  : 'xsd:anyURI', '@value': URL}
                                    }])
 ;  logging:fatal('\'remote\' has no associated \'get\'').


/*******************************************************************************/
/*
 *  select([list of Swoql variables], 'and'/'where' primitive)
 *
 */
ask_select(ArgList, DictIn, DictOut) :-
  [Selects|[Where|_]] = ArgList,
  is_list(Selects)
  -> (do_ask(Where, Verb, DictIn, Dict1),
     ((Verb == and; Verb == where)
     ->   maplist_with_counter(map_select_variable, 0, Selects, SelectList),
         DictOut = DictIn.put(['@type'               : 'woql:Select',
                            'woql:variable_list'  : SelectList,
                            'woql:query'          : Dict1])
      ;  logging:fatal('Expected \'where\' or \'and\' after \'select\' (got \'~w\')~n', Verb)))
  ;  logging:fatal('\'select\' requires a list parameter').


/*******************************************************************************/
/*
 *  triple(subject, predicate, object)
 *
 */
ask_triple(ArgList, DictIn, DictOut) :-
  [Subj|[Pred|[Obj|_]]] = ArgList,
  triple_subject(Subj, SubjectDict),
  triple_predicate(Pred, PredicateDict),
  triple_object(Obj, ObjectDict),
  DictOut = DictIn.put(['@type'         : 'woql:Triple',
                        'woql:subject'  : SubjectDict,
                        'woql:predicate': PredicateDict,
                        'woql:object'   : ObjectDict]).


/*******************************************************************************/
/*
 *  cast(Swoql variable,  Swoql variable^^Type)
 *
 */
ask_typecast(ArgList, DictIn, DictOut) :-
  [Var1|[Arg|_]] = ArgList,
  (wvar(Var1)
  ->  triple_var(Var1, Var1Dict),
      (Var2^^Type = Arg
       -> (triple_var(Var2, Var2Dict)
           -> typecast_type(Type, TypeDict),
              DictOut = DictIn.put(['@type'                 : 'woql:Typecast',
                                    'woql:typecast_value'   : Var1Dict,
                                    'woql:typecast_type'    : TypeDict,
                                    'woql:typecast_result'  : Var2Dict])
          ; logging:fatal('\'cast\' requires second parameter as a typed variable in the form \'variable^^type\'..'))
       ; logging:fatal('\'cast\' requires second parameter as a typed variable in the form \'variable^^type\'..'))
  ;  logging:fatal('\'cast\' requires first parameter to be a swoql variable')).


/*******************************************************************************/
/*
 *  when(Swoql primitive,  Swoql primitive)
 *
 */
ask_when(ArgList, DictIn, DictOut) :-
   [Query|[Update|_]] = ArgList,
   (Query == true
   -> QueryDict = _{'@type'   : 'woql:True'}
    ; do_ask(Query, _, _{}, Dict1),
      purge_scopes(Dict1, QueryDict)
    ),
   do_ask(Update, _, _{}, Dict2),
   purge_scopes(Dict2, UpdateDict),
   DictOut = DictIn.put(['@type'            : 'woql:When',
                         'woql:query'       : QueryDict,
                         'woql:consequent'  : UpdateDict]).


/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*
 *  labels and descriptions:
 *     Swoql Primitive << label/description {<< label/description}
 *
 */


/*******************************************************************************/
/*
 *  implement label
 *
 */
% do_label(+Label string, +DictIn, -DictOut)
do_label(Label, DictIn, DictOut) :-
  (scope_has(insert, ScopeValue, DictIn)
    -> (wvar(Label)
      -> % insert of a label which is itself a Swoql variable
         jsonise_var(Label, JLabel),
         do_ask(add_triple(ScopeValue, 'rdfs:label', JLabel), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2)
      ; % insert for a Swoql variable now being labelled
         literal_dict(Label, LiteralDict),
         do_ask(add_triple(ScopeValue, 'rdfs:label', LiteralDict), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2))
   ; ((scope_has(doc, Value, DictIn); scope_has(property, Value, DictIn))
     -> % a doctype or property in a schema
         triple_datatype(Label, DataTypeDict),
         jsonise_scm(Value, Scm),
         do_ask(add_quad(Scm, 'rdfs:label', DataTypeDict, 'schema/main'), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2)
      ; logging:fatal('\'label\' \'~w\' has no associated insert, doctype nor property', [Label]))),
   append_to_query_list(DictIn, Dict2, 'add label', Label, DictOut).


/*******************************************************************************/
/*
 *  implement description
 *
 */
% do_description(+Description string, +Parent, +DictIn, -DictOut)
do_description(Description, DictIn, DictOut) :-
  (scope_has(insert, ScopeValue, DictIn)
    -> (wvar(Description)
      -> % insert of a description which is itself a Swoql variable
         jsonise_var(Description, JDescription),
         do_ask(add_triple(ScopeValue, 'rdfs:comment', JDescription), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2)
      ; % insert for a Swoql variable now being described
         literal_dict(Description, LiteralDict),
         do_ask(add_triple(ScopeValue, 'rdfs:comment', LiteralDict), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2))
   ; ((scope_has(doc, Value, DictIn); scope_has(property, Value, DictIn))
     -> % a doctype or property in a schema
         triple_datatype(Description, DataTypeDict),
         jsonise_scm(Value, Scm),
         do_ask(add_quad(Scm, 'rdfs:comment', DataTypeDict, 'schema/main'), _, _{}, Dict1),
         purge_scopes(Dict1, Dict2)
      ; logging:fatal('\'description\' \'~w\' has no associated insert, doctype nor property', [Description]))),
   append_to_query_list(DictIn, Dict2, 'add description', Description, DictOut).


/*******************************************************************************/
/*
 *  Implement label or description
 *
 */
% process_label_description(+ArgList, +DictIn, -DictOut)
process_label_description(ArgList, DictIn, DictOut) :-
  [Base|[TagTerm|_]] = ArgList,
  decompose(TagTerm, Tag, N, ArgList1),
  [TagArg|_] = ArgList1,
   (N == 1
   -> do_ask(Base, _, DictIn, Dict1),
      (Tag == label
      -> do_label(TagArg, Dict1, DictOut)
      ;  (Tag == description
         -> do_description(TagArg, Dict1, DictOut)
         ;  logging:fatal('Inappropriate tag \'~w\'', [Tag])))
   ;  logging:fatal('Incorrect number of arguments ~w to \'~w\' (expected 1)', [N, Tag])).



/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*
 *  Recursively parse an 'ask'
 *
 */

% Swoql primitive, swipl primitive to use,  size of argument list
jump_table(add_quad, ask_add_quad, 4).
jump_table(add_triple, ask_add_triple, 3).
jump_table(and, ask_and, 1).
jump_table(as, ask_as, 2).
jump_table(cast, ask_typecast, 2).
jump_table(concat, ask_concat, 2).
jump_table(delete_quad, ask_delete_quad, 4).
jump_table(delete_triple, ask_delete_triple, 3).
jump_table(doctype, ask_doctype1, 1).
jump_table(doctype, ask_doctype, 2).
jump_table(eq, ask_eq, 2).
jump_table(file, ask_file, 2).
jump_table(get, ask_get, 1).
jump_table(get_csv, ask_get_csv, 2).
jump_table(greater, ask_greater, 2).
jump_table(idgen, ask_idgen, 3).
jump_table(insert, ask_insert, 2).
jump_table(insert, ask_insert1, 1).
jump_table((<<), process_label_description, 2).
jump_table(less, ask_less, 2).
jump_table(not, ask_not, 1).
jump_table(opt, ask_opt, 1).
jump_table(or, ask_or, 1).
jump_table(property, ask_property_1, 1).
jump_table(property, ask_property_2, 2).
jump_table(quad, ask_quad, 4).
jump_table(remote, ask_remote, 2).
jump_table(select, ask_select, 2).
jump_table(triple, ask_triple, 3).
jump_table(when, ask_when, 2).
jump_table(where, ask_and, 1).


/*******************************************************************************/
/*
 *  do_ask -- decompose a query primitive into its verb and arguments
 *
 */
% do_ask(+Query, -Name, +DictIn, -DictOut)
do_ask(Query, Name, DictIn, DictOut) :-
  var(Query)
  -> DictOut = DictIn                               % probably '_'
  ;  (decompose(Query, Name, Len, ArgList),
      (jump_table(Name, Jump, Len)
      ->  call(Jump, ArgList, DictIn, DictOut)
      ;  logging:fatal('unknown verb\'~w\' with ~w arguments ~n', [Name, Len]))).

% do_ask(+Query)
do_ask(Query) :-
  do_ask(Query, _, _{}, _).

/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*
 *  Recursively parse an 'ask'
 *
 *  ask(+Client, +Query, -Result)
 */
ask(Client, Query, Result) :-
   ((nonvar(Client), nonvar(Query),var(Result))
   -> true
   ;  logging:fatal('\'execute\' called with incorrect arguments..')),
   do_ask(Query, _, _{}, Dict1),
   purge_scopes(Dict1, Dict2),
   contains_update_check(Dict2, Is_Update),
   version(Version),
   (Is_Update
   -> atomic_concat('Update Query generated by Swipl Woql v', Version, CommitMsg),
      CommitDict = _{'commit_info': _{'author': Client.user, 'message': CommitMsg}}
   ;  CommitDict = _{}),
   client:update_database(Client, Client.db, Dict2, CommitDict, Result).


/*******************************************************************************/
/*******************************************************************************/
/*******************************************************************************/
/*
 *  Support for analysing results from the server
 *
 *     get_var_name - get variable name from a URI key
 *     get_value   - get @value from a URI key
 *     get_bindings - get bindings list from a result
 *     empty_response - succeeds if a result has empty bindings list
 *     process_key_val - extract variable and value pair from a binding
 *     process_dict  - build variable and value pairs as dict, from a binding
 *     process_result - extract variable and value list from a binding list
 *     pretty_print - print out results in tabular form
 *     result_to_dict - convert JSON-LD result to a dict
 *     result_check_statistic - check counter stat in a result has expected value
 *     result_success - interpret a result:  was it successful?..
 */

%
%  A result is a dict.
%  The dict has a key 'bindings'
%  The value of that key is a single list
%  The list contains a list of dicts
%  Each dict has different keys with values, for a single result
%

% get_var_name(+Uri, -NameAtom)
get_var_name(Uri, NameAtom) :-
   NameAtom = Uri.


% get_value(+Uri, -Value)
get_value(ValueURI, Value) :-
  is_dict(ValueURI)
  -> (get_dict('@value', ValueURI, Val)
     -> (number(Val)
         -> Value = Val
         ;  term_string(Val, Value))
     ;  logging:fatal('No \'@value\' entry found in \'~w\'', [ValueURI]))
  ;  term_string(ValueURI, Value).


% get_bindings(+Reply, -BindingsResult)
get_bindings(Reply, BindingsResult) :-
  ((nonvar(Reply), var(BindingsResult))
  -> true
  ;  logging:fatal('\'get_bindings\' requires a bound and unbound arguments..')),
 open_string(Reply, InStr),
 json_read_dict(InStr, ReplyDict),
 (is_dict(ReplyDict)
 -> (get_dict('bindings', ReplyDict, Bindings)
    -> (is_dict(Bindings)
       -> BindingsResult = []
       ;  BindingsResult = Bindings)
    ;  logging:fatal('No bindings in response ~w', ReplyDict))
 ;  logging:fatal('Response should be a dict, but is not! ~w', ReplyDict)).


% empty_response(+Reply)
empty_response(Reply) :-
  (nonvar(Reply)
  -> true
  ;  logging:fatal('\'empty_response\' requires a bound argument..')),
  get_bindings(Reply, Bindings),
  Bindings == [].


% process_key_val(+BindPair, -Pair)
process_key_val(BindPair, Pair) :-
  BindPair = Key-Val,
  get_var_name(Key, Var),
  get_value(Val, Value),
  Pair = Var-Value.


% process_dict(+BindDict, -Dict)
process_dict(BindDict, Dict) :-
  is_dict(BindDict)
  ->  (dict_pairs(BindDict, _, BindPairs),
      maplist(process_key_val, BindPairs, Pairs),
      dict_pairs(Dict, _, Pairs))
    ; Dict = _{}.


% process_result(+Response, -ResultsList)
process_result(Response, ResultsList)  :-
  ((nonvar(Response), var(ResultsList))
  -> true
  ;  logging:fatal('\'process_result\' called with incorrect parameter(s)..')),
  get_bindings(Response, BindingsList),
  maplist(process_dict, BindingsList, ResultsList).


% pretty_print(+ResultsList)
pretty_print(ResultsList) :-
  (nonvar(ResultsList)
  -> true
  ;  logging:fatal('\'pretty_print\' requires a bound parameter..')),
  is_list(ResultsList)
  -> (foreach(member(D, ResultsList),(write(D), nl)))
   ; logging:fatal('Argument to \'pretty_print\' is not a list!').


% result_to_dict(+Result, -Dict)
result_to_dict(Result, Dict) :-
  open_string(Result, InStream),
  json_read_dict(InStream, Dict).


% result_check_statistic(+Category, +Target, +Result)
result_check_statistic(Category, Target, Result) :-
  ((nonvar(Category), nonvar(Target), nonvar(Result))
    -> true
    ;  logging:fatal('\'result_check_statistic\' called with incorrect parameter(s)..')),
  result_to_dict(Result, Dict),
  (get_dict(Category, Dict, Count)
   -> Target == Count
   ;  logging:fatal('No such result statistic \'~w\'~n', [Category])).


% result_success(+Result)
result_success(Result) :-
  (nonvar(Result)
    -> true
    ;  logging:fatal('\'result_success\' requires a bound argument..')),
  result_to_dict(Result, Dict),
  (is_dict(Dict)
   -> (get_dict('terminus:status', Dict, Terminus_Result)
      -> (Terminus_Result == "terminus:success"               %NB: have to use string quotes here,  not ''
          -> true
          ;  logging:info('\'terminus:status\' = \'~w\' in result ~w passed to \'result_success\'', [Terminus_Result, Result]),
             false)
      ;  (get_dict('terminus:agent_key_hash', Dict, _)
          -> true                                  % Call was actually a 'connect',  and the result looks valid
          ;  (get_dict('bindings', Dict, _)
             -> logging:info('\'bindings\' received in result passed to \'result_success\': ~w', [Result])
            ;   logging:info('Missing response in result passed to \'result_success\': ~w', [Result]),
                false)))
   ; logging:info('\'result_success\' has a non-dict result: ~w', [Result]),
     false).
