
use_module(swoql).
use_module(client).
use_module(logging).

server_url('http://localhost:6363').
db('Swipl').
account('admin').
user('admin').
key('root').
data_url('https://raw.githubusercontent.com/Chrisjhorn/terminusDB/master/python/jupyter-tutorials/tutorial2/people.csv').


check_terminus_error(Name, Key, Dict, Error, Offset) :-
  get_dict(Key, Dict, ErrMsg),
  string_length(Error, Len),
  string_length(ErrMsg, Len1),
  ((Len1 >= Len, Len > 0)
    ->   sub_string(ErrMsg, Offset, Len, _, XXX),
         atom_string(Error,E), atom_string(XXX,X),
         (E == X
          -> format('- ~w got expected exception -------------------------------------------------~n', Name),
             true
           ; false)
    ; false)
; false.

terminus_message(Name, Dict, ErrType, ErrMsg) :-
  ErrType == 'tm'
  ->  check_terminus_error(Name, 'terminus:message', Dict, ErrMsg, 0)
    ; false.

witness_message(Name, Arg, ErrType, ErrMsg) :-
  ErrType == 'vi'
  -> (is_dict(Arg)
      ->  (get_dict('terminus:witnesses', Arg, List)
           -> [Dict2|_] = List,
              is_dict(Dict2),
              check_terminus_error(Name, 'vio:literal', Dict2, ErrMsg, 1)
           ;  false)
      ;  [Dict2|_] = Arg,
         is_dict(Dict2),
         (get_dict('vio:message', Dict2, Dict3)
          -> check_terminus_error(Name, '@value', Dict3, ErrMsg, 0)
          ;  false))
  ; false.


test(Name, Client, Query, Inserts, ErrType, Error, Result) :-
  format('- ~w starting --------------------------------------------------------~n', Name),
  logging:info('- ~w starting --------------------------------------------------~n', [Name]),
  swoql:ask(Client,
             Query,
             Result),
  (swoql:result_success(Result)
   -> (swoql:result_check_statistic('inserts', Inserts, Result)
       -> true
       ;  logging:fatal('~w failed: expected ~w inserts~n', [Name, Inserts]))
   ;  (swoql:result_to_dict(Result, Dict),
      (terminus_message(Name, Dict, ErrType, Error)
      -> true
      ;  (witness_message(Name, Dict, ErrType, Error)
         -> true
         ;  format('witness_message failed...~n'),
            logging:fatal('~w failed...~w', [Name, Result]))))),
  format('- ~w Done ------------------------------------------------------------~n~n', Name),
  logging:info('- ~w Done ------------------------------------------------------~n~n', [Name]).

test(Name, Client, Query) :-
  test(Name, Client, Query, 0, '', '', _).

test(Name, Client, Query, Inserts) :-
  test(Name, Client, Query, Inserts, '', '', _).

test(Name, Client, Query, ErrType, Error) :-
  test(Name, Client, Query, 0, ErrType, Error, _).

test_bindings(Name, Client, Query, InsertsCnt, BindingsCnt) :-
  test(Name, Client, Query, InsertsCnt, '', '', Result),
  swoql:get_bindings(Result, BindingsResult),
  length(BindingsResult, Len),
  (Len == BindingsCnt
   -> true
   ;  logging:fatal('\'~w\' failed: expected ~w bindings, got ~w~n', [Name, BindingsCnt, Len])).

run() :-

   logging:log('logfile.log'),

   server_url(Server),
   account(Account),
   user(User),
   key(Key),
   db(DB),

   Cli = client{}.create(Server, Account, User, Key),

   write("Calling connect.."),nl,
   Cli1 = Cli.connect(Result1),
   (swoql:result_success(Result1)
   -> true
   ;  logging:fatal('Could not connect to the server!')),
   write('---------------------------------------------------------'),nl,

   % write("Calling get metadata.."),nl,
   % _ = Cli.get_metadata(DB, MetaDataResult),
   % (swoql:result_success(MetaDataResult)
   % -> true
   % ;  logging:fatal('Could not connect to the server!')),
   % write('---------------------------------------------------------'),nl,

   write("Calling delete_database.."),nl,
   Cli2 = Cli1.delete_database(DB, Result2),
   (swoql:result_success(Result2)
   -> true
   ;  format('No database to delete!~n')),
   write('---------------------------------------------------------'),nl,

   write("Calliing create_database.."),nl,
   Client = Cli2.create_database(DB, 'Swipl api', 'My first swipl DB!', Result3),
   (swoql:result_success(Result3)
   -> true
   ;  logging:fatal('Could not create database!')),
   write('---------------------------------------------------------'),nl,


    test('Test-T1', Client, triple('AAA', 'BBB', 'CCC')),
    test('Test-T2', Client, triple('v:AAA', 'BBB', 'CCC')),
    test('Test-T3', Client, triple(v('AAA'), 'BBB', 'CCC')),
    test('Test-T4', Client, triple(v(abc), 'BBB', 'CCC')),
    test('Test-T5', Client, triple('AAA', 'label', 'CCC')),
    test('Test-T6', Client, triple('AAA', 'rdfs:BBB', 'CCC')),
    test('Test-T7', Client, triple('AAA', 'v:BBB', 'CCC')),
    test('Test-T8', Client, triple('AAA', 'BBB', v(abc))),
    test('Test-T9', Client, triple('AAA', 'BBB', 'scm:CCC')),

    test('Test-AT1', Client, add_triple('AAA', 'BBB', 'CCC'), 'vi', 'The property terminusdb:'),
    test('Test-AT2', Client, add_triple('v:AAA', 'label', 'scm:CCC'), 'tm', 'Error: instantiation_error in'),
    test('Test-AT3', Client, add_triple('scm:PersonType', 'rdf:type', 'owl:Class'), 'vi', 'The subject '),

    test('Test-DT1', Client, delete_triple('v:AAA', 'label', 'scm:CCC'), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-DT2', Client, delete_triple('AAA', 'BBB', 'CCC')),
    test('Test-DT3', Client, delete_triple('scm:PersonType', 'rdf:type', 'owl:Class')),

    test('Test-Q1', Client, quad('AAA', 'BBB', 'CCC', 'DDD'), 'vi', 'Unable to compile AST query'),

    test('Test-AQ1', Client, add_quad('AAA', 'BBB', 'CCC', 'DDD'), 'vi', 'Unable to compile AST query'),
    test('Test-AQ2', Client, add_quad('scm:PersonType', 'BBB', 'CCC', 'DDD'), 'vi', 'Unable to compile AST query'),
    test('Test-AQ3', Client, add_quad('scm:PersonType', 'rdf:type', 'CCC', 'DDD'), 'vi', 'Unable to compile AST query'),
    test('Test-AQ4', Client, add_quad('scm:PersonType', 'rdf:type', 'owl:Class', 'DDD'), 'vi', 'Unable to compile AST query'),
    test('Test-AQ5', Client, add_quad('scm:PersonType', 'rdf:type', 'owl:Class', 'schema/main'), 1),

    test('Test-And1', Client, and([triple('AAA', 'BBB', 'CCC')])),
    test('Test-And2', Client, and([triple('AAA', 'BBB', 'CCC'), triple('DDD', 'EEE', 'FFF')])),
    test('Test-And3', Client, and([triple('AAA', 'BBB', 'CCC'), add_triple('DDD', 'EEE', 'FFF')])),

    test('Test-Or1', Client, or([triple('AAA', 'BBB', 'CCC')])),
    test('Test-Or2', Client, or([triple('AAA', 'BBB', 'CCC'), triple('DDD', 'EEE', 'FFF')])),
    test('Test-Or3', Client, or([triple('AAA', 'BBB', 'CCC'), add_triple('DDD', 'EEE', 'FFF')]), 'vi', 'The property terminusdb:'),

    test('Test-Not1', Client, not(triple('AAA', 'BBB', 'CCC'))),

    test('Test-Gt1', Client, greater(v(abc), 64), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Gt2', Client, greater(64, v(abc)), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Gt3', Client, greater('v:def', v(abc)), 'tm', 'Error: instantiation_error in CTX'),

    test('Test-Ls1', Client, less(v(abc), 64), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Ls2', Client, less(64, v(abc)), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Ls3', Client, less('v:def', v(abc)), 'tm', 'Error: instantiation_error in CTX'),

    test('Test-Eq1', Client, eq(v(abc), 64), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Eq2', Client, eq(64, v(abc)), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Eq3', Client, eq('v:def', v(abc)), 'tm', 'Error: instantiation_error in CTX'),

    test('Test-DT1', Client, doctype('PersonTypeDT1'), 2),

    test('Test-DTL1', Client, doctype('PersonTypeDTL1') << label('A label'), 3),
    test('Test-DTD1', Client, doctype('PersonTypeDTD1') << description('A description'), 3),
    test('Test-DTDL1', Client, doctype('PersonTypeDTDL1') << description('A description') << label('A label'), 4),
    test('Test-DTLD1', Client, doctype('PersonTypeDTLD1') << label('A label') << description('A description'), 4),

    test('Test-Prop1', Client, doctype('PersonTypeProp1', property('Name'^^'string')), 5),
    test('Test-Prop2', Client, doctype('PersonTypeProp2',
                                        and([property('Name'^^'string')])), 3),  %% 3???  Not 5??
    test('Test-Prop3', Client, doctype('PersonTypeProp3',
                                        and([property('Name'^^'string'),
                                             property('Age'^^'integer')])), 6),

    test('Test-PropLab1', Client, doctype('PersonTypePropLab1', property('PL1Name'^^'string') << label('PL1Name_property')), 6),
    test('Test-PropLab2', Client, doctype('PersonTypePropLab2',
                                        and([property('PL2Name'^^'string') << label('PL2Name_property'),
                                             property('PL2Age'^^'integer') << label('PL2Age_property') ])), 10),
    % test('Test-PropLab3', Client, doctype('PersonTypePropLab3',
    %                                          doctype('PersonType',  and([
    %                                                                  property('Name'^^'string') << label('first_name'),
    %                                                                  property('Age'^^'integer') << label('years')
    %                                          ])) << label('Some Person') << description('Somebody'))),
    test('Test-PropLab3', Client, doctype('PersonTypePropLab3',
                                             and([property('PL3Name'^^'string') << label('PL3first_name'),
                                                  property('PL3Age'^^'integer') << label('PL3years')
                                                 ])) << label('PL3Some Person') << description('PL3Somebody'), 12),

    test('Test-IDGen1', Client, idgen('doc:PersonTypePropLab3', [597], v('Person_ID'))),
    test('Test-IDGen2', Client, idgen('doc:PersonTypePropLab3', [597, 1003], v('Person_ID'))),

    test('Test-Insert1', Client, insert(v('Person_ID')^^'PersonTypePropLab3', property('PL3Name', 'Joe')), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert2', Client, insert(v('Person_ID')^^'PersonTypePropLab3',
                                          and([property('PL3Name', 'Joe')])), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert3', Client, insert(v('Person_ID')^^'PersonTypePropLab3',
                                          and([property('PL3Name', 'Joe'),
                                              property('PL3Age', 17)])), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert4', Client, insert(v('Person_ID')^^'PersonTypePropLab3',
                                          and([property('PL3Name', 'Joe'),
                                              property('PL3Age', 17)]) << label('hello')), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert5', Client, insert(v('Person_ID')^^'PersonTypePropLab3',
                                          and([property('PL3Name', 'Joe'),
                                              property('PL3Age', 17)]) << description('hello')), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert6', Client, insert(v('Person_ID')^^'PersonTypePropLab3'), 'tm', 'Error: instantiation_error in CTX'),
    test('Test-Insert7', Client, insert(v('Person_ID')^^'PersonTypePropLab3') << label('hello'), 'tm', 'Error: instantiation_error in CTX'),

    test('Test-When1Sch', Client, when(true, doctype('When1Doc',
                                             and([property('When1Name'^^'string')]))), 5),
    test('Test-When1', Client, when(idgen('doc:When1Doc', [1234], v('Person_ID')),
                                              insert(v('Person_ID')^^'When1Doc', property('When1Name', 'Joe'))), 2),

    test('Test-When2Sch', Client, when(true, doctype('When2Doc',
                                             and([property('When2Name'^^'string'), property('When2Age'^^'integer')]))), 8),
    test('Test-When2', Client, when(idgen('doc:When2Doc', [1234], v('Person_ID')),
                                              insert(v('Person_ID')^^'When2Doc',
                                              and([property('When2Name', 'Joe'), property('When2Age', 17)]))), 3),

    test('Test-When3Sch', Client, when(true, doctype('When3Doc',
                                             and([property('When3Name'^^'string')])) <<label('Person') <<description('Somebody')), 7),
    test('Test-When3', Client, when(idgen('doc:When3Doc', [1234], v('Person_ID')),
                                              insert(v('Person_ID')^^'When3Doc', property('When3Name', 'Joe')) <<label('Nr1234')), 3), %only 3??

    test('Test-Select1Sch', Client, when(true,
                                         doctype('Select1PersonType',  and([
                                                                 property('Select1Name'^^'string') << label('Select1first_name'),
                                                                 property('Select1Age'^^'integer') << label('Select1years')
                                         ])) << label('Select1Some Person') << description('Seelect1Somebody')), 12),
    test('Test-Select1Ins1', Client, when(idgen('doc:Select1PersonType', [1], v('Person_ID')),
                                              insert(v('Person_ID')^^'Select1PersonType',
                                                         and([property('Select1Name', 'Joe'),
                                                              property('Select1Age', 17)
                                                             ])) << label(1)), 4),
    test('Test-Select1Ins2', Client, when(idgen('doc:Select1PersonType', [2], v('Person_ID')),
                                             insert(v('Person_ID')^^'Select1PersonType',
                                                        and([property('Select1Name', 'Mary'),
                                                             property('Select1Age', 24)
                                                            ])) << label(2)), 4),
   test_bindings('Test-Select1', Client, select(['v:Select1Age', 'v:Select1Name'],
                                            and([triple('v:Person_ID', 'Select1Name', 'v:Select1Name'),
                                                  triple('v:Person_ID', 'Select1Age', 'v:Select1Age')])), 0, 2),


   test('Test-Get1', Client, get([as('Nr', 'v:Nr'),
                                  as('Name', 'v:Name'),
                                  as('Age', 'v:Age'),
                                  as('Partner', 'v:Partner')]), 'tm', 'Not well formed WOQL'),

   data_url(Data_URL),
   test('Test-Remote1Sch', Client, when(
                                    true,
                                    doctype('Remote1PersonType',
                                            and([
                                                  property('Remote1Name'^^'string') << label('Remote1:first_name'),
                                                  property('Remote1Age'^^'string') << label('Remote1:years'),   % python needs string not integer :-)
                                                  property('Remote1Partner'^^'string') << label('Remote1:partner_first_name')
                                                ])) << label('Remote1Person') << description('Remote1:Somebody who has a partner')), 16),
   test_bindings('Test-Remote1', Client, when(
                                        and([
                                              get_csv(Data_URL,         % or remote,  or file
                                                    get([
                                                       as('Nr', 'v:Nr'),
                                                       as('Name', 'v:Name'),
                                                       as('Age', 'v:Age'),
                                                       as('Partner', 'v:Partner')])),
                                              idgen('doc:Remote1PersonType', ['v:Nr'], 'v:People_ID')
                                          ]),
                                         insert(v('People_ID')^^'Remote1PersonType',
                                                    and([property('Remote1Age', 'v:Age'),
                                                         property('Remote1Partner', 'v:Partner')])
                                                ) << label('v:Name')
                                      ), 44, 11),


 test('Test-Concat1', Client, concat('Journey from v:Start_ID to v:End_ID at v:Start_Time', 'v:Journey_Label')),

 test('Test-TypeCast1', Client, cast('v:Duration', v('Duration_Cast')^^integer), 'tm', 'Error: \'Variable unbound in typcast to'),  % server spelling mistake!
 test('Test-TypeCast2', Client, cast(v('Duration'), v('Duration_Cast')^^'xsd:integer'), 'tm', 'Error: \'Variable unbound in typcast to'),

 test('Test-Opt1', Client, opt(triple('AAA', 'BBB', 'CCC'))),

 format('~nAll tests finished...~n').
