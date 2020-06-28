/**
 *
 * Tutorial for Swipl WOQL
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

use_module(swoql).
use_module(client).
use_module(logging).

/*************************************************************************/
/*                                                                       */
/* The tutorial uses a simple in-memory set of data to build a new       */
/* database.                                                             */
/*                                                                       */
/* The database is a simple collection of people,  each of whom has an   */
/* Age and a Name.                                                       */
/*                                                                       */
/*************************************************************************/



/*******************************************************************************/
/*
 * Required settings.
 *
 */
server_url('http://localhost:6363').
db('Swoql1').
account('admin').
user('admin').
key('root').


/*******************************************************************************/
/*
 * Define an in-memory sample database.
 *
 */
person('Joe', 17).
person('Siobhan', 18).
person('Seamus', 45).
person('Mary', 46).
person('Pat', 71).
person('Cliona', 74).


/*******************************************************************************/
/*
 * Utility function, modelled on Python zip
 *
 */
zip([], [], []).
zip([(X1,X2)|Xs], [Y|Ys], [(X1,X2,Y)|Zs]) :-
  zip(Xs,Ys,Zs).


/*******************************************************************************/
/*
 *  Copy the in memory database into TerminusDB
 *
 *  Use swoql idgen to generate unique identifiers for each PersonType document.
 *  Use swoql insert to construct PersonType document, including its properties
 *  Use swoql when to update TerminusDB:
 *      when idgen generates a new identifier,
 *       then insert the document
 */
insert_person(Name, Age, Cnt, Client) :-
  format('~w ~w ~w ~n', [Name, Age, Cnt]),
  atomic_concat('Nr:', Cnt, Nr),
  swoql:ask(Client,
         when(idgen('doc:PersonType', [Cnt], v('Person_ID')),
              insert(v('Person_ID')^^'PersonType',
                         and([property('Name', Name),
                              property('Age', Age)
                             ])
                     ) << label(Nr)
          ),
         Result),
  (swoql:result_success(Result)
  -> true
  ;  logging:fatal('Could not insert \'~w\'!', [Name])).


/*******************************************************************************/
/*
 *  Main entry point for the tutorial
 *
 */
run() :-
   format('Tutorial 1~n'),

   % enable logging to file in current working directory
   logging:log('logfile.log'),

   % pick up the configuration parameters
   server_url(Server),
   account(Account),
   user(User),
   key(Key),
   db(DB),

   % construct a new client dict
   Cli = client{}.create(Server, Account, User, Key),

   % connect to TerminusDB
   format('Calling connect..~n'),
   Cli1 = Cli.connect(Result1),
   (swoql:result_success(Result1)
   -> true
   ;  logging:fatal('Could not connect to the server!')),
   format('---------------------------------------------------------~n'),

   % delete prior instance of the database, if it exists,
   %  so that we have a clean start...
   format('Calling delete_database..~n'),
   Cli2 = Cli1.delete_database(DB, Result2),
   (swoql:result_success(Result2)
   -> true
   ;  format('No database to delete!~n')),
   format('---------------------------------------------------------~n'),

   % create the new database
   format('Calling create_database..~n'),
   Client = Cli2.create_database(DB, 'Tutorial 1', 'My first swoql DB!', Result3),
   (swoql:result_success(Result3)
   -> true
   ;  logging:fatal('Could not create database!')),
   format('---------------------------------------------------------~n'),

   % create the schema
   format('- Schema --------------------------------------------------------~n'),
   swoql:ask(Client,
         when(true,
              doctype('PersonType',  and([
                                      property('Name'^^'string') << label('first_name'),
                                      property('Age'^^'integer') << label('years')
              ])) << label('Some Person') << description('Somebody')
          ),
         Result_schema),
   (swoql:result_success(Result_schema)
    -> true
    ;  logging:fatal('Could not create schema!')),
   format('- Schema created ------------------------------------------------~n'),

   % populate the new database using the in mmeory data
   format('Populating data..~n'),
   aggregate_all(count, person(N,A), Count),            % count number of entries
   findall((N,A), person(N,A), PList),                  % Build PList of Names and Ages
   findall(X, between(1,Count,X), NList),               % Build list of counters (to be used in the swoql idgen)
   zip(PList, NList, List),                             % combine the two lists together as new list of names, ages and counter
   foreach(member((Name, Age, Cnt), List),              % repetitively call insert_person, for each list entry
              insert_person(Name, Age, Cnt, Client)),
   format('- Data populated ------------------------------------------------~n'),

   % Do a query
   %  Should return all the PersonType documents and their associated label value
   %  Note that the label value was set in insert_person as 'Nr:<counter value>'
   format('First Query..~n'),
   swoql:ask(Client,
              triple('v:Person', 'label', 'v:Value'),
              Q1_Result),
   swoql:process_result(Q1_Result, Q1_PResult),
   swoql:pretty_print(Q1_PResult),
   format('-- First Query done ---------------------------------------------~n'),

   % Verify that a non-matching query returns no results at all
   format('Second Query..~n'),
   swoql:ask(Client,
              triple('v:Person', 'Who', 'v:Value'),
              Q2_Result),
   (swoql:empty_response(Q2_Result)
   -> format('Empty Result -- as expected..]~n')
   ;  logging:fatal('Query failed:  expected no results, got ~w', [Q2_Result])),
   format('- Second query done ---------------------------------------------~n'),

   % Return all PersonType documents, with their names and ages
   format('Third Query..~n'),
   swoql:ask(Client,
              and([ triple('v:Person', 'Name', 'v:Name'),
                    triple('v:Person', 'Age', 'v:Age')]),
              Q3_Result),
   (swoql:empty_response(Q3_Result)
   -> logging:fatal('Empty Result!!..]~n')
   ;  swoql:process_result(Q3_Result, Q3_PResult),
      swoql:pretty_print(Q3_PResult)),
   format('- Third query done ----------------------------------------------~n'),

   % Return all PersonType documents, with their names and ages and labels (Nr:count values)
   format('Fourth Query..~n'),
   swoql:ask(Client,
              and([ triple('v:Person', 'Name', 'v:Name'),
                    triple('v:Person', 'Age', 'v:Age'),
                    triple('v:Person', 'label', 'v:label')]),
              Q4_Result),
   (swoql:empty_response(Q4_Result)
    -> logging:fatal('Empty Result!!..]~n')
    ;  swoql:process_result(Q4_Result, Q4_PResult),
       swoql:pretty_print(Q4_PResult)),
   format('- Fourth query done ---------------------------------------------~n'),

   % select the names, ages and labels only (dont return the PersonType documents themselves)
   format('Fifth Query..~n'),
   swoql:ask(Client,
              select(['v:Name', 'v:Age', v(label)],
                    and([ triple('v:Person', 'Name', 'v:Name'),
                          triple('v:Person', 'Age', 'v:Age'),
                          triple('v:Person', 'label', 'v:label')])),
              Q5_Result),
   (swoql:empty_response(Q5_Result)
    -> logging:fatal('Empty Result!!..]~n')
    ;  swoql:process_result(Q5_Result, Q5_PResult),
       swoql:pretty_print(Q5_PResult)),
   format('- Fifth query done ----------------------------------------------~n'),

   % select the names and ages of those over 50
   format('Sixth Query..~n'),
   swoql:ask(Client,
              select(['v:Name', 'v:Age'],
                    where([ triple('v:Person', 'Age', 'v:Age'),
                            greater('v:Age', 50),
                            triple('v:Person', 'Name', 'v:Name')])),
              Q6_Result),
   (swoql:empty_response(Q6_Result)
    -> logging:fatal('Empty Result!!..]~n')
    ;  swoql:process_result(Q6_Result, Q6_PResult),
       swoql:pretty_print(Q6_PResult)),
   format('- Sixth query done ----------------------------------------------~n'),

   % Delete the data relating to people over 50.
   %
   % Note that we have to delete each of the four triples involved for
   % each PersonType document, including deleting the PersonType document itself.
   %
   % I believe the core team will soon add a delete_object WOQL primitive, which
   % in a single call will delete a document and all its associated triples..
   %
   % The result returned is the result of the query: ie those people
   % over the age of 50 (who were then deleted from the database).
   %
   format('Seventh Query..~n'),
   swoql:ask(Client,
              when(
                  and([
                        triple('v:Person', 'Age', 'v:Age'),
                        greater('v:Age', 50),
                        triple('v:Person', 'Name', 'v:Name'),
                        triple('v:Person', 'label', 'v:label')]),
                  and([
                        delete_triple('v:Person', 'Name', 'v:Name'),
                        delete_triple('v:Person', 'Age', 'v:Age'),
                        delete_triple('v:Person', 'label', 'v:label'),
                        delete_triple('v:Person', 'type', 'scm:PersonType')])),
              Q7_Result),
   (swoql:empty_response(Q7_Result)
    -> logging:fatal('Empty Result!!..]~n')
    ;  swoql:process_result(Q7_Result, Q7_PResult),
       swoql:pretty_print(Q7_PResult)),
   format('- Seventh query done --------------------------------------------~n'),

   format('~nTutorial-1 finished..~n'),
   format('If you wish, take a look at the logfile to see the http traffic generated..~n').
