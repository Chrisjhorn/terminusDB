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


/*************************************************************************/
/*                                                                       */
/* The tutorial uses an external .csv file to build a new database. This */
/* is at the URL given below in data_url.                                */
/*                                                                       */
/* The database is a simple collection of people,  each of whom can have */
/* a single partner.                                                     */
/*                                                                       */
/* Two people (Sue and Dave) have no partners.                           */
/*                                                                       */
/* One person (Joe) claims to have a partner (Kris),  but that person    */
/* denies it and has somebody else as a partner (Jim).                   */
/*                                                                       */
/*************************************************************************/

use_module(swoql).
use_module(client).
use_module(logging).

/*******************************************************************************/
/*
 * Required configuration settings.
 *
 */
server_url('http://localhost:6363').
db('Swoql2').
account('admin').
user('admin').
key('root').

data_url('https://raw.githubusercontent.com/Chrisjhorn/terminusDB/master/python/jupyter-tutorials/tutorial2/people.csv').


/*******************************************************************************/
/*
 *  Main entry point for the tutorial
 *
 */
run() :-
   format('Tutorial 2~n'),

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
                                      property('Age'^^'integer') << label('years'),
                                      property('Partner'^^'string') << label('partner_first_name')
              ])) << label('Some Person') << description('Somebody')
          ),
         Result_schema),
   (swoql:result_success(Result_schema)
    -> true
    ;  logging:fatal('Could not create schema!')),
   format('- Schema created ------------------------------------------------~n'),

   % populate the new database using an external .csv file
   %
   %  when:
   %       read the .csv file as given by Resource
   %       get the following columns from that .csv file:
   %           Nr, and use it for v:Nr
   %           Name, and use it for v:Name
   %           Age, and use it for v:Age
   %           Partner, and use it for v:Partner
   %       idgen new ids for PersonType documents, based on v:Nr values
   %  then:
   %       insert a new PersonType document with id v:People_ID
   %           and Age property,  the v:Age value
   %           and Partner property,  the v:Partner value
   %       and finally give it a label using the v:Name value
   %
   format('- Populating data -----------------------------------------------~n'),
   data_url(Resource),
   woql:ask(Client,
           when(
                 and([get_csv(Resource,
                               get([
                                     as('Nr', 'v:Nr'),
                                     as('Name', 'v:Name'),
                                     as('Age', 'v:AgeString'),
                                     as('Partner', 'v:Partner')])),
                       cast('v:AgeString', 'v:Age'^^'integer'),
                       idgen('doc:PersonType', ['v:Nr'], 'v:People_ID')]),
                 insert(v('People_ID')^^'PersonType',
                            and([property('Age', 'v:Age'),
                                 property('Partner', 'v:Partner')
                                ])
                        ) << label('v:Name')

           ),
           Result_Ins),
   (woql:result_success(Result_Ins)
   -> true
   ;  logging:fatal('Couldn\'t do inserts!')),


   % Do a query
   %  Return all the PersonType documents and their associated
   %  properties,  and label values
   format('First Query..~n'),
   swoql:ask(Client,
                 select(['v:Name', 'v:Age', 'v:Partner'],
                        and([triple('v:PersonID', 'label', 'v:Name'),
                              triple('v:PersonID', 'Age', 'v:Age'),
                              triple('v:PersonID', 'Partner', 'v:Partner')])),
             Q1_Result),
   swoql:process_result(Q1_Result, Q1_PResult),
   swoql:pretty_print(Q1_PResult),
   format('-- First Query done ---------------------------------------------~n'),

   % Query for people with no partners..
   format('Second Query..~n'),
   swoql:ask(Client,
                 select(['v:Name'],
                        and([ triple('v:PersonID', 'label', 'v:Name'),
                              triple('v:PersonID', 'Partner', '')])),
            Q2_Result),
   (swoql:empty_response(Q2_Result)
   -> logging:fatal('Empty Result!!..]~n')
   ;  swoql:process_result(Q2_Result, Q2_PResult),
      swoql:pretty_print(Q2_PResult)),
   format('- Second Query done----------------------------------------------~n'),

   % Remove people with no partners from the database
   %
   % Note that we have to delete each of the four triples involved for
   % each PersonType document, including deleting the PersonType document itself.
   %
   % And: to be able to give the object part (3rd argument) of each delete_triple,
   % we need to ensure that these are located in the first part of the when clause.
   %
   % I believe the core team will soon add a delete_object WOQL primitive, which
   % in a single call will delete a document and all its associated triples.
   %
   format('Third Query..~n'),
   swoql:ask(Client,
                 when(
                      and([ triple('v:PersonID', 'label', 'v:Name'),
                            triple('v:PersonID', 'Age', 'v:Age'),
                            triple('v:PersonID', 'Partner', 'v:Partner'),
                            eq('v:Partner', '')
                          ]),
                      and([
                            delete_triple('v:PersonID', 'label', 'v:Name'),
                            delete_triple('v:PersonID', 'Age', 'v:Age'),
                            delete_triple('v:PersonID', 'Partner', 'v:Partner'),
                            delete_triple('v:PersonID', 'type', 'scm:PersonType')])),
            Q3_Result),
   (swoql:empty_response(Q3_Result)
   -> logging:fatal('Empty Result!!..]~n')
   ;  swoql:process_result(Q3_Result, Q3_PResult),
      swoql:pretty_print(Q3_PResult)),
   format('- Third Query done------------------------------------------------~n'),

   % Fourth Query
   %
   % Look for any people who claim they have a partner,  but that person asserts
   % that their partner is somebody else..
   %
   % Also: I use the v(..) syntax for swoql variables here, just to remind you
   % that it is an alternative to the 'v:' form used earlier..
   %
   format('Fourth Query..~n'),
   swoql:ask(Client,
                 select([v(invalid_partner)],
                      and([ triple(v(person1), 'label', v(invalid_partner)),
                            triple(v(person2), 'label', v(claimed_partner)),
                            triple(v(person1), 'Partner', v(claimed_partner)),
                            not(triple(v(person2), 'Partner', v(invalid_partner)))
                          ])),
            Q4_Result),
   (swoql:empty_response(Q4_Result)
   -> logging:fatal('Empty Result!!..]~n')
   ;  swoql:process_result(Q4_Result, Q4_PResult),
      swoql:pretty_print(Q4_PResult)),
   format('- Fourth Query done------------------------------------------------~n'),

   format('~nTutorial-2 finished..~n'),
   format('If you wish, take a look at the logfile to see the http traffic generated..~n').
