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
/* The tutorial is the swoql version of the bike tutorial which is in    */
/* the standard distribution, in both Javascript and Python.             */
/*                                                                       */
/* After loading the raw data from the web resource (see data_url below) */
/* a query is made,  just as an example.                                 */
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
db('Swoql3').
account('admin').
user('admin').
key('root').

data_url('https://terminusdb.com/t/data/bikeshare/2011-capitalbikeshare-tripdata.csv').


/*******************************************************************************/
/*
 *  Main entry point for the tutorial
 *
 */
run() :-
   format('Tutorial 3~n'),

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
              and([
                    doctype('Station') << label('Bike Station')
                                        << description('A station where bikes are deposited'),
                    doctype('Bicycle') << label('Bicycle'),
                    doctype('Journey',
                              and([
                                    property('start_station'^^'Station') << label('Start Station'),
                                    property('end_station'^^'Station') << label('End Station'),
                                    property('duration'^^'integer') << label('Journey Duration'),
                                    property('start_time'^^'dateTime') << label('Time Started'),
                                    property('end_time'^^'dateTime') << label('Time Ended'),
                                    property('journey_bicycle'^^'Bicycle') << label('Bicycle Used')
                              ])) << label('Journey')
                   ])),
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
                 and([
                       get_csv(Resource,
                                get([
                                     as('Start station', 'v:Start_Station'),
                                     as('End station', 'v:End_Station'),
                                     as('Start date', 'v:Start_Time'),
                                     as('End date', 'v:End_Time'),
                                     as('Duration', 'v:Duration'),
                                     as('Start station number', 'v:Start_ID'),
                                     as('End station number', 'v:End_ID'),
                                     as('Bike number', 'v:Bike'),
                                     as('Member type', 'v:Member_Type')
                                  ])),
                         idgen('doc:Journey', ['v:Start_ID', 'v:Start_Time', 'v:Bike'], 'v:Journey_ID'),
                         idgen('doc:Station', ['v:Start_ID'], 'v:Start_Station_URL'),
                         cast('v:Duration', 'v:Duration_Cast'^^'integer'),
                         cast('v:Bike', 'v:Bike_Label'^^'string'),
                         cast('v:Start_Time', 'v:Start_Time_Cast'^^'dateTime'),
                         cast('v:End_Time', 'v:End_Time_Cast'^^'dateTime'),
                         cast('v:Start_Station', 'v:Start_Station_Label'^^'string'),
                         cast('v:End_Station', 'v:End_Station_Label'^^'string'),
                         idgen('doc:Station', ['v:End_ID'], 'v:End_Station_URL'),
                         idgen('doc:Bicycle', ['v:Bike_Label'], 'v:Bike_URL'),
                         concat('Journey from v:Start_ID to v:End_ID at v:Start_Time', 'v:Journey_Label'),
                         concat('Bike v:Bike from v:Start_Station to v:End_Station at v:Start_Time until v:End_Time',
                                                                                           'v:Journey_Description')
                    ]),
                 and([
                        insert('v:Journey_ID'^^'Journey',
                               and([
                                     property('start_time', 'v:Start_Time_Cast'),
                                     property('end_time', 'v:End_Time_Cast'),
                                     property('duration', 'v:Duration_Cast'),
                                     property('start_station', 'v:Start_Station_URL'),
                                     property('end_station', 'v:End_Station_URL'),
                                     property('journey_bicycle', 'v:Bike_URL')
                               ]) << label('v:Journey_Label') << description('v:Journey_Description')),
                        insert('v:Start_Station_URL'^^'Station') << label('v:Start_Station_Label'),
                        insert('v:End_Station_URL'^^'Station') << label('v:End_Station_Label'),
                        insert('v:Bike_URL'^^'Bicycle') << label('v:Bike_Label')
                    ])
           ),
           Result_Ins),
   (woql:result_success(Result_Ins)
   -> true
   ;  logging:fatal('Couldn\'t do inserts!')),


   % Do a query
   %  Return the start and end stations used by bicycle W000675.
   %  Consult the .csv file as specified by data_url to see
   %  eg the underlying bicycle numbers and stations..
   format('Query..~n'),
   swoql:ask(Client,
                 select([v(start), v(end)],
                         and([
                              triple(v(bicycle), 'label', 'W00675'),
                              triple(v(journey),  'journey_bicycle', v(bicycle)),
                              triple(v(journey), 'start_station', v(start)),
                              triple(v(journey), 'end_station', v(end))
                            ])),
             Q1_Result),
  (swoql:empty_response(Q1_Result)
  -> logging:fatal('Empty Result!!..]~n')
  ;  swoql:process_result(Q1_Result, Q1_PResult),
     swoql:pretty_print(Q1_PResult)),
   format('-- Query done ---------------------------------------------~n'),

   format('~nTutorial-3 finished..~n'),
   format('If you wish, take a look at the logfile to see the http traffic generated..~n').
