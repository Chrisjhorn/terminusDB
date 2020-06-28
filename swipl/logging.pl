/** <module> Logging
 *
 * Python style logging support for Swipl Woql.
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
:- module(logging, [
                    error/2,
                    error/1,
                    fatal/2,
                    fatal/1,
                    get_level/1,
                    get_stream/1,
                    info/2,
                    info/1,
                    log/2,
                    log/0,
                    log/1,
                    set_level/1,
                    set_stream/1,
                    warning/2,
                    warning/1
]).
version('1.0').

 /**********************************************************************************************/
 /*                                                                                            */
 /* Features:                                                                                  */
 /*   Log to current output or file                                                            */
 /*   Log an entry only if its severity level is above a set threshold: default level 0        */
 /*   Conditionally abort on a fatal error: default true                                       */
 /*                                                                                            */
 /*   INFO log entries have severity level 0                                                   */
 /*   WARNING log entries have severity level 1                                                */
 /*   ERROR log entries have severity level 2                                                  */
 /*                                                                                            */
 /**********************************************************************************************/

/*
 * Flag: whether to abort if an error is seen.
 */
abort_on_error(true).


/*
 * Set and Get current output stream for logging.
 *
 */

% set_stream(+Stream)
set_stream(Stream):-
  (nonvar(Stream)
   -> true
   ;  logging:fatal('\'set_stream\' requires a bound argument..')),
   nb_setval(terminus_swipl_log, Stream).


% get_stream(-Stream)
get_stream(Stream):-
  (var(Stream)
   -> true
   ;  logging:fatal('\'get_stream\' requires an unbound argument..')),
   nb_getval(terminus_swipl_log, Stream).



/*
 * Set and Get current severity threshold for logging.
 *
 */
% set_level(+Level)
set_level(Level):-
   (nonvar(Level)
    -> true
    ;  logging:fatal('\'set_level\' requires a bound argument..')),
   nb_setval(terminus_swipl_loglevel, Level).


% get_level(-Level)
get_level(Level):-
   (var(Level)
   -> true
   ;  logging:fatal('\'get_level\' requires an unbound argument..')),
   nb_getval(terminus_swipl_loglevel, Level).



/******************************************************************************/
/*
 * Utility predicates for rest of the code..
 *
 */

% begins_with(+String, +Pat)
begins_with(String, Pat) :-
  atomic_concat('^', Pat, Pat2),
  re_match(Pat2, String, []).


% time_now(+Stream)
time_now(Stream) :-
  get_time(Now),
  format_time(Stream, '%F %H:%M:%S ', Now).



/******************************************************************************/
/*
 * Conditional abort,  depending on abort flag setting
 */
maybeAbort() :-
  abort_on_error(Flag),
  Flag == true
  -> abort
  ;  true.



/******************************************************************************/
/*
 * Fatal error.
 *
 * Report error to current log stream.
 * Conditionally abort.
 */

% fatal(+Str, +List)
fatal(Str, List) :-
  atomic_list_concat(['[', Str, ']'], CStr),
  make_entry(current_output, 0, '', CStr, List, 0, false),
  error(CStr, List),
  maybeAbort().


% fatal(+Str)
fatal(Str) :-
  fatal(Str, []).



/******************************************************************************/
/*
 * Establish log stream and severity level
 *
 * Default is current output stream, level of 0:  set by log/0
 *
 * Log stream is either:
 *   current_output, or
 *   absolute file path, or
 *   relative to current working directory
 */

% log(+File, +Level)
log(File, Level) :-
  ((nonvar(File), nonvar(Level))
  -> true
  ;  logging:fatal('\'log\' requires bound arguments..')),
  set_level(Level),
  (File == current_output
  -> set_stream(current_output),
     format('[Using \'current_output\' for log file..]~n')
  ; (begins_with(File, '//')              % absolute file path
     -> FilePath = File
     ;  working_directory(CWD, CWD),
        atomic_concat(CWD, File, FilePath)),
    (open(FilePath, write, Strm, [close_on_abort, buffer(false)])
     -> set_stream(Strm),
        format('[Using \'~w\' for log file..]~n', FilePath)
     ;  fatal('[Could not open log file \'~w\']~n', FilePath))).


log() :-
  log(current_output, 0).


% log(+File)
log(File) :-
  (nonvar(File)
  -> true
  ;  logging:fatal('\'log\' requires a bound argument..')),
  log(File, 0).



/******************************************************************************/
/*
 * Make log entry.
 *
 * Only do so if its severity level is above the threshold setting - set_level/1
 *
 * Insert timestamp in log entry
 *
 * Log message uses standard 'format/2',  with a list of parameters
 *
 */

% make_entry(+Strm, +Level, +Prefix, +Msg, +List, +Threshold, +TimeStamp)
make_entry(Strm, Level, Prefix, Msg, List, Threshold, TimeStamp) :-
  Level =< Threshold
  -> (TimeStamp
      -> time_now(Strm)
      ;  true),
     atomic_list_concat([Prefix, ' ', Msg, '~n'], Output),
     format(Strm, Output, List)
  ;  true.

% make_entry(+Prefix, +Msg, +List, +Threshold)
make_entry(Prefix, Msg, List, Threshold) :-
  get_level(Level)
  -> get_stream(Strm)
     -> make_entry(Strm, Level, Prefix, Msg, List, Threshold, true)
     ;  true
  ;  true.



/******************************************************************************/
/*
 * INFO, WARNING and ERROR log entries
 *
 */

%info(+Msg, +List)
info(Msg, List) :-
  ((nonvar(Msg), nonvar(List))
  -> true
  ;  logging:fatal('\'info\' requires bound arguments..')),
  make_entry('INFO', Msg, List, 0).

%warning(+Msg, +List)
warning(Msg, List) :-
  ((nonvar(Msg), nonvar(List))
  -> true
  ;  logging:fatal('\'warning\' requires bound arguments..')),
  make_entry('WARNING', Msg, List, 1).

%error(+Msg, +List)
error(Msg, List) :-
  ((nonvar(Msg), nonvar(List))
  -> true
  ;  logging:fatal('\'error\' requires bound arguments..')),
  make_entry('ERROR', Msg, List, 2).

%info(+Msg)
info(Msg) :-
  (nonvar(Msg)
  -> true
  ;  logging:fatal('\'info\' requires a bound argument..')),
  make_entry('INFO', Msg, [], 0).

%warning(+Msg)
warning(Msg) :-
  (nonvar(Msg)
  -> true
  ;  logging:fatal('\'warning\' requires a bound argument..')),
  make_entry('WARNING', Msg, [], 1).

%error(+Msg)
error(Msg) :-
  (nonvar(Msg)
  -> true
  ;  logging:fatal('\'error\' requires a bound argument..')),
  make_entry('ERROR', Msg, [], 2).
