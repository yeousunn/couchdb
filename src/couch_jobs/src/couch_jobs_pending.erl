% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_jobs_pending).


-export([
    enqueue/4,
    dequeue/3,
    remove/4
]).


-include("couch_jobs.hrl").


-define(RANGE_LIMIT, 1024).


enqueue(#{jtx := true} = JTx, Type, STIme, JobId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Key = erlfdb_tuple:pack({?PENDING, Type, STIme, JobId}, Jobs),
    erlfdb:set(Tx, Key, <<>>),
    WatchKey = erlfdb_tuple:pack({?WATCHES_PENDING, Type}, Jobs),
    WatchVal = erlfdb_tuple:pack_vs({?UNSET_VS}),
    erlfdb:set_versionstamped_value(Tx, WatchKey, WatchVal),
    ok.


dequeue(#{jtx := true} = JTx, Type, MaxPriority) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Prefix = erlfdb_tuple:pack({?PENDING, Type}, Jobs),
    StartKeySel = erlfdb_key:first_greater_than(Prefix),
    End = erlfdb_tuple:pack({MaxPriority, <<16#FF>>}, Prefix),
    EndKeySel = erlfdb_key:first_greater_or_equal(End),
    case clear_random_key_from_range(Tx, StartKeySel, EndKeySel) of
        not_found ->
            {not_found, get_pending_watch(JTx, Type)};
        <<_/binary>> = PendingKey ->
            {_, JobId} = erlfdb_tuple:unpack(PendingKey, Prefix),
            {ok, JobId}
    end.


remove(#{jtx := true} = JTx, Type, JobId, STime) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Key = erlfdb_tuple:pack({?PENDING, Type, STime, JobId}, Jobs),
    erlfdb:clear(Tx, Key).


% Pick a random key from the range snapshot. Then radomly pick a key to clear.
% Before clearing, ensure there is a read conflict on the key in in case other
% workers have picked the same key.
%
clear_random_key_from_range(Tx, Start, End) ->
    Opts = [
        {limit, ?RANGE_LIMIT},
        {snapshot, true}
    ],
    case erlfdb:wait(erlfdb:get_range(Tx, Start, End, Opts)) of
        [] ->
            not_found;
        [{_, _} | _] = KVs ->
            Index = rand:uniform(length(KVs)),
            {Key, _} = lists:nth(Index, KVs),
            erlfdb:add_read_conflict_key(Tx, Key),
            erlfdb:clear(Tx, Key),
            Key
    end.


get_pending_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = couch_jobs_fdb:get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES_PENDING, Type}, Jobs),
    erlfdb:watch(Tx, Key).
