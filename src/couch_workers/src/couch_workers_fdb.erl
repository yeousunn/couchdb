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

-module(couch_workers_fdb).

-export([
    set_worker/4,
    clear_worker/3,
    get_worker/3,

    set_worker_health/5,
    get_worker_health/3,
    get_workers_health/2,

    set_workers_vs/2,
    get_workers_vs/2,

    get_workers/2
]).


%% Switch these to numbers eventually
-define(COUCH_WORKERS, <<"couch_workers">>).
-define(JOBS, <<"jobs">>).
-define(PENDING, <<"pending">>).
-define(ACTIVE, <<"active">>).
-define(WORKERS, <<"workers">>).
-define(WORKERS_VS, <<"workers_vs">>).
-define(HEALTH, <<"health">>).

-define(uint2bin(I), binary:encode_unsigned(I, little)).
-define(bin2uint(I), binary:decode_unsigned(I, little)).
-define(UNSET_VS, {versionstamp, 16#FFFFFFFFFFFFFFFF, 16#FFFF}).

-define(PREFIX_CACHE, '$couch_worker_prefix').

%% (?COUCH_WORKERS, ?JOBS, JobType, JobId) = (JobState, WorkerId, Priority, CancelReq, JobInfo, JobOps)
%% (?COUCH_WORKERS, ?PENDING, JobType, Priority, JobId) = ""

%% (?COUCH_WORKERS, ?ACTIVE, WorkerType, Worker, JobId) = JobState

%% (?COUCH_WORKERS, WorkerType, ?WORKERS_VS) = VS
%% (?COUCH_WORKERS, WorkerType, ?WORKERS, Worker) = WOpts
%% (?COUCH_WORKERS, WorkerType, ?HEALTH, Worker) = (VS, TStamp, WorkerTimeout)


get_worker(Tx, WorkerType, Worker) ->
    Key = erlfdb_tuple:pack({?WORKERS, Worker}, workers_prefix(Tx, WorkerType)),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> = Val ->
            % Use json here
            binary_to_term(Val, [safe]);
        not_found ->
            not_found
    end.


set_worker(Tx, WorkerType, Worker, WOpts) ->
    Key = erlfdb_tuple:pack({?WORKERS, Worker}, workers_prefix(Tx, WorkerType)),
    case get_worker(Tx, WorkerType, Worker) of
        not_found ->
            set_workers_vs(Tx, WorkerType);
        #{} ->
            ok
    end,
    erlfdb:wait(erlfdb:set(Tx, Key, jiffy:encode(WOpts))).


clear_worker(Tx, WorkerType, Worker) ->
    Prefix = workers_prefix(Tx, WorkerType),
    case get_worker(Tx, WorkerType, Worker) of
        not_found ->
            ok;
        #{} ->
            WPrefix = erlfdb_tuple:pack({?WORKERS, Worker}, Prefix),
            erlfdb:clear_range_startswith(Tx, WPrefix),
            HPrefix = erlfdb_tuple:pack({?HEALTH, Worker}, Prefix),
            erlfdb:clear_range_startswith(Tx, HPrefix),
            set_workers_vs(Tx, WorkerType),
            ok
    end.


get_worker_health(Tx, WorkerType, Worker) ->
    Key = erlfdb_tuple:pack({?HEALTH, Worker}, workers_prefix(Tx, WorkerType)),
    Val = erlfdb:wait(erlfdb:get(Tx, Key)),
    {VS, TStamp, WorkerTimeout} = erlfdb_tuple:unpack(Val),
    {VS, TStamp, WorkerTimeout}.


get_workers_health(Tx, WorkerType) ->
    Prefix = workers_prefix(Tx, WorkerType),
    {Start, End} = erlfdb_tuple:range({?HEALTH}, Prefix),
    RawKVs = erlfdb:wait(erlfdb:get_range(Tx, Start, End)),
    KVs = lists:map(fun({K, V}) ->
        {?HEALTH, Worker} = erlfdb_tuple:unpack(K, Prefix),
        WOpts = jiffy:decode(V, [return_maps]),
        {Worker, WOpts}
    end, RawKVs),
    maps:from_list(KVs).


set_worker_health(Tx, WorkerType, Worker, TStamp, WorkerTimeout) when
        is_integer(TStamp), is_integer(WorkerTimeout) ->
    Key = erlfdb_tuple:pack({?HEALTH, Worker}, workers_prefix(Tx, WorkerType)),
    Val = erlfdb_tuple:pack({?UNSET_VS, TStamp, WorkerTimeout}),
    erlfdb:wait(erlfdb:set(Tx, Key, Val)).


get_workers_vs(Tx, WorkerType) ->
    % return a watch here eventually
    Key = erlfdb_tuple:pack({?WORKERS_VS}, workers_prefix(Tx, WorkerType)),
    erlfdb:wait(erlfdb:get(Tx, Key)).


set_workers_vs(Tx, WorkerType) ->
    % return a watch here eventually
    Key = erlfdb_tuple:pack({?WORKERS_VS}, workers_prefix(Tx, WorkerType)),
    Val = erlfdb_tuple:pack({?UNSET_VS}),
    erlfdb:wait(erlfdb:set(Tx, Key, Val)).


get_workers(Tx, WorkerType) ->
    Prefix = workers_prefix(Tx, WorkerType),
    {Start, End} = erlfdb_tuple:range({?WORKERS}, Prefix),
    RawKVs = erlfdb:wait(erlfdb:get_range(Tx, Start, End)),
    KVs = lists:map(fun({K, V}) ->
        {?WORKERS, Worker} = erlfdb_tuple:unpack(K, Prefix),
        WOpts = jiffy:decode(V, [return_maps]),
        {Worker, WOpts}
    end, RawKVs),
    maps:from_list(KVs).


workers_prefix(Tx, WorkerType) ->
    case get({?PREFIX_CACHE, WorkerType}) of
        undefined ->
            Root = erlfdb_directory:root(),
            CouchDB = erlfdb_directory:create_or_open(Tx, Root, [<<"couchdb">>]),
            Prefix = erlfdb_directory:get_name(CouchDB),
            Res = erlfdb_tuple:pack({?COUCH_WORKERS, WorkerType}, Prefix),
            put({?PREFIX_CACHE, WorkerType}, Res),
            Res;
        Res ->
            Res
    end.
