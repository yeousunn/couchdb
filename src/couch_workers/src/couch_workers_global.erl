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

-module(couch_workers_global).

-behaviour(gen_server).


-export([
    start_link/0,
    subscribe/3,
    unsubscribe/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-define(ROLE_MONITOR_POLL_INTERVAL_MSEC, 5000).

-define(CACHE, couch_workers_global_cache).
-define(SUBSCRIBERS, couch_workers_global_subscribers).
-define(MONITOR_PIDS, couch_workers_global_monitor_pids).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, nil, []).


subscribe(WorkerType, Module, Pid) ->
    gen_server:call(?MODULE, {subscribe, WorkerType, Module, Pid}, infinity).


unsubscribe(Ref) ->
    gen_server:call(?MODULE, {unsubscribe, Ref}, infinity).


get_workers(WorkerType) ->
    case ets:lookup(?CACHE, WorkerType) of
        [{WorkerType, VS, Workers}] ->
            {ok, VS, Workers};
        [] ->
            {error, not_found}
    end.


%% gen_server callbacks

init(_) ->
    EtsOpts = [protected, named_table],
    % {WorkerType, VS, Workers}
    ets:new(?CACHE, EtsOpts ++ [{read_concurrency, true}]),
    % {{WorkerType, Ref}, Module, VS}
    ets:new(?SUBSCRIBERS, EtsOpts ++ [ordered_set]),
    % {WorkerType, Pid}
    ets:new(?MONITOR_PIDS, EtsOpts),
    {ok, nil}.


terminate(_, _St) ->
    ok.


handle_call({subscribe, WorkerType, Mod, Pid}, From, St) ->
    Ref = erlang:monitor(process, Pid),
    subscribe_int(WorkerType, Ref, Mod),
    gen_server:reply(From, Ref),
    case get_workers(WorkerType) of
        {ok, VS, Workers} ->
            do_callback(Mod, WorkerType, Workers, VS, Ref);
        {error, not_found} ->
            ok
    end,
    {noreply, St};

handle_call({unsubscribe, Ref}, _From, St) ->
    unsubscribe_int(Ref),
    {reply, ok, St};

handle_call({membership_update, WorkerType, VS, Workers}, _From, St) ->
    membership_update(WorkerType, VS, Workers),
    {reply, ok, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _Pid, _Reason}, St) ->
    unsubscribe_int(Ref),
    {reply, ok, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


%% Utility functions

membership_update(WorkerType, VS, Workers) ->
    true = ets:insert(?CACHE, {WorkerType, VS, Workers}),
    lists:foreach(fun
        ({_Ref, _Mod, VS}) ->  ok;
        ({Ref, Mod, _OldVs}) -> do_callback(Mod, WorkerType, Workers, VS, Ref)
    end, find_subscribers(WorkerType)).


do_callback(nil, _, _, _, _) ->
    % User didn't want a callback, they'll be polling the cache
    ok;

do_callback(Mod, WorkerType, Workers, VS, Ref) ->
    try
        Mod:couch_workers_membership_update(WorkerType, Workers, VS, Ref)
    catch
        Tag:Err ->
            ErrMsg = "~p : failed when calling callback Mod:~p WorkerType:~p ~p:~p",
            couch_log:error(ErrMsg, [?MODULE, Mod, WorkerType, Tag, Err])
    end.


subscribe_int(WorkerType, Ref, Module) ->
     true = ets:insert(?SUBSCRIBERS, {{WorkerType, Ref}, Module, nil}),
     maybe_start_role_monitor(WorkerType).


unsubscribe_int(Ref) ->
    case find_subscriber_role(Ref) of
        {ok, WorkerType} ->
            true = ets:delete(?SUBSCRIBERS, {WorkerType, Ref}),
            case find_subscribers(WorkerType) of
                [] ->
                    true = ets:delete(?CACHE, WorkerType),
                    stop_role_monitor(WorkerType);
                [_] ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.


find_subscriber_role(Ref) ->
    case ets:match(?SUBSCRIBERS, {{'$1', Ref}, '_'}) of
        [] -> {error, not_found};
        [[WorkerType]] -> {ok, WorkerType}
    end.


find_subscribers(WorkerType) ->
    % Using ETS partial key match in ordered set
    Subscribers = ets:match(?SUBSCRIBERS, {{WorkerType, '$1'}, '$2', '$3'}),
    lists:map(fun([Ref, Mod, VS]) -> {Ref, Mod, VS} end, Subscribers).


maybe_start_role_monitor(WorkerType) ->
    case ets:lookup(?MONITOR_PIDS, WorkerType) of
        [{_WorkerType, _Pid}] ->
            false;
        [] ->
            PollMSec = ?ROLE_MONITOR_POLL_INTERVAL_MSEC,
            Self = self(),
            Pid = spawn_link(fun() ->
                 role_monitor_loop(WorkerType, nil, Self, PollMSec)
            end),
            true = ets:insert(?MONITOR_PIDS, {WorkerType, Pid})
    end.


stop_role_monitor(WorkerType) ->
    [{_, Pid}] = ets:lookup(?MONITOR_PIDS, WorkerType),
    true = ets:delete(?MONITOR_PIDS, WorkerType),
    Ref = monitor(process, Pid),
    unlink(Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, _, _, _} -> ok end.



% Replace with a watch eventually but with a polling fallback
% in case we make too many watches
role_monitor_loop(WorkerType, VS, ReportPid, PollMSec) ->
    NewVS = case get_workers_vs(WorkerType) of
        VS ->
            VS;
        OtherVS when is_binary(OtherVS) ->
            {VS1, Workers} = get_workers_and_vs(WorkerType),
            CallMsg = {membership_updated, WorkerType, VS1, Workers},
            ok = gen_server:call(ReportPid, CallMsg, infinity),
            VS1
    end,
    timer:sleep(PollMSec),
    role_monitor_loop(WorkerType, NewVS, ReportPid, PollMSec).


get_workers_vs(WorkerType) ->
    fabric2_fdb:transactional(fun(Tx) ->
        couch_workers_fdb:get_workers_vs(Tx, WorkerType)
    end).


get_workers_and_vs(WorkerType) ->
    fabric2_fdb:transactional(fun(Tx) ->
        VS = couch_workers_fdb:get_workers(Tx, WorkerType),
        Workers = couch_workers_fdb:get_workers(Tx, WorkerType),
        {VS, Workers}
    end).
