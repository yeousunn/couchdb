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

-module(couch_workers_local).

-behaviour(gen_server).


-export([
    start_link/0,
    worker_register/4,
    worker_unregister/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-define(DEFAULT_HEALTH_TIMEOUT_SEC, 15).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, nil, []).


worker_register(WorkerType, Id, Opts, Pid) ->
    gen_server:call(?MODULE, {worker_register, WorkerType, Id, Opts, Pid}, infinity).


worker_unregister(Ref) ->
    get_server:call(?MODULE, {worker_unregsiter, Ref}, infinity).


init(_) ->
    % {Ref, WorkerType, Id, HealthPid}
    ets:new(?MODULE, [protected, named_table]),
    {ok, nil}.


terminate(_, _St) ->
    ok.


handle_call({worker_register, WorkerType, Id, Opts, Pid}, _From, St) ->
    worker_register_int(WorkerType, Id, Opts, Pid),
    {noreply, St};

handle_call({worker_unregister, Ref}, _From, St) ->
    worker_unregister_int(Ref),
    {reply, ok, St};


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _Pid, _Reason}, St) ->
    worker_unregister(Ref),
    {reply, ok, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


%% Utility functions

worker_register_int(WorkerType, Id, Pid, Opts) ->
    case ets:match(?MODULE, {'$1', WorkerType, Id, '_'}) of
        [] ->
            Ref = erlang:monitor(process, Pid),
            ok = set_worker(WorkerType, Id, Opts),
            Timeout = maps:get(timeout, Opts, ?DEFAULT_HEALTH_TIMEOUT_SEC),
            HPid = spawn_link(fun() ->
                health_pinger_loop(WorkerType, Id, Timeout)
            end),
            true = ets:insert(?MODULE, {Ref, WorkerType, Id, HPid}),
            Ref;
        [[Ref]] ->
            Ref
    end.


worker_unregister_int(Ref) ->
    case ets:lookup(?MODULE, Ref) of
        [{_, WorkerType, Id, HealthPid}] ->
            ok = clear_worker(WorkerType, Id),
            kill_health_pinger(HealthPid),
            true = ets:delete(?MODULE, Ref),
            ok;
        [] ->
            couch_log:error("~p : unknown worker reference ~p", [?MODULE, Ref]),
            ok
    end.


now_sec() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    Mega * 1000000 + Sec.


kill_health_pinger(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    unlink(Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, _, _, _} -> ok end.


health_pinger_loop(WorkerType, Id, Timeout) ->
    set_worker_health(WorkerType, Id, now_sec(), Timeout),
    % todo: dd jitter here
    timer:sleep(max(10, Timeout * 1000 / 3)),
    health_pinger_loop(WorkerType, Id, Timeout).


set_worker_health(WorkerType, Worker, TStamp, Timeout) ->
    fabric2_fdb:transactional(fun(Tx) ->
        couch_workers_fdb:set_worker_health(Tx, WorkerType, Worker, TStamp, Timeout)
    end).


set_worker(WorkerType, Worker, Opts) ->
    fabric2_fdb:transactional(fun(Tx) ->
        couch_workers_fdb:set_worker(Tx, WorkerType, Worker, Opts)
    end),
    ok.


clear_worker(WorkerType, Worker) ->
    fabric2_fdb:transactional(fun(Tx) ->
        couch_workers_fdb:clear_worker(Tx, WorkerType, Worker)
    end),
    ok.
