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

-module(couch_jobs_notifier).

-behaviour(gen_server).


-export([
    start_link/1,
    subscribe/3,
    unsubscribe/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include("couch_jobs.hrl").


-define(TYPE_MONITOR_HOLDOFF_DEFAULT, 250).
-define(TYPE_MONITOR_TIMEOUT_DEFAULT, "infinity").


-record(st, {
    jtx,
    type,
    monitor_pid,
    subs
}).


start_link(Type) ->
    gen_server:start_link(?MODULE, [Type], []).


subscribe(Server, JobId, Pid) when is_pid(Pid) ->
    gen_server:call(Server, {subscribe, JobId, Pid}, infinity).


unsubscribe(Server, Ref) when is_reference(Ref) ->
    gen_server:call(Server, {unsubscribe, Ref}, infinity).


init([Type]) ->
    % ETS schema {{JobId,Ref}, {SubscriberPid, LastState, Versionstamp}}
    Ets =  ets:new(?MODULE, [ordered_set, protected]),
    JTx = couch_jobs_fdb:get_jtx(),
    St = #st{jtx = JTx, type = Type, subs = Ets},
    VS = get_type_vs(St),
    HoldOff = get_holdoff(),
    Timeout = get_timeout(),
    Pid = couch_jobs_type_monitor:start(Type, VS, HoldOff, Timeout),
    {ok, St#st{monitor_pid = Pid}}.


terminate(_, _St) ->
    ok.


handle_call({subscribe, JobId, Pid}, _From, #st{} = St) ->
    Res = case get_job_state(St, JobId) of
        {error, not_found} ->
            {error, not_found};
        {ok, finished, _Seq} ->
            {ok, finished};
        {ok, State, Seq} ->
            Ref = erlang:monitor(process, Pid),
            ets:insert(St#st.subs, {{JobId, Ref}, {Pid, State, Seq}}),
            {ok, {Ref, State}}
    end,
    {reply, Res, St};

handle_call({unsubscribe, Ref}, _From, #st{subs = Subs} = St) ->
    true = ets:match_delete(Subs, {{'$1', Ref}, '_'}),
    erlang:demonitor(Ref, [flush]),
    {reply, ok, St};

handle_call({type_updated, VS}, _From, St) ->
    ok = notify_subscribers(VS, St),
    {reply, ok, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _, _}, #st{subs = Subs} = St) ->
    true = ets:match_delete(Subs, {{'$1', Ref}, '_'}),
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


get_job_state(#st{jtx = JTx, type = Type}, JobId) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        Job = #{job => true, type => Type, id => JobId},
        case couch_jobs_fdb:get_job_state_and_data(JTx1, Job) of
            {ok, Seq, State, _Data} ->
                {ok, State, Seq};
            {error, not_found} ->
                {error, not_found}
        end
    end).


get_jobs(#st{jtx = JTx, type = Type}, JobIds) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        lists:map(fun(JobId) ->
            Job = #{job => true, type => Type, id => JobId},
            case couch_jobs_fdb:get_job_state_and_data(JTx1, Job) of
                {ok, Seq, State, _Data} ->
                    {JobId, State, Seq};
                {error, not_found} ->
                    {JobId, not_found, null}
            end
        end, JobIds)
    end).


get_type_vs(#st{jtx = JTx, type = Type}) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_activity_vs(JTx1, Type)
    end).


% "Active since" is the list of jobs that have been active (running)
% and updated at least once since the given versionstamp. These are relatively
% cheap to find as it's just a range read in the ?ACTIVITY subspace.
%
get_active_since(#st{} = _St, not_found, _SubscribedJobs) ->
    [];

get_active_since(#st{jtx = JTx, type = Type}, VS, SubscribedJobs) ->
    AllUpdatedSet = sets:from_list(couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_active_since(JTx1, Type, VS)
    end)),
    SubscribedSet = sets:from_list(SubscribedJobs),
    SubscribedActiveSet = sets:intersection(AllUpdatedSet, SubscribedSet),
    sets:to_list(SubscribedActiveSet).


get_subscribers(JobId, #st{subs = Subs}) ->
    % Use ordered ets's fast matching of partial key prefixes here
    lists:map(fun([Ref, {Pid, State, VS}]) ->
        {Ref, Pid, State, VS}
    end, ets:match(Subs, {{JobId, '$1'}, '$2'})).


get_subscribed_job_ids(#st{subs = Subs}) ->
    Matches = ets:match(Subs, {{'$1', '_'}, '_'}),
    lists:usort(lists:flatten(Matches)).


notify_subscribers(ActiveVS, #st{subs = Subs, type = Type} = St) ->
    Ids = get_subscribed_job_ids(St),
    % First gather the easy (cheap) active jobs. Then with those out of way
    % inspect each job to get its state.
    Active = get_active_since(St, ActiveVS, Ids),
    JobStates = [{Id, running, ActiveVS} || Id <- Active],
    NotActive = Ids -- Active,
    JobStates1 = JobStates ++ get_jobs(St, NotActive),
    lists:foreach(fun({Id, State, VS}) ->
        lists:foreach(fun
            ({_, _, running, OldVS}) when State =:= running, OldVS >= VS ->
                ok;
            ({Ref, Pid, running, OldVS}) when State =:= running, OldVS < VS ->
                % For running state send updates even if state doesn't change
                notify(Pid, Ref, Type, Id, State),
                ets:insert(Subs, {{Id, Ref}, {Pid, running, VS}});
            ({_, _, OldState, _}) when OldState =:= State ->
                ok;
            ({Ref, Pid, _, _}) ->
                notify(Pid, Ref, Type, Id, State),
                ets:insert(Subs, {{Id, Ref}, {Pid, State, VS}})
        end, get_subscribers(Id, St)),
        case lists:member(State, [finished, not_found]) of
            true -> ets:match_delete(Subs, {{Id, '_'}, '_'});
            false -> ok
        end
    end, JobStates1).


notify(Pid, Ref, Type, Id, State) ->
    Pid ! {?COUCH_JOBS_EVENT, Ref, Type, Id, State}.


get_holdoff() ->
    config:get_integer("couch_jobs", "type_monitor_holdoff_msec",
        ?TYPE_MONITOR_HOLDOFF_DEFAULT).


get_timeout() ->
    Default =  ?TYPE_MONITOR_TIMEOUT_DEFAULT,
    case config:get("couch_jobs", "type_monitor_timeout_msec", Default) of
        "infinity" -> infinity;
        Milliseconds -> list_to_integer(Milliseconds)
    end.
