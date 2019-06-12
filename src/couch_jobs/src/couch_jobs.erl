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

-module(couch_jobs).

-export([
    % Job creation
    add/3,
    add/4,
    remove/1,
    resubmit/1,
    get_job_data/1,
    get_job_state/1,

    % Job processing
    accept/1,
    accept/2,
    finish/2,
    finish/3,
    resubmit/2,
    resubmit/3,
    is_resubmitted/1,
    update/2,
    update/3,

    % Subscriptions
    subscribe/1,
    subscribe/2,
    unsubscribe/1,
    wait/2,
    wait/3,

    % Type timeouts
    set_type_timeout/2,
    clear_type_timeout/1,
    get_type_timeout/1
]).


-include("couch_jobs.hrl").


-define(MIN_ACCEPT_WAIT_MSEC, 100).


%% Job Creation API

-spec add(job_type(), job_id(), job_data()) -> {ok, job()} | {error, any()}.
add(Type, JobId, JobData) ->
    add(Type, JobId, JobData, 0).


-spec add(job_type(), job_id(), job_data(), scheduled_time()) ->
    ok | {error, any()}.
add(Type, JobId, JobData, ScheduledTime) when is_map(JobData),
        is_integer(ScheduledTime) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:add(JTx, Type, JobId, JobData, ScheduledTime)
    end).


-spec remove(job()) -> ok | {error, any()}.
remove(#{job := true} = Job) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:remove(JTx, Job)
    end).


-spec resubmit(job()) -> {ok, job()} | {error, any()}.
resubmit(#{job := true} = Job) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Job, undefined)
    end).


-spec get_job_data(job()) -> {ok, job_data()} | {error, any()}.
get_job_data(#{job := true} = Job) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        case couch_jobs_fdb:get_job_state_and_data(JTx, Job) of
            {ok, _Seq, _State, Data} ->
                {ok, couch_jobs_fdb:decode_data(Data)};
            {error, Error} ->
                {error, Error}
        end
    end).


-spec get_job_state(job()) -> {ok, job_state()} | {error, any()}.
get_job_state(#{job := true} = Job) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        case couch_jobs_fdb:get_job_state_and_data(JTx, Job) of
            {ok, _Seq, State, _Data} ->
                {ok, State};
            {error, Error} ->
                {error, Error}
        end
    end).


%% Job processor API

-spec accept(job_type()) -> {ok, job()} | {error, any()}.
accept(Type) ->
    accept(Type, ?UNDEFINED_MAX_SCHEDULED_TIME).


-spec accept(job_type(), scheduled_time()) -> {ok, job()} | {error, any()}.
accept(Type, MaxSchedTime) ->
    TxFun =  fun(JTx) -> couch_jobs_fdb:accept(JTx, Type, MaxSchedTime) end,
    case couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), TxFun) of
        {ok, Job} ->
            {ok, Job};
        {not_found, PendingWatch} ->
            case wait_pending(PendingWatch, MaxSchedTime) of
                {error, not_found} ->
                    {error, not_found};
                ok ->
                    accept(Type, MaxSchedTime)
            end
    end.


-spec finish(jtx(), job()) -> ok | {error, any()}.
finish(Tx, Job) ->
    finish(Tx, Job, undefined).


-spec finish(jtx(), job(), job_data()) -> ok | {error, any()}.
finish(Tx, #{jlock := <<_/binary>>} = Job, JobData) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:finish(JTx, Job, JobData)
    end).


-spec resubmit(jtx(), job()) -> {ok, job()} | {error, any()}.
resubmit(Tx, Job) ->
    resubmit(Tx, Job, ?UNDEFINED_MAX_SCHEDULED_TIME).


-spec resubmit(jtx(), job(), scheduled_time()) -> {ok, job()} | {error, any()}.
resubmit(Tx, #{jlock := <<_/binary>>} = Job, SchedTime) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Job, SchedTime)
    end).


-spec is_resubmitted(job()) -> true | false.
is_resubmitted(#{job := true} = Job) ->
    maps:get(resubmit, Job, false).


-spec update(jtx(), job()) -> {ok, job()} | {error, any()}.
update(Tx, Job) ->
    update(Tx, Job, undefined).


-spec update(jtx(), job(), job_data()) -> {ok, job()} | {error, any()}.
update(Tx, #{jlock := <<_/binary>>} = Job, JobData) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:update(JTx, Job, JobData)
    end).


%% Subscription API

% Receive events as messages. Wait for them using `wait/2,3`
% functions.
%
-spec subscribe(job()) ->  {ok, job_subscription(), job_state()}
    | {ok, finished} | {error, any()}.
subscribe(#{job := true, type := Type, id := JobId}) ->
    subscribe(Type, JobId).


-spec subscribe(job_type(), job_id()) -> {ok, job_subscription(), job_state()}
    | {ok, finished} | {error, any()}.
subscribe(Type, JobId) ->
    case couch_jobs_server:get_notifier_server(Type) of
        {ok, Server} ->
            case couch_jobs_notifier:subscribe(Server, JobId, self()) of
                {ok, finished} -> {ok, finished};
                {error, not_found} -> {error, not_found};
                {ok, {Ref, JobState}} -> {ok, {Server, Ref}, JobState}
            end;
        {error, Error} ->
            {error, Error}
    end.


% Unsubscribe from getting notifications based on a particular subscription.
% Each subscription should be followed by its own unsubscription call. However,
% subscriber processes are also monitored and auto-unsubscribed if they exit.
% So the subscribing process is exiting, calling this function is optional.
%
-spec unsubscribe(job_subscription()) -> ok.
unsubscribe({Server, Ref}) when is_pid(Server), is_reference(Ref) ->
    try
        couch_jobs_notifier:unsubscribe(Server, Ref)
    after
        flush_notifications(Ref)
    end.


% Wait to receive job state updates
%
-spec wait(job_subscription() | [job_subscription()], timeout()) ->
    {job_type(), job_id(), job_state()} | timeout.
wait({_, Ref}, TimeoutMSec) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, Id, State} ->
            {Type, Id, State}
    after
        TimeoutMSec -> timeout
    end;

wait(SubscriptionIDs, TimeoutMSec) when is_list(SubscriptionIDs) ->
    {Result, ResendQ} = wait_any(SubscriptionIDs, TimeoutMSec, []),
    lists:foreach(fun(Msg) -> self() ! Msg end, ResendQ),
    Result.


-spec wait(job_subscription() | [job_subscription()], job_state(), timeout())
    -> {job_type(), job_id(), job_state()} | timeout.
wait({_, Ref} = Sub, State, TimeoutMSec) when is_atom(State) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, Id, MsgState} ->
            case MsgState =:= State of
                true -> {Type, Id, State};
                false -> wait(Sub, State, TimeoutMSec)
            end
    after
        TimeoutMSec -> timeout
    end;

wait(SubscriptionIDs, State, TimeoutMSec) when is_list(SubscriptionIDs),
        is_atom(State) ->
    {Result, ResendQ} = wait_any(SubscriptionIDs, State, TimeoutMSec, []),
    lists:foreach(fun(Msg) -> self() ! Msg end, ResendQ),
    Result.


%% Job type timeout API

% These functions manipulate the activity timeout for each job type.

-spec set_type_timeout(job_type(), timeout()) -> ok.
set_type_timeout(Type, Timeout) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:set_type_timeout(JTx, Type, Timeout)
    end).


-spec clear_type_timeout(job_type()) -> ok.
clear_type_timeout(Type) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:clear_type_timeout(JTx, Type)
    end).


-spec get_type_timeout(job_type()) -> timeout().
get_type_timeout(Type) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_type_timeout(JTx, Type)
    end).


%% Private utilities

wait_pending(PendingWatch, MaxSTime) ->
    NowMSec = erlang:system_time(millisecond),
    TimeoutMSec0 = max(?MIN_ACCEPT_WAIT_MSEC, MaxSTime * 1000 - NowMSec),
    TimeoutMSec = limit_timeout(TimeoutMSec0),
    try
        erlfdb:wait(PendingWatch, [{timeout, TimeoutMSec}]),
        ok
    catch
        error:{timeout, _} ->
            {error, not_found}
    end.


wait_any(SubscriptionIDs, Timeout0, ResendQ) when is_list(SubscriptionIDs) ->
    Timeout = limit_timeout(Timeout0),
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, Id, State} = Msg ->
            case lists:keyfind(Ref, 2, SubscriptionIDs) of
                false ->
                    wait_any(SubscriptionIDs, Timeout, [Msg | ResendQ]);
                {_, Ref} ->
                    {{Type, Id, State}, ResendQ}
            end
    after
        Timeout -> {timeout, ResendQ}
    end.


wait_any(SubscriptionIDs, State, Timeout0, ResendQ) when
        is_list(SubscriptionIDs) ->
    Timeout = limit_timeout(Timeout0),
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, Id, MsgState} = Msg ->
            case lists:keyfind(Ref, 2, SubscriptionIDs) of
                false ->
                    wait_any(SubscriptionIDs, Timeout, [Msg | ResendQ]);
                {_, Ref} ->
                    case MsgState =:= State of
                        true -> {{Type, Id, State}, ResendQ};
                        false -> wait_any(SubscriptionIDs, Timeout, ResendQ)
                    end
            end
    after
        Timeout -> {timeout, ResendQ}
    end.


limit_timeout(Timeout) when is_integer(Timeout), Timeout < 16#FFFFFFFF ->
    Timeout;

limit_timeout(_Timeout) ->
    infinity.


flush_notifications(Ref) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, _, _, _} ->
            flush_notifications(Ref)
    after
        0 -> ok
    end.
