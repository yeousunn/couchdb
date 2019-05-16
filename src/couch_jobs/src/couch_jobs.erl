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
    add/3,
    remove/2,
    stop_and_remove/3,
    resubmit/2,
    resubmit/3,
    get_job/2,
    get_jobs/1,
    get_jobs/0,

    accept/1,
    accept/2,
    finish/5,
    resubmit/5,
    update/5,

    subscribe/2,
    subscribe/3,
    unsubscribe/1,
    wait_job_state/2,
    wait_job_state/3,

    set_type_timeout/2,
    clear_type_timeout/1,
    get_type_timeout/1
]).


-include("couch_jobs.hrl").


%% Job Creation API

-spec add(job_type(), job_id(), job_opts()) -> ok | {error, any()}.
add(Type, JobId, JobOpts) ->
    try validate_job_opts(JobOpts) of
        ok ->
            couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
                couch_jobs_fdb:add(JTx, Type, JobId, JobOpts)
            end)
    catch
        Tag:Err -> {error, {invalid_args, {Tag, Err}}}
    end.


-spec remove(job_type(), job_id()) -> ok | not_found | canceled.
remove(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:remove(JTx, Type, JobId)
    end).


-spec stop_and_remove(job_type(), job_id(), timeout()) ->
    ok | not_found | timeout.
stop_and_remove(Type, JobId, Timeout) ->
    case remove(Type, JobId) of
        not_found ->
            not_found;
        ok ->
            ok;
        canceled ->
            case subscribe(Type, JobId) of
                not_found ->
                    not_found;
                finished ->
                    ok = remove(Type, JobId);
                {ok, SubId, _JobState} ->
                    case wait_job_state(SubId, finished, Timeout) of
                        timeout ->
                            timeout;
                        {Type, JobId, finished} ->
                            ok = remove(Type, JobId)
                    end
            end
    end.


-spec resubmit(job_type(), job_id()) -> ok | not_found.
resubmit(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId, undefined)
    end).


-spec resubmit(job_type(), job_id(), job_priority()) -> ok | not_found.
resubmit(Type, JobId, NewPriority) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId, NewPriority)
    end).


-spec get_job(job_type(), job_id()) -> {ok, job_opts(), job_state()}
    | not_found.
get_job(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_job(JTx, Type, JobId)
    end).


-spec get_jobs(job_type()) -> [{job_id(), job_state(), job_opts()}].
get_jobs(Type) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_jobs(JTx, Type)
    end).


-spec get_jobs() -> [{job_type(), job_id(), job_state(), job_opts()}].
get_jobs() ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_jobs(JTx)
    end).


%% Worker Implementation API

-spec accept(job_type()) -> {ok, job_id(), worker_lock()} | not_found.
accept(Type) ->
    accept(Type, undefined).


-spec accept(job_type(), job_priority()) -> {ok, job_id(), worker_lock()}
    | not_found.
accept(Type, MaxPriority) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:accept(JTx, Type, MaxPriority)
    end).


-spec finish(jtx(), job_type(), job_id(), job_opts(), worker_lock()) -> ok |
    worker_conflict | no_return().
finish(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:finish(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


-spec resubmit(jtx(), job_type(), job_id(), job_priority() | undefined,
    worker_lock()) -> ok | worker_conflict | canceled | no_return().
resubmit(Tx, Type, JobId, NewPriority, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId, NewPriority, WorkerLockId)
    end).


-spec update(jtx(), job_type(), job_id(), job_opts(), worker_lock()) -> ok |
    worker_conflict | canceled | no_return().
update(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:update(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


%% Subscription API

% Receive events as messages. Wait for them using `wait_job_state/2,3`
% functions.
%
-spec subscribe(job_type(), job_id()) -> {ok, job_subscription(), job_state()}
    | not_found | {error, any()}.
subscribe(Type, JobId) ->
    case couch_jobs_server:get_notifier_server(Type) of
        {ok, Server} ->
            case couch_jobs_notifier:subscribe(Server, JobId, self()) of
                {Ref, JobState} -> {ok, {Server, Ref}, JobState};
                not_found -> not_found;
                finished -> finished
            end;
        {error, Error} ->
            {error, Error}
    end.


% Receive events as callbacks. Callback arguments will be:
%    Fun(SubscriptionRef, Type, JobId, JobState)
%
% Returns:
%   - {ok, SubscriptionRef, CurrentState} where:
%      - SubscriptionRef is opaque reference for the subscription
%      - CurrentState is the current job state
%   - `not_found` if job was not found
%   - `finished` if job is already in the `finished` state
%
-spec subscribe(job_type(), job_id(), job_callback()) -> {ok,
    job_subscription(), job_state()} | not_found | {error, any()}.
subscribe(Type, JobId, Fun) when is_function(Fun, 3) ->
    case couch_jobs_server:get_notifier_server(Type) of
        {ok, Server} ->
            case couch_jobs_notifier:subscribe(Server, JobId, Fun, self()) of
                {Ref, JobState} -> {ok, {Server, Ref}, JobState};
                not_found -> not_found;
                finished -> finished
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


% Wait to receive job state updates as messages.
%
-spec wait_job_state(job_subscription(), timeout()) -> {job_type(), job_id(),
    job_state()} | timeout.
wait_job_state({_, Ref}, Timeout) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, JobId, JobState} ->
            {Type, JobId, JobState}
    after
        Timeout -> timeout
    end.


% Wait for a particular job state received as a message.
%
-spec wait_job_state(job_subscription(), job_state(), timeout()) ->
    {job_type(), job_id(), job_state()} | timeout.
wait_job_state({_, Ref}, JobState, Timeout) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, JobId, JobState} ->
            {Type, JobId, JobState}
    after
        Timeout -> timeout
    end.


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

validate_job_opts(#{} = JobOpts) ->
    jiffy:encode(JobOpts),
    case maps:get(?OPT_RESUBMIT, JobOpts, undefined) of
        undefined -> ok;
        true -> ok;
        Resubmit -> error({invalid_resubmit, Resubmit, JobOpts})
    end,
    case maps:get(?OPT_PRIORITY, JobOpts, undefined) of
        undefined -> ok;
        Binary when is_binary(Binary) -> ok;
        Int when is_integer(Int), Int >= 0 -> ok;
        Priority -> error({invalid_priority, Priority, JobOpts})
    end.


flush_notifications(Ref) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, _, _, _} ->
            flush_notifications(Ref)
    after
        0 -> ok
    end.
