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

-module(couch_jobs_fdb).


-export([
    add/4,
    remove/3,
    resubmit/4,
    get_job/3,
    get_jobs/2,
    get_jobs/1,

    accept/3,
    finish/5,
    resubmit/5,
    update/5,

    set_type_timeout/3,
    clear_type_timeout/2,
    get_type_timeout/2,
    get_types/1,

    get_activity_vs/2,
    get_activity_vs_and_watch/2,
    get_active_since/3,
    get_inactive_since/3,
    re_enqueue_inactive/3,

    init_cache/0,

    get_jtx/0,
    get_jtx/1,
    tx/2,

    has_versionstamp/1,

    clear_jobs/0,
    clear_type/1
]).


-include("couch_jobs.hrl").


% Data model
%
% (?JOBS, ?DATA, Type, JobId) = (Sequence, WorkerLockId, Priority, JobOpts)
% (?JOBS, ?PENDING, Type, Priority, JobId) = ""
% (?JOBS, ?WATCHES, Type) = Sequence
% (?JOBS, ?ACTIVITY_TIMEOUT, Type) = ActivityTimeout
% (?JOBS, ?ACTIVITY, Type, Sequence) = JobId

% Job creation API

add(#{jtx := true} = JTx0, Type, JobId, JobOpts) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    case get_type_timeout(JTx, Type) of
        not_found ->
            {error, no_type_timeout};
        Int when is_integer(Int) ->
            Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
            case erlfdb:wait(erlfdb:get(Tx, Key)) of
                <<_/binary>> -> {error, duplicate_job};
                not_found -> maybe_enqueue(JTx, Type, JobId, JobOpts, true)
            end
    end.


remove(#{jtx := true} = JTx0, Type, JobId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {_, WorkerLockId, _Priority, _} = Job when WorkerLockId =/= null ->
            ok = cancel(JTx, Key, Job),
            canceled;
        {_, _WorkerLockId, Priority, _} when Priority =/= null ->
            couch_jobs_pending:remove(JTx, Type, Priority, JobId),
            erlfdb:clear(Tx, Key),
            ok;
        {_, _WorkerLockId, _Priority, _} ->
            erlfdb:clear(Tx, Key),
            ok;
        not_found ->
            not_found
    end.


resubmit(#{jtx := true} = JTx, Type, JobId, NewPriority) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {Seq, WorkerLockId, Priority, #{} = JobOpts} ->
            JobOpts1 = JobOpts#{?OPT_RESUBMIT => true},
            JobOpts2 = update_priority(JobOpts1, NewPriority),
            JobOptsEnc = jiffy:encode(JobOpts2),
            Val = erlfdb_tuple:pack({Seq, WorkerLockId, Priority, JobOptsEnc}),
            erlfdb:set(Tx, Key, Val),
            ok;
        not_found ->
            not_found
    end.


get_job(#{jtx := true} = JTx, Type, JobId) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {_, WorkerLockId, Priority, JobOpts} ->
            {ok, JobOpts, job_state(WorkerLockId, Priority)};
        not_found ->
            not_found
    end.


get_jobs(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?DATA, Type}, Jobs),
    Opts = [{streaming_mode, want_all}],
    Result = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    lists:map(fun({K, V}) ->
        {JobId} = erlfdb_tuple:unpack(K, Prefix),
        {_Seq, WorkerLockId, Priority, JobOpts} = unpack_job(V),
        JobState = job_state(WorkerLockId, Priority),
        {JobId, JobState, JobOpts}
    end, Result).


get_jobs(#{jtx := true} = JTx) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?DATA}, Jobs),
    Opts = [{streaming_mode, want_all}],
    Result = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    lists:map(fun({K, V}) ->
        {Type, JobId} = erlfdb_tuple:unpack(K, Prefix),
        {_Seq, WorkerLockId, Priority, JobOpts} = unpack_job(V),
        JobState = job_state(WorkerLockId, Priority),
        {Type, JobId, JobState, JobOpts}
    end, Result).


% Worker public API

accept(#{jtx := true} = JTx0, Type, MaxPriority) ->
    #{jtx := true} = JTx = get_jtx(JTx0),
    case couch_jobs_pending:dequeue(JTx, Type, MaxPriority) of
        not_found ->
            not_found;
        <<_/binary>> = JobId ->
            WorkerLockId = fabric2_util:uuid(),
            update_lock(JTx, Type, JobId, WorkerLockId),
            update_activity(JTx, Type, JobId, null),
            {ok, JobId, WorkerLockId}
    end.


finish(#{jtx := true} = JTx0, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job_and_status(Tx, Key, WorkerLockId) of
        {Status, {Seq, _, _, JobOptsCur}} when
                Status =:= ok orelse Status =:= canceled ->
            % If the job was canceled, allow updating its data one last time
            JobOpts1 = maps:merge(JobOptsCur, JobOpts),
            Resubmit = maps:get(?OPT_RESUBMIT, JobOpts1, false) == true,
            maybe_enqueue(JTx, Type, JobId, JobOpts1, Resubmit),
            clear_activity(JTx, Type, Seq),
            update_watch(JTx, Type),
            ok;
        {worker_conflict, _} ->
            worker_conflict
    end.


resubmit(#{jtx := true} = JTx0, Type, JobId, NewPriority, WorkerLockId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job_and_status(Tx, Key, WorkerLockId) of
        {ok, {Seq, WorkerLockId, null, JobOpts}} ->
            update_activity(JTx, Type, JobId, Seq),
            JobOpts1 = JobOpts#{?OPT_RESUBMIT => true},
            JobOpts2 = update_priority(JobOpts1, NewPriority),
            update_job(JTx, Type, JobId, WorkerLockId, JobOpts2, Seq),
            ok;
        {Status, _} when Status =/= ok ->
            Status
    end.


update(#{jtx := true} = JTx, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job_and_status(Tx, Key, WorkerLockId) of
        {ok, {Seq, WorkerLockId, null, JobOptsCur}} ->
            JobOpts1 = maps:merge(JobOptsCur, JobOpts),
            update_job(JTx, Type, JobId, WorkerLockId, JobOpts1, Seq),
            ok;
        {Status, _} when Status =/= ok ->
            Status
    end.


% Type and activity monitoring API

set_type_timeout(#{jtx := true} = JTx, Type, Timeout) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT, Type}, Jobs),
    Val = erlfdb_tuple:pack({Timeout}),
    erlfdb:set(Tx, Key, Val).


clear_type_timeout(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT, Type}, Jobs),
    erlfdb:clear(Tx, Key).


get_type_timeout(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT, Type}, Jobs),
    case erlfdb:wait(erlfdb:get_ss(Tx, Key)) of
        not_found ->
            not_found;
        Val ->
            {Timeout} = erlfdb_tuple:unpack(Val),
            Timeout
    end.


get_types(#{jtx := true} = JTx) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT}, Jobs),
    Opts = [{streaming_mode, want_all}],
    Result = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
    lists:map(fun({K, _V}) ->
        {Type} = erlfdb_tuple:unpack(K, Prefix),
        Type
    end, Result).


get_activity_vs(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        not_found ->
            not_found;
        Val ->
            {VS} = erlfdb_tuple:unpack(Val),
            VS
    end.


get_activity_vs_and_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    Future = erlfdb:get(Tx, Key),
    Watch = erlfdb:watch(Tx, Key),
    case erlfdb:wait(Future) of
        not_found ->
            {not_found, Watch};
        Val ->
            {VS} = erlfdb_tuple:unpack(Val),
            {VS, Watch}
    end.


get_active_since(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?ACTIVITY}, Jobs),
    StartKey = erlfdb_tuple:pack({Type, Versionstamp}, Prefix),
    StartKeySel = erlfdb_key:first_greater_than(StartKey),
    {_, EndKey} = erlfdb_tuple:range({Type}, Prefix),
    Opts = [{streaming_mode, want_all}],
    Future = erlfdb:get_range(Tx, StartKeySel, EndKey, Opts),
    lists:map(fun({_K, JobId}) -> JobId end, erlfdb:wait(Future)).


get_inactive_since(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?ACTIVITY}, Jobs),
    {StartKey, _} = erlfdb_tuple:range({Type}, Prefix),
    EndKey = erlfdb_tuple:pack({Type, Versionstamp}, Prefix),
    EndKeySel = erlfdb_key:first_greater_than(EndKey),
    Opts = [{streaming_mode, want_all}],
    Future = erlfdb:get_range(Tx, StartKey, EndKeySel, Opts),
    lists:map(fun({_K, JobId}) -> JobId end, erlfdb:wait(Future)).


re_enqueue_inactive(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    JobIds = get_inactive_since(JTx, Type, Versionstamp),
    lists:foreach(fun(JobId) ->
        Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
        {Seq, _, _, JobOpts} = get_job(Tx, Key),
        clear_activity(JTx, Type, Seq),
        maybe_enqueue(JTx, Type, JobId, JobOpts, true)
    end, JobIds),
    case length(JobIds) > 0 of
        true -> update_watch(JTx, Type);
        false -> ok
    end,
    JobIds.


% Cache initialization API. Called from the supervisor just to create the ETS
% table. It returns `ignore` to tell supervisor it won't actually start any
% process, which is what we want here.
%
init_cache() ->
    ConcurrencyOpts = [{read_concurrency, true}, {write_concurrency, true}],
    ets:new(?MODULE, [public, named_table] ++ ConcurrencyOpts),
    ignore.


% Cached job transaction object. This object wraps a transaction, caches the
% directory lookup path, and the metadata version. The function can be used
% from or outside the transaction. When used from a transaction it will verify
% if the metadata was changed, and will refresh automatically.
%
get_jtx() ->
    get_jtx(undefined).


get_jtx(#{tx := Tx} = _TxDb) ->
    get_jtx(Tx);

get_jtx(undefined = _Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] ->
            JTx;
        [] ->
            JTx = update_jtx_cache(init_jtx(undefined)),
            JTx#{tx := undefined}
    end;

get_jtx({erlfdb_transaction, _} = Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] ->
            ensure_current(JTx#{tx := Tx});
        [] ->
            update_jtx_cache(init_jtx(Tx))
    end.


% Transaction processing to be used with couch jobs' specific transaction
% contexts
%
tx(#{jtx := true} = JTx, Fun) when is_function(Fun, 1) ->
    fabric2_fdb:transactional(JTx, Fun).


% Utility fdb functions used by other module in couch_job. Maybe move these to
% a separate module if the list keep growing

has_versionstamp(?UNSET_VS) ->
    true;

has_versionstamp(Tuple) when is_tuple(Tuple) ->
    has_versionstamp(tuple_to_list(Tuple));

has_versionstamp([Elem | Rest]) ->
    has_versionstamp(Elem) orelse has_versionstamp(Rest);

has_versionstamp(_Other) ->
    false.


% Debug and testing API

clear_jobs() ->
    fabric2_fdb:transactional(fun(Tx) ->
        #{jobs_path := Jobs} = init_jtx(Tx),
        erlfdb:clear_range_startswith(Tx, Jobs)
    end).


clear_type(Type) ->
    Sections = [?DATA, ?PENDING, ?WATCHES, ?ACTIVITY_TIMEOUT, ?ACTIVITY],
    fabric2_fdb:transactional(fun(Tx) ->
        #{jobs_path := Jobs} = init_jtx(Tx),
        lists:foreach(fun(Section) ->
            Prefix = erlfdb_tuple:pack({Section, Type}, Jobs),
            erlfdb:clear_range_startswith(Tx, Prefix)
        end, Sections)
    end).


% Private helper functions

update_job(JTx, Type, JobId, WorkerLockId, JobOpts, OldSeq) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    update_activity(JTx, Type, JobId, OldSeq),
    ValTup = {?UNSET_VS, WorkerLockId, null, jiffy:encode(JobOpts)},
    Val = erlfdb_tuple:pack_vs(ValTup),
    erlfdb:set_versionstamped_value(Tx, Key, Val).


update_priority(JobOpts, undefined) ->
    JobOpts;

update_priority(JobOpts, NewPriority) ->
    OldPriority = maps:get(?OPT_PRIORITY, JobOpts, undefined),
    case NewPriority =/= OldPriority of
        true -> JobOpts#{?OPT_PRIORITY => NewPriority};
        false -> JobOpts
    end.


cancel(#{jx := true}, _, {_, _, _, #{?OPT_CANCEL := true}}) ->
    ok;

cancel(#{jtx := true, tx := Tx}, Key, Job) ->
    {Seq, WorkerLockId, Priority, JobOpts} = Job,
    JobOpts1 = JobOpts#{?OPT_CANCEL => true},
    JobOptsEnc = jiffy:encode(JobOpts1),
    Val = erlfdb_tuple:pack({Seq, WorkerLockId, Priority, JobOptsEnc}),
    erlfdb:set(Tx, Key, Val),
    ok.


maybe_enqueue(#{jtx := true} = JTx, Type, JobId, JobOpts, Resubmit) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    Cancel = maps:get(?OPT_CANCEL, JobOpts, false) == true,
    JobOpts1 = maps:without([?OPT_RESUBMIT], JobOpts),
    Priority = maps:get(?OPT_PRIORITY, JobOpts1, ?UNSET_VS),
    JobOptsEnc = jiffy:encode(JobOpts1),
    case Resubmit andalso not Cancel of
        true ->
            case has_versionstamp(Priority) of
                true ->
                    Val = erlfdb_tuple:pack_vs({null, null, Priority,
                        JobOptsEnc}),
                    erlfdb:set_versionstamped_value(Tx, Key, Val);
                false ->
                    Val = erlfdb_tuple:pack({null, null, Priority,
                        JobOptsEnc}),
                    erlfdb:set(Tx, Key, Val)
            end,
            couch_jobs_pending:enqueue(JTx, Type, Priority, JobId);
        false ->
            Val = erlfdb_tuple:pack({null, null, null, JobOptsEnc}),
            erlfdb:set(Tx, Key, Val)
    end,
    ok.


get_job(Tx = {erlfdb_transaction, _}, Key) ->
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> = Val -> unpack_job(Val);
        not_found -> not_found
    end.


unpack_job(<<_/binary>> = JobVal) ->
    {Seq, WorkerLockId, Priority, JobOptsEnc} = erlfdb_tuple:unpack(JobVal),
    JobOpts = jiffy:decode(JobOptsEnc, [return_maps]),
    {Seq, WorkerLockId, Priority, JobOpts}.


get_job_and_status(Tx, Key, WorkerLockId) ->
    case get_job(Tx, Key) of
        {_, LockId, _, _} = Res when WorkerLockId =/= LockId ->
            {worker_conflict, Res};
        {_, _, _, #{?OPT_CANCEL := true}} = Res ->
            {canceled, Res};
        {_, _, _, #{}} = Res ->
            {ok, Res};
        not_found ->
            {worker_conflict, not_found}
    end.


update_activity(#{jtx := true} = JTx, Type, JobId, Seq) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    case Seq =/= null of
        true -> clear_activity(JTx, Type, Seq);
        false -> ok
    end,
    Key = erlfdb_tuple:pack_vs({?ACTIVITY, Type, ?UNSET_VS}, Jobs),
    erlfdb:set_versionstamped_key(Tx, Key, JobId),
    update_watch(JTx, Type).


clear_activity(#{jtx := true} = JTx, Type, Seq) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?ACTIVITY, Type, Seq}, Jobs),
    erlfdb:clear(Tx, Key).


update_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    Val = erlfdb_tuple:pack_vs({?UNSET_VS}),
    erlfdb:set_versionstamped_value(Tx, Key, Val).


update_lock(#{jtx := true} = JTx, Type, JobId, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    {null, null, _, JobOpts} = get_job(Tx, Key),
    ValTup = {?UNSET_VS, WorkerLockId, null, jiffy:encode(JobOpts)},
    Val = erlfdb_tuple:pack_vs(ValTup),
    erlfdb:set_versionstamped_value(Tx, Key, Val).


job_state(WorkerLockId, Priority) ->
    case {WorkerLockId, Priority} of
        {null, null} ->
            finished;
        {WorkerLockId, _} when WorkerLockId =/= null ->
            running;
        {_, Priority} when Priority =/= null ->
            pending
    end.


% This a transaction context object similar to the Db = #{} one from
% fabric2_fdb. It's is used to cache the jobs path directory (to avoid extra
% lookups on every operation) and to check for metadata changes (in case
% directory changes).
%
init_jtx(undefined) ->
    fabric2_fdb:transactional(fun(Tx) -> init_jtx(Tx) end);

init_jtx({erlfdb_transaction, _} = Tx) ->
    Root = erlfdb_directory:root(),
    CouchDB = erlfdb_directory:create_or_open(Tx, Root, [<<"couchdb">>]),
    LayerPrefix = erlfdb_directory:get_name(CouchDB),
    Jobs = erlfdb_tuple:pack({?JOBS}, LayerPrefix),
    Version = erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)),
    % layer_prefix, md_version and tx here match db map fields in fabric2_fdb
    % but we also assert that this is a job transaction using the jtx => true
    % field
    #{
        jtx => true,
        tx => Tx,
        layer_prefix => LayerPrefix,
        jobs_path => Jobs,
        md_version => Version
    }.


ensure_current(#{jtx := true, tx := Tx, md_version := Version} = JTx) ->
    case erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)) of
        Version -> JTx;
        _NewVersion -> update_jtx_cache(init_jtx(Tx))
    end.


update_jtx_cache(#{jtx := true} = JTx) ->
    CachedJTx = JTx#{tx := undefined},
    ets:insert(?MODULE, {?JOBS, CachedJTx}),
    JTx.
