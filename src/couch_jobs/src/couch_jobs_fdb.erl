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
    add/5,
    remove/2,
    get_job_state_and_data/2,

    accept/3,
    finish/3,
    resubmit/3,
    update/3,

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

    encode_data/1,
    decode_data/1,

    get_jtx/0,
    get_jtx/1,
    tx/2,

    get_job/2,
    get_jobs/0,
    clear_jobs/0
]).


-include("couch_jobs.hrl").


-record(jv, {
    seq,
    jlock,
    stime,
    resubmit,
    data
}).

% Data model
%
% (?JOBS, ?DATA, Type, JobId) = (Sequence, Lock, SchedTime, Resubmit, JobData)
% (?JOBS, ?PENDING, Type, ScheduledTime, JobId) = ""
% (?JOBS, ?WATCHES_PENDING, Type) = Sequence
% (?JOBS, ?WATCHES_ACTIVITY, Type) = Sequence
% (?JOBS, ?ACTIVITY_TIMEOUT, Type) = ActivityTimeout
% (?JOBS, ?ACTIVITY, Type, Sequence) = JobId


% Job creation API

add(#{jtx := true} = JTx0, Type, JobId, Data, STime) ->
    #{tx := Tx} = JTx = get_jtx(JTx0),
    Job = #{job => true, type => Type, id => JobId},
    case get_type_timeout(JTx, Type) of
        not_found ->
            {error, no_type_timeout};
        Int when is_integer(Int) ->
            Key = job_key(JTx, Job),
            case erlfdb:wait(erlfdb:get(Tx, Key)) of
                <<_/binary>> ->
                    {ok, _} = resubmit(JTx, Job, STime),
                    ok;
                not_found ->
                    try
                        maybe_enqueue(JTx, Type, JobId, STime, true, Data)
                    catch
                        error:{json_encoding_error, Error} ->
                            {error, {json_encoding_error, Error}}
                    end
            end
    end.


remove(#{jtx := true} = JTx0, #{job := true} = Job) ->
    #{tx := Tx} = JTx = get_jtx(JTx0),
    #{type := Type, id := JobId} = Job,
    Key = job_key(JTx, Job),
    case get_job_val(Tx, Key) of
        #jv{stime = STime} ->
            couch_jobs_pending:remove(JTx, Type, JobId, STime),
            erlfdb:clear(Tx, Key),
            ok;
        not_found ->
            {error, not_found}
    end.


get_job_state_and_data(#{jtx := true} = JTx, #{job := true} = Job) ->
    case get_job_val(get_jtx(JTx), Job) of
        #jv{seq = Seq, jlock = JLock, data = Data} ->
            {ok, Seq, job_state(JLock, Seq), Data};
        not_found ->
            {error, not_found}
    end.


% Job processor API

accept(#{jtx := true} = JTx0, Type, MaxSTime)
        when is_integer(MaxSTime) orelse MaxSTime =:= undefined ->
    #{jtx := true, tx := Tx} = JTx = get_jtx(JTx0),
    case couch_jobs_pending:dequeue(JTx, Type, MaxSTime) of
        {not_found, PendingWatch} ->
            {not_found, PendingWatch};
        {ok, <<_/binary>> = JobId} ->
            JLock = fabric2_util:uuid(),
            Key = job_key(JTx, Type, JobId),
            #jv{jlock = null} = JV0 = get_job_val(Tx, Key),
            JV = JV0#jv{seq = ?UNSET_VS, jlock = JLock, resubmit = false},
            set_job_val(Tx, Key, JV),
            update_activity(JTx, Type, JobId, null),
            Job = #{
                job => true,
                type => Type,
                id => JobId,
                jlock => JLock
            },
            {ok, Job}
    end.


finish(#{jtx := true} = JTx0, #{jlock := <<_/binary>>} = Job, Data) when
        is_map(Data) orelse Data =:= undefined ->
    #{tx := Tx} = JTx = get_jtx(JTx0),
    #{type := Type, jlock := JLock, id := JobId} = Job,
    case get_job_or_halt(Tx, job_key(JTx, Job), JLock) of
        #jv{seq = Seq, stime = STime, resubmit = Resubmit, data = OldData} ->
            NewData = case Data =:= undefined of
                true -> OldData;
                false -> Data
            end,
            try maybe_enqueue(JTx, Type, JobId, STime, Resubmit, NewData) of
                ok ->
                    clear_activity(JTx, Type, Seq),
                    update_watch(JTx, Type)
            catch
                error:{json_encoding_error, Error} ->
                    {error, {json_encoding_error, Error}}
            end;
        halt ->
            {error, halt}
    end.


resubmit(#{jtx := true} = JTx0, #{job := true} = Job, NewSTime) ->
    #{tx := Tx} = JTx = get_jtx(JTx0),
    #{type := Type, id := JobId} = Job,
    Key = job_key(JTx, Job),
    case get_job_val(Tx, Key) of
        #jv{seq = Seq, jlock = JLock, stime = OldSTime, data = Data} = JV ->
            STime = case NewSTime =:= undefined of
                true -> OldSTime;
                false -> NewSTime
            end,
            case job_state(JLock, Seq) of
                finished ->
                    ok = maybe_enqueue(JTx, Type, JobId, STime, true, Data),
                    {ok, Job};
                pending ->
                    JV1 = JV#jv{seq = ?UNSET_VS, stime = STime},
                    set_job_val(Tx, Key, JV1),
                    couch_jobs_pending:remove(JTx, Type, JobId, OldSTime),
                    couch_jobs_pending:enqueue(JTx, Type, STime, JobId),
                    {ok, Job#{stime => STime}};
                running ->
                    JV1 = JV#jv{stime = STime, resubmit = true},
                    set_job_val(Tx, Key, JV1),
                    {ok, Job#{resubmit => true, stime => STime}}
            end;
        not_found ->
            {error, not_found}
    end.


update(#{jtx := true} = JTx0, #{jlock := <<_/binary>>} = Job, Data) when
        is_map(Data) orelse Data =:= undefined ->
    #{tx := Tx} = JTx = get_jtx(JTx0),
    #{jlock := JLock, type := Type, id := JobId} = Job,
    Key = job_key(JTx, Job),
    case get_job_or_halt(Tx, Key, JLock) of
        #jv{seq=Seq, stime = STime, resubmit = Resubmit} = JV0 ->
            update_activity(JTx, Type, JobId, Seq),
            JV = case Data =:= undefined of
                true -> JV0;
                false -> JV0#jv{data = Data}
            end,
            try set_job_val(Tx, Key, JV#jv{seq = ?UNSET_VS}) of
                ok ->
                    {ok, Job#{resubmit => Resubmit, stime => STime}}
            catch
                error:{json_encoding_error, Error} ->
                    {error, {json_encoding_error, Error}}
            end;
        halt ->
            {error, halt}
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
    Key = erlfdb_tuple:pack({?WATCHES_ACTIVITY, Type}, Jobs),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        not_found ->
            not_found;
        Val ->
            {VS} = erlfdb_tuple:unpack(Val),
            VS
    end.


get_activity_vs_and_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES_ACTIVITY, Type}, Jobs),
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
    StartKeySel = erlfdb_key:first_greater_or_equal(StartKey),
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


re_enqueue_inactive(#{jtx := true} = JTx, Type, JobIds) when is_list(JobIds) ->
    #{tx := Tx} = get_jtx(JTx),
    lists:foreach(fun(JobId) ->
        case get_job_val(Tx, job_key(JTx, Type, JobId)) of
            #jv{seq = Seq, stime = STime, data = Data} ->
                clear_activity(JTx, Type, Seq),
                maybe_enqueue(JTx, Type, JobId, STime, true, Data);
            not_found ->
                ok
        end
    end, JobIds),
    case length(JobIds) > 0 of
        true -> update_watch(JTx, Type);
        false -> ok
    end.


% Cache initialization API. Called from the supervisor just to create the ETS
% table. It returns `ignore` to tell supervisor it won't actually start any
% process, which is what we want here.
%
init_cache() ->
    ConcurrencyOpts = [{read_concurrency, true}, {write_concurrency, true}],
    ets:new(?MODULE, [public, named_table] ++ ConcurrencyOpts),
    ignore.


% Functions to encode / decode JobData
%
encode_data(#{} = JobData) ->
    try
        jiffy:encode(JobData)
    catch
        throw:{error, Error} ->
            % legacy clause since new versions of jiffy raise error instead
            error({json_encoding_error, Error});
        error:{error, Error} ->
            error({json_encoding_error, Error})
    end.


decode_data(<<_/binary>> = JobData) ->
    jiffy:decode(JobData, [return_maps]).


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


% Debug and testing API

get_job(Type, JobId) ->
    fabric2_fdb:transactional(fun(Tx) ->
        JTx = init_jtx(Tx),
        case get_job_val(Tx, job_key(JTx, Type, JobId)) of
            #jv{seq = Seq, jlock = JLock} = JV ->
                #{
                    job => true,
                    type => Type,
                    id => JobId,
                    seq => Seq,
                    jlock => JLock,
                    stime => JV#jv.stime,
                    resubmit => JV#jv.resubmit,
                    data => decode_data(JV#jv.data),
                    state => job_state(JLock, Seq)
                };
            not_found ->
                not_found
        end
    end).


get_jobs() ->
    fabric2_fdb:transactional(fun(Tx) ->
        #{jobs_path := Jobs} = init_jtx(Tx),
        Prefix = erlfdb_tuple:pack({?DATA}, Jobs),
        Opts = [{streaming_mode, want_all}],
        Result = erlfdb:wait(erlfdb:get_range_startswith(Tx, Prefix, Opts)),
        lists:map(fun({K, V}) ->
            {Type, JobId} = erlfdb_tuple:unpack(K, Prefix),
            {Seq, JLock, _, _, Data} = erlfdb_tuple:unpack(V),
            JobState = job_state(JLock, Seq),
            {Type, JobId, JobState, decode_data(Data)}
        end, Result)
    end).


clear_jobs() ->
    fabric2_fdb:transactional(fun(Tx) ->
        #{jobs_path := Jobs} = init_jtx(Tx),
        erlfdb:clear_range_startswith(Tx, Jobs)
    end).


% Private helper functions

maybe_enqueue(#{jtx := true} = JTx, Type, JobId, STime, Resubmit, Data) ->
    #{tx := Tx} = JTx,
    Key = job_key(JTx, Type, JobId),
    JV = #jv{
        seq = null,
        jlock = null,
        stime = STime,
        resubmit = false,
        data = Data
    },
    case Resubmit of
        true ->
            set_job_val(Tx, Key, JV#jv{seq = ?UNSET_VS}),
            couch_jobs_pending:enqueue(JTx, Type, STime, JobId);
        false ->
            set_job_val(Tx, Key, JV)
    end,
    ok.


job_key(#{jtx := true, jobs_path := Jobs}, Type, JobId) ->
    erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs).


job_key(JTx, #{type := Type, id := JobId}) ->
    job_key(JTx, Type, JobId).


get_job_val(#{jtx := true, tx := Tx} = JTx, #{job := true} = Job) ->
    get_job_val(Tx, job_key(JTx, Job));

get_job_val(Tx = {erlfdb_transaction, _}, Key) ->
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> = Val ->
            {Seq, JLock, STime, Resubmit, Data} = erlfdb_tuple:unpack(Val),
            #jv{
                seq = Seq,
                jlock = JLock,
                stime = STime,
                resubmit = Resubmit,
                data = Data
            };
        not_found ->
            not_found
    end.


set_job_val(Tx = {erlfdb_transaction, _}, Key, #jv{} = JV) ->
    #jv{
        seq = Seq,
        jlock = JLock,
        stime = STime,
        resubmit = Resubmit,
        data = Data0
    } = JV,
    Data = case Data0 of
        #{} -> encode_data(Data0);
        <<_/binary>> -> Data0
    end,
    case Seq of
        ?UNSET_VS ->
            Val = erlfdb_tuple:pack_vs({Seq, JLock, STime, Resubmit, Data}),
            erlfdb:set_versionstamped_value(Tx, Key, Val);
        _Other ->
            Val = erlfdb_tuple:pack({Seq, JLock, STime, Resubmit, Data}),
            erlfdb:set(Tx, Key, Val)
    end,
    ok.


get_job_or_halt(Tx, Key, JLock) ->
    case get_job_val(Tx, Key) of
        #jv{jlock = CurJLock} when CurJLock =/= JLock ->
            halt;
        #jv{} = Res ->
            Res;
        not_found ->
            halt
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
    Key = erlfdb_tuple:pack({?WATCHES_ACTIVITY, Type}, Jobs),
    Val = erlfdb_tuple:pack_vs({?UNSET_VS}),
    erlfdb:set_versionstamped_value(Tx, Key, Val),
    ok.


job_state(JLock, Seq) ->
    case {JLock, Seq} of
        {null, null} -> finished;
        {JLock, _} when JLock =/= null -> running;
        {null, Seq} when Seq =/= null -> pending
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


ensure_current(#{jtx := true, tx := Tx} = JTx) ->
    case get(?COUCH_JOBS_CURRENT) of
        Tx ->
            JTx;
        _ ->
            JTx1 = update_current(JTx),
            put(?COUCH_JOBS_CURRENT, Tx),
            JTx1
    end.


update_current(#{tx := Tx, md_version := Version} = JTx) ->
  case erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)) of
      Version -> JTx;
      _NewVersion -> update_jtx_cache(init_jtx(Tx))
  end.


update_jtx_cache(#{jtx := true} = JTx) ->
    CachedJTx = JTx#{tx := undefined},
    ets:insert(?MODULE, {?JOBS, CachedJTx}),
    JTx.
