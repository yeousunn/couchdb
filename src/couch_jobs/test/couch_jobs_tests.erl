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

-module(couch_jobs_tests).


-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("eunit/include/eunit.hrl").


% Job creation API can take an undefined Tx object
% in that case it will start its own transaction
-define(TX, undefined).


couch_jobs_basic_test_() ->
    {
        "Test couch jobs basics",
        {
            setup,
            fun setup_couch/0, fun teardown_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun add_remove_pending/1,
                    fun add_remove_errors/1,
                    fun get_job_data_and_state/1,
                    fun resubmit_as_job_creator/1,
                    fun type_timeouts_and_server/1,
                    fun dead_notifier_restarts_jobs_server/1,
                    fun bad_messages_restart_couch_jobs_server/1,
                    fun bad_messages_restart_notifier/1,
                    fun bad_messages_restart_activity_monitor/1,
                    fun basic_accept_and_finish/1,
                    fun accept_blocking/1,
                    fun job_processor_update/1,
                    fun resubmit_enqueues_job/1,
                    fun add_with_scheduled_time/1,
                    fun resubmit_custom_schedtime/1,
                    fun accept_max_schedtime/1,
                    fun subscribe/1,
                    fun subscribe_wait_multiple/1,
                    fun enqueue_inactive/1,
                    fun remove_running_job/1,
                    fun check_get_jobs/1,
                    fun use_fabric_transaction_object/1
                ]
            }
        }
    }.


setup_couch() ->
    test_util:start_couch([fabric]).


teardown_couch(Ctx) ->
    test_util:stop_couch(Ctx),
    meck:unload().


setup() ->
    couch_jobs_fdb:clear_jobs(),
    application:start(couch_jobs),
    T1 = {<<"t1">>, 1024}, % a complex type should work
    T2 = 42, % a number should work as well
    T1Timeout = 2,
    T2Timeout = 3,
    couch_jobs:set_type_timeout(T1, T1Timeout),
    couch_jobs:set_type_timeout(T2, T2Timeout),
    #{
        t1 => T1,
        t2 => T2,
        t1_timeout => T1Timeout,
        j1 => <<"j1">>,
        j2 => <<"j2">>,
        dbname => ?tempdb()
    }.


teardown(#{dbname := DbName}) ->
    application:stop(couch_jobs),
    couch_jobs_fdb:clear_jobs(),
    AllDbs = fabric2_db:list_dbs(),
    case lists:member(DbName, AllDbs) of
        true -> ok = fabric2_db:delete(DbName, []);
        false -> ok
    end,
    meck:unload().


restart_app() ->
    application:stop(couch_jobs),
    application:start(couch_jobs),
    couch_jobs_server:force_check_types().


get_job(Type, JobId) ->
    couch_jobs_fdb:get_job(Type, JobId).


add_remove_pending(#{t1 := T1, j1 := J1, t2 := T2, j2 := J2}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(?TX, T1, J1, #{})),
        ?assertMatch(#{state := pending, data := #{}}, get_job(T1, J1)),
        ?assertEqual(ok, couch_jobs:remove(?TX, T1, J1)),
        % Data and numeric type should work as well. Also do it in a
        % transaction
        Data = #{<<"x">> => 42},
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:add(Tx, T2, J2, Data)
        end)),
        ?assertMatch(#{state := pending, data := Data}, get_job(T2, J2)),
        ?assertEqual(ok, couch_jobs:remove(?TX, T2, J2))
    end).


get_job_data_and_state(#{t1 := T, j1 := J}) ->
    ?_test(begin
        Data = #{<<"x">> => 42},
        ok = couch_jobs:add(?TX, T, J, Data),
        ?assertEqual({ok, Data}, couch_jobs:get_job_data(?TX, T, J)),
        ?assertEqual({ok, pending}, couch_jobs:get_job_state(?TX, T, J)),
        ?assertEqual(ok, couch_jobs:remove(?TX, T, J)),
        ?assertEqual({error, not_found}, couch_jobs:get_job_data(?TX, T, J)),
        ?assertEqual({error, not_found}, couch_jobs:get_job_state(?TX, T, J))
    end).


add_remove_errors(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ?assertEqual({error, not_found}, couch_jobs:remove(?TX, 999, 2)),
        ?assertMatch({error, {json_encoding_error, _}}, couch_jobs:add(?TX, T,
            J, #{1 => 2})),
        ?assertEqual({error, no_type_timeout}, couch_jobs:add(?TX, <<"x">>, J,
            #{})),
        ?assertEqual(ok, couch_jobs:add(?TX, T, J, #{})),
        ?assertEqual(ok, couch_jobs:add(?TX, T, J, #{})),
        ?assertEqual(ok, couch_jobs:remove(?TX, T, J))
    end).


resubmit_as_job_creator(#{t1 := T, j1 := J}) ->
    ?_test(begin
        Data = #{<<"x">> => 42},
        ok = couch_jobs:add(?TX, T, J, Data, 15),

        % Job was pending, doesn't get resubmitted
        ok = couch_jobs:add(?TX, T, J, Data, 16),
        ?assertMatch(#{state := pending, stime := 16}, get_job(T, J)),

        {ok, Job1, Data} = couch_jobs:accept(T),

        % If is running, it gets flagged to be resubmitted
        ok = couch_jobs:add(?TX, T, J, Data, 17),
        ?assertMatch(#{state := running, stime := 17}, get_job(T, J)),
        ?assertEqual(true, couch_jobs:is_resubmitted(get_job(T, J))),

        ?assertEqual(ok, couch_jobs:finish(?TX, Job1)),
        % It should be pending according to the resubmit flag
        ?assertMatch(#{state := pending, stime := 17}, get_job(T, J)),

        % A finished job will be re-enqueued
        {ok, Job2, _} = couch_jobs:accept(T),
        ?assertEqual(ok, couch_jobs:finish(?TX, Job2)),
        ?assertMatch(#{state := finished, stime := 17}, get_job(T, J)),
        ok = couch_jobs:add(?TX, T, J, Data, 18),
        ?assertMatch(#{state := pending, stime := 18}, get_job(T, J))
    end).


type_timeouts_and_server(#{t1 := T, t1_timeout := T1Timeout}) ->
    ?_test(begin
        couch_jobs_server:force_check_types(),

        ?assertEqual(T1Timeout, couch_jobs:get_type_timeout(T)),

        ?assertEqual(2,
            length(couch_jobs_activity_monitor_sup:get_child_pids())),
        ?assertEqual(2, length(couch_jobs_notifier_sup:get_child_pids())),
        ?assertMatch({ok, _}, couch_jobs_server:get_notifier_server(T)),

        ?assertEqual(ok, couch_jobs:set_type_timeout(<<"t3">>, 8)),
        couch_jobs_server:force_check_types(),
        ?assertEqual(3,
            length(couch_jobs_activity_monitor_sup:get_child_pids())),
        ?assertEqual(3, length(couch_jobs_notifier_sup:get_child_pids())),

        ?assertEqual(ok, couch_jobs:clear_type_timeout(<<"t3">>)),
        couch_jobs_server:force_check_types(),
        ?assertEqual(2,
            length(couch_jobs_activity_monitor_sup:get_child_pids())),
        ?assertEqual(2,
            length(couch_jobs_notifier_sup:get_child_pids())),
        ?assertMatch({error, _},
            couch_jobs_server:get_notifier_server(<<"t3">>)),

        ?assertEqual(not_found, couch_jobs:get_type_timeout(<<"t3">>))
    end).


dead_notifier_restarts_jobs_server(#{}) ->
    ?_test(begin
        couch_jobs_server:force_check_types(),

        ServerPid = whereis(couch_jobs_server),
        Ref = monitor(process, ServerPid),

        [Notifier1, _Notifier2] = couch_jobs_notifier_sup:get_child_pids(),
        exit(Notifier1, kill),

        % Killing a notifier should kill the server as well
        receive {'DOWN', Ref, _, _, _} -> ok end
    end).


bad_messages_restart_couch_jobs_server(#{}) ->
    ?_test(begin
        % couch_jobs_server dies on bad cast
        ServerPid1 = whereis(couch_jobs_server),
        Ref1 = monitor(process, ServerPid1),
        gen_server:cast(ServerPid1, bad_cast),
        receive {'DOWN', Ref1, _, _, _} -> ok end,

        restart_app(),

        % couch_jobs_server dies on bad call
        ServerPid2 = whereis(couch_jobs_server),
        Ref2 = monitor(process, ServerPid2),
        catch gen_server:call(ServerPid2, bad_call),
        receive {'DOWN', Ref2, _, _, _} -> ok end,

        restart_app(),

        % couch_jobs_server dies on bad info
        ServerPid3 = whereis(couch_jobs_server),
        Ref3 = monitor(process, ServerPid3),
        ServerPid3 ! a_random_message,
        receive {'DOWN', Ref3, _, _, _} -> ok end,

        restart_app()
    end).


bad_messages_restart_notifier(#{}) ->
    ?_test(begin
        couch_jobs_server:force_check_types(),

        % bad cast kills the activity monitor
        [AMon1, _] = couch_jobs_notifier_sup:get_child_pids(),
        Ref1 = monitor(process, AMon1),
        gen_server:cast(AMon1, bad_cast),
        receive {'DOWN', Ref1, _, _, _} -> ok end,

        restart_app(),

        % bad calls restart activity monitor
        [AMon2, _] = couch_jobs_notifier_sup:get_child_pids(),
        Ref2 = monitor(process, AMon2),
        catch gen_server:call(AMon2, bad_call),
        receive {'DOWN', Ref2, _, _, _} -> ok end,

        restart_app(),

        % bad info message kills activity monitor
        [AMon3, _] = couch_jobs_notifier_sup:get_child_pids(),
        Ref3 = monitor(process, AMon3),
        AMon3 ! a_bad_message,
        receive {'DOWN', Ref3, _, _, _} -> ok end,


        restart_app()
    end).


bad_messages_restart_activity_monitor(#{}) ->
    ?_test(begin
        couch_jobs_server:force_check_types(),

        % bad cast kills the activity monitor
        [AMon1, _] = couch_jobs_activity_monitor_sup:get_child_pids(),
        Ref1 = monitor(process, AMon1),
        gen_server:cast(AMon1, bad_cast),
        receive {'DOWN', Ref1, _, _, _} -> ok end,

        restart_app(),

        % bad calls restart activity monitor
        [AMon2, _] = couch_jobs_activity_monitor_sup:get_child_pids(),
        Ref2 = monitor(process, AMon2),
        catch gen_server:call(AMon2, bad_call),
        receive {'DOWN', Ref2, _, _, _} -> ok end,

        restart_app(),

        % bad info message kills activity monitor
        [AMon3, _] = couch_jobs_activity_monitor_sup:get_child_pids(),
        Ref3 = monitor(process, AMon3),
        AMon3 ! a_bad_message,
        receive {'DOWN', Ref3, _, _, _} -> ok end,

        restart_app()
    end).


basic_accept_and_finish(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J, #{}),
        {ok, Job, #{}} = couch_jobs:accept(T),
        ?assertMatch(#{state := running}, get_job(T, J)),
        % check json validation for bad data in finish
        ?assertMatch({error, {json_encoding_error, _}},
            fabric2_fdb:transactional(fun(Tx) ->
                couch_jobs:finish(Tx, Job, #{1 => 1})
            end)),
        Data = #{<<"x">> => 42},
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, Job, Data)
        end)),
        ?assertMatch(#{state := finished, data := Data}, get_job(T, J))
    end).


accept_blocking(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        Accept = fun() -> exit(couch_jobs:accept(T)) end,
        WaitAccept = fun(Ref) ->
            receive
                {'DOWN', Ref, _, _, Res} -> Res
            after
                500 -> timeout
            end
        end,
        {_, Ref1} = spawn_monitor(Accept),
        ok = couch_jobs:add(?TX, T, J1, #{}),
        ?assertMatch({ok, #{id := J1}, #{}}, WaitAccept(Ref1)),
        {_, Ref2} = spawn_monitor(Accept),
        ?assertEqual(timeout, WaitAccept(Ref2)),
        ok = couch_jobs:add(?TX, T, J2, #{}),
        ?assertMatch({ok, #{id := J2}, #{}}, WaitAccept(Ref2))
    end).


job_processor_update(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J, #{}),
        {ok, Job, #{}} = couch_jobs:accept(T),

        % Use proper transactions in a few places here instead of passing in ?TX
        % This is mostly to increase code coverage

        ?assertMatch({ok, #{job := true}}, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, Job, #{<<"x">> => 1})
        end)),

        ?assertMatch(#{data := #{<<"x">> := 1}, state := running},
            get_job(T, J)),

        ?assertMatch({ok, #{job := true}}, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, Job)
        end)),

        ?assertMatch(#{data := #{<<"x">> := 1}, state := running},
            get_job(T, J)),

        ?assertMatch({ok, #{job := true}}, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, Job, #{<<"x">> => 2})
        end)),

        % check json validation for bad data in update
        ?assertMatch({error, {json_encoding_error, _}},
            fabric2_fdb:transactional(fun(Tx) ->
                couch_jobs:update(Tx, Job, #{1 => 1})
            end)),

        ?assertMatch(#{data := #{<<"x">> := 2}, state := running},
            get_job(T, J)),

        % Finish may update the data as well
        ?assertEqual(ok, couch_jobs:finish(?TX, Job, #{<<"x">> => 3})),
        ?assertMatch(#{data := #{<<"x">> := 3}, state := finished},
            get_job(T, J))
    end).


resubmit_enqueues_job(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J, #{}),
        {ok, Job1, #{}} = couch_jobs:accept(T),
        ?assertMatch({ok, _}, couch_jobs:resubmit(?TX, Job1, 6)),
        ?assertEqual(ok, couch_jobs:finish(?TX, Job1)),
        ?assertMatch(#{state := pending, stime := 6}, get_job(T, J)),
        {ok, Job2, #{}} = couch_jobs:accept(T),
        ?assertEqual(ok, couch_jobs:finish(?TX, Job2)),
        ?assertMatch(#{state := finished}, get_job(T, J))
    end).


add_with_scheduled_time(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(?TX, T, J1, #{}, 5)),
        ?assertEqual(ok, couch_jobs:add(?TX, T, J2, #{}, 3)),
        ?assertMatch({ok, #{id := J}, _} when J =:= J1 orelse J =:= J2,
            couch_jobs:accept(T, 3)),
        ?assertMatch({ok, #{id := J}, _} when J =:= J1 orelse J =:= J2,
            couch_jobs:accept(T, 9))
    end).


resubmit_custom_schedtime(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(?TX, T, J, #{}, 7)),
        {ok, Job, #{}} = couch_jobs:accept(T),
        ?assertMatch({ok, _}, couch_jobs:resubmit(?TX, Job, 9)),
        ?assertEqual(ok, couch_jobs:finish(?TX, Job)),
        ?assertMatch(#{stime := 9, state := pending}, get_job(T, J))
    end).


accept_max_schedtime(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J1, #{}, 5),
        ok = couch_jobs:add(?TX, T, J2, #{}, 3),
        ?assertMatch({ok, #{id := J2}, _}, couch_jobs:accept(T, 3)),
        ?assertMatch({ok, #{id := J1}, _}, couch_jobs:accept(T, 9))
    end).


subscribe(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J, #{}),

        ?assertEqual({error, not_found}, couch_jobs:subscribe(<<"xyz">>, J)),
        ?assertEqual({error, not_found}, couch_jobs:subscribe(T, <<"j5">>)),

        SubRes0 =  couch_jobs:subscribe(T, J),
        ?assertMatch({ok, {_, _}, pending}, SubRes0),
        {ok, SubId0, pending} = SubRes0,

        ?assertEqual(ok, couch_jobs:unsubscribe(SubId0)),

        SubRes =  couch_jobs:subscribe(T, J),
        ?assertMatch({ok, {_, _}, pending}, SubRes),
        {ok, SubId, pending} = SubRes,

        {ok, Job, _} = couch_jobs:accept(T),
        ?assertMatch({T, J, running}, couch_jobs:wait(SubId, 5000)),
        ?assertMatch({ok, _}, couch_jobs:update(?TX, Job)),
        % Make sure we get intermediate `running` updates
        ?assertMatch({T, J, running}, couch_jobs:wait(SubId, 5000)),

        ?assertEqual(ok, couch_jobs:finish(?TX, Job)),
        ?assertMatch({T, J, finished}, couch_jobs:wait(SubId, finished, 5000)),

        ?assertEqual(timeout, couch_jobs:wait(SubId, 50)),
        ?assertEqual({ok, finished}, couch_jobs:subscribe(T, J)),

        ?assertEqual(ok, couch_jobs:remove(?TX, T, J)),
        ?assertEqual({error, not_found}, couch_jobs:subscribe(T, J))
    end).


subscribe_wait_multiple(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J1, #{}),
        ok = couch_jobs:add(?TX, T, J2, #{}),

        {ok, S1, pending} = couch_jobs:subscribe(T, J1),
        {ok, S2, pending} = couch_jobs:subscribe(T, J2),

        Subs = [S1, S2],

        % Accept one job. Only one running update is expected. PJob1 and PJob2
        % do not necessarily correspond got Job1 and Job2, they could be
        % accepted as Job2 and Job1 respectively.
        {ok, PJob1, _} = couch_jobs:accept(T),
        ?assertMatch({_, _, running}, couch_jobs:wait(Subs, 5000)),
        ?assertMatch(timeout, couch_jobs:wait(Subs, 50)),

        % Accept another job. Expect another update.
        {ok, PJob2, _} = couch_jobs:accept(T),
        ?assertMatch({_, _, running}, couch_jobs:wait(Subs, 5000)),
        ?assertMatch(timeout, couch_jobs:wait(Subs, 50)),

        ?assertMatch({ok, _}, couch_jobs:update(?TX, PJob1)),
        ?assertMatch({ok, _}, couch_jobs:update(?TX, PJob2)),

        % Each job was updated once, expect two running updates.
        ?assertMatch({_, _, running}, couch_jobs:wait(Subs, 5000)),
        ?assertMatch({_, _, running}, couch_jobs:wait(Subs, 5000)),

        % Finish one job. Expect one finished update only.
        ?assertEqual(ok, couch_jobs:finish(?TX, PJob1)),

        ?assertMatch({_, _, finished}, couch_jobs:wait(Subs, finished, 5000)),
        ?assertMatch(timeout, couch_jobs:wait(Subs, finished, 50)),

        % Finish another job. However, unsubscribe should flush the
        % the message and we should not get it.
        ?assertEqual(ok, couch_jobs:finish(?TX, PJob2)),
        ?assertEqual(ok, couch_jobs:unsubscribe(S1)),
        ?assertEqual(ok, couch_jobs:unsubscribe(S2)),
        ?assertMatch(timeout, couch_jobs:wait(Subs, finished, 50))
    end).


enqueue_inactive(#{t1 := T, j1 := J, t1_timeout := Timeout}) ->
    {timeout, 10, ?_test(begin
        couch_jobs_server:force_check_types(),

        ok  = couch_jobs:add(?TX, T, J, #{}),
        {ok, Job, _} = couch_jobs:accept(T),

        {ok, SubId, running} = couch_jobs:subscribe(T, J),
        Wait = 3 * Timeout * 1000,
        ?assertEqual({T, J, pending}, couch_jobs:wait(SubId, pending, Wait)),
        ?assertMatch(#{state := pending}, get_job(T, J)),

        % After job was re-enqueued, old job processor can't update it anymore
        ?assertEqual({error, halt}, couch_jobs:update(?TX, Job)),
        ?assertEqual({error, halt}, couch_jobs:finish(?TX, Job))
    end)}.


remove_running_job(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T, J, #{}),
        {ok, Job, _} = couch_jobs:accept(T),
        ?assertEqual(ok, couch_jobs:remove(?TX, T, J)),
        ?assertEqual({error, not_found}, couch_jobs:remove(?TX, T, J)),
        ?assertEqual({error, halt}, couch_jobs:update(?TX, Job)),
        ?assertEqual({error, halt}, couch_jobs:finish(?TX, Job))
    end).


check_get_jobs(#{t1 := T1, j1 := J1, t2 := T2, j2 := J2}) ->
    ?_test(begin
        ok = couch_jobs:add(?TX, T1, J1, #{}),
        ok = couch_jobs:add(?TX, T2, J2, #{}),
        ?assertMatch([
            {T2, J2, pending, #{}},
            {T1, J1, pending, #{}}
        ], lists:sort(couch_jobs_fdb:get_jobs())),
        {ok, _, _} = couch_jobs:accept(T1),
        ?assertMatch([
            {T2, J2, pending, #{}},
            {T1, J1, running, #{}}
        ], lists:sort(couch_jobs_fdb:get_jobs()))
    end).


use_fabric_transaction_object(#{t1 := T1, j1 := J1, dbname := DbName}) ->
    ?_test(begin
        {ok, Db} = fabric2_db:create(DbName, []),
        ?assertEqual(ok, couch_jobs:add(Db, T1, J1, #{})),
        ?assertMatch(#{state := pending, data := #{}}, get_job(T1, J1)),
        {ok, Job, _} = couch_jobs:accept(T1),
        ?assertEqual(ok, fabric2_fdb:transactional(Db, fun(Db1) ->
            {ok, #{}} = couch_jobs:get_job_data(Db1, T1, J1),
            Doc1 = #doc{id = <<"1">>, body = {[]}},
            {ok, {_, _}} = fabric2_db:update_doc(Db1, Doc1),
            Doc2 = #doc{id = <<"2">>, body = {[]}},
            {ok, {_, _}} = fabric2_db:update_doc(Db1, Doc2),
            couch_jobs:finish(Db1, Job, #{<<"d">> => 1})
        end)),
        ok = couch_jobs:remove(#{tx => undefined}, T1, J1),
        ok = fabric2_db:delete(DbName, [])
    end).
