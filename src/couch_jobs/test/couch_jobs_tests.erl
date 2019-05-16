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


-define(DATA, <<"data">>).
-define(RESUBMIT, <<"resubmit">>).
-define(PRIORITY, <<"priority">>).


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
                    fun get_jobs/1,
                    fun resubmit_as_job_creator/1,
                    fun type_timeouts_and_server/1,
                    fun dead_notifier_restarts_jobs_server/1,
                    fun bad_messages_restart_couch_jobs_server/1,
                    fun bad_messages_restart_notifier/1,
                    fun bad_messages_restart_activity_monitor/1,
                    fun worker_accept_and_finish/1,
                    fun worker_update/1,
                    fun resubmit_enqueues_job/1,
                    fun add_custom_priority/1,
                    fun resubmit_custom_priority/1,
                    fun accept_max_priority/1,
                    fun subscribe/1,
                    fun subscribe_callback/1,
                    fun subscribe_errors/1,
                    fun enqueue_inactive/1,
                    fun cancel_running_job/1,
                    fun stop_and_remove_running_job/1,
                    fun clear_type_works/1
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
    T1 = <<"t1">>,
    T2 = 424242,  % a number should work as well
    T1Timeout = 2,
    T2Timeout = 3,
    couch_jobs:set_type_timeout(T1, T1Timeout),
    couch_jobs:set_type_timeout(T2, T2Timeout),
    couch_jobs_server:force_check_types(),
    #{
        t1 => T1,
        t2 => T2,
        t1_timeout => T1Timeout,
        t2_timeout => T2Timeout,
        j1 => <<"j1">>,
        j2 => <<"j2">>,
        j1_data => #{<<"j1_data">> => 42}
    }.


teardown(#{}) ->
    application:stop(couch_jobs),
    couch_jobs_fdb:clear_jobs(),
    meck:unload().


restart_app() ->
    application:stop(couch_jobs),
    application:start(couch_jobs),
    couch_jobs_server:force_check_types().


add_remove_pending(#{t1 := T, j1 := J, j1_data := Data}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(T, J, #{?DATA => Data})),
        ?assertMatch({ok, #{?DATA := Data}, pending},
            couch_jobs:get_job(T, J)),
        ?assertEqual(ok, couch_jobs:remove(T, J)),
        ?assertEqual(ok, couch_jobs:add(T, J, #{?DATA => Data})),
        ?assertMatch({ok, #{?DATA := Data}, pending},
            couch_jobs:get_job(T, J)),
        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


add_remove_errors(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ?assertEqual(not_found, couch_jobs:remove(<<"bad_type">>, <<"1">>)),
        ?assertMatch({error, {invalid_args, _}}, couch_jobs:add(T, J,
            #{1 => 2})),
        ?assertEqual({error, no_type_timeout}, couch_jobs:add(<<"x">>, J,
            #{})),
        ?assertEqual(ok, couch_jobs:add(T, J, #{})),
        ?assertEqual({error, duplicate_job}, couch_jobs:add(T, J, #{})),
        ?assertEqual(ok, couch_jobs:remove(T, J)),
        ?assertEqual(not_found, couch_jobs:stop_and_remove(T, J, 100)),
        ?assertEqual(not_found, couch_jobs:remove(T, J)),
        ?assertMatch({error, {invalid_args, _}}, couch_jobs:add(T, J,
            #{?RESUBMIT => potato})),
        ?assertMatch({error, {invalid_args, _}}, couch_jobs:add(T, J,
            #{?PRIORITY => #{bad_priority => nope}}))

    end).


get_jobs(#{t1 := T1, t2 := T2, j1 := J1, j2 := J2, j1_data := Data}) ->
    ?_test(begin
        ok = couch_jobs:add(T1, J1, #{?DATA => Data}),
        ok = couch_jobs:add(T2, J2, #{}),

        ?assertMatch({ok, #{?DATA := Data}, pending},
            couch_jobs:get_job(T1, J1)),
        ?assertEqual({ok, #{}, pending}, couch_jobs:get_job(T2, J2)),

        ?assertEqual([{J1, pending, #{?DATA => Data}}],
            couch_jobs:get_jobs(T1)),
        ?assertEqual([{J2, pending, #{}}], couch_jobs:get_jobs(T2)),
        ?assertEqual([], couch_jobs:get_jobs(<<"othertype">>)),

        ?assertEqual(lists:sort([
            {T1, J1, pending, #{?DATA => Data}},
            {T2, J2, pending, #{}}
        ]), lists:sort(couch_jobs:get_jobs())),
        ?assertEqual(ok, couch_jobs:remove(T1, J1)),
        ?assertEqual([{T2, J2, pending, #{}}], couch_jobs:get_jobs()),
        ?assertEqual(ok, couch_jobs:remove(T2, J2)),
        ?assertEqual([], couch_jobs:get_jobs())
    end).


resubmit_as_job_creator(#{t1 := T, j1 := J, j1_data := Data}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{?DATA => Data}),

        ?assertEqual(not_found, couch_jobs:resubmit(T, <<"badjob">>)),

        ?assertEqual(ok, couch_jobs:resubmit(T, J)),
        JobOpts1 = #{?DATA => Data, ?RESUBMIT => true},
        ?assertEqual({ok, JobOpts1, pending}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:resubmit(T, J)),
        ?assertEqual({ok, JobOpts1, pending}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:resubmit(T, J, <<"a">>)),
        JobOpts2 = #{?DATA => Data, ?RESUBMIT => true, ?PRIORITY => <<"a">>},
        ?assertEqual({ok, JobOpts2, pending}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:resubmit(T, J, <<"b">>)),
        JobOpts3 = #{?DATA => Data, ?RESUBMIT => true, ?PRIORITY => <<"b">>},
        ?assertEqual({ok, JobOpts3, pending}, couch_jobs:get_job(T, J))
    end).


type_timeouts_and_server(#{t1 := T, t1_timeout := T1Timeout}) ->
    ?_test(begin
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


worker_accept_and_finish(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        AcceptResponse = couch_jobs:accept(T),
        ?assertMatch({ok, J, <<_/binary>>}, AcceptResponse),
        {ok, J, WLock} = AcceptResponse,

        ?assertEqual({ok, #{}, running}, couch_jobs:get_job(T, J)),
        Res = #{<<"result">> => <<"done">>},
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => Res}, WLock)
        end)),
        ?assertEqual({ok, #{?DATA => Res}, finished},
            couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


worker_update(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        AcceptResponse = couch_jobs:accept(T),
        ?assertMatch({ok, J, <<_/binary>>}, AcceptResponse),
        {ok, J, WLock} = AcceptResponse,

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 1}, WLock)
        end)),
        ?assertEqual({ok, #{?DATA => 1}, running}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 2}, WLock)
        end)),
        ?assertEqual({ok, #{?DATA => 2}, running}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 3}, WLock)
        end)),
        ?assertEqual({ok, #{?DATA => 3}, finished}, couch_jobs:get_job(T, J)),

        ?assertEqual(worker_conflict, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 4}, WLock)
        end)),

        ?assertMatch(not_found, couch_jobs:accept(T)),

        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


resubmit_enqueues_job(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        {ok, J, WLock1} = couch_jobs:accept(T),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:resubmit(Tx, T, J, undefined, WLock1)
        end)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 1}, WLock1)
        end)),

        ?assertEqual({ok, #{?DATA => 1}, pending}, couch_jobs:get_job(T, J)),

        {ok, J, WLock2} =  couch_jobs:accept(T),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 2}, WLock2)
        end)),
        ?assertEqual({ok, #{?DATA => 2}, finished}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


add_custom_priority(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(T, J1, #{?PRIORITY => 5})),
        ?assertEqual(ok, couch_jobs:add(T, J2, #{?PRIORITY => 3})),

        ?assertMatch({ok, J2, _}, couch_jobs:accept(T)),
        ?assertMatch({ok, J1, _}, couch_jobs:accept(T)),
        ?assertMatch(not_found, couch_jobs:accept(T))
    end).


resubmit_custom_priority(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ?assertEqual(ok, couch_jobs:add(T, J, #{?PRIORITY => 7})),
        {ok, J, WLock} = couch_jobs:accept(T),
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:resubmit(Tx, T, J, 9, WLock)
        end)),
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 1}, WLock)
        end)),
        ?assertEqual({ok, #{?DATA => 1, ?PRIORITY => 9}, pending},
            couch_jobs:get_job(T, J))
    end).


accept_max_priority(#{t1 := T, j1 := J1, j2 := J2}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J1, #{?PRIORITY => <<"5">>}),
        ok = couch_jobs:add(T, J2, #{?PRIORITY => <<"3">>}),
        ?assertEqual(not_found, couch_jobs:accept(T, <<"2">>)),
        ?assertMatch({ok, J2, _}, couch_jobs:accept(T, <<"3">>)),
        ?assertMatch({ok, J1, _}, couch_jobs:accept(T, <<"9">>))
    end).


subscribe(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        SubRes0 =  couch_jobs:subscribe(T, J),
        ?assertMatch({ok, {_, _}, pending}, SubRes0),
        {ok, SubId0, pending} = SubRes0,

        ?assertEqual(ok, couch_jobs:unsubscribe(SubId0)),

        SubRes =  couch_jobs:subscribe(T, J),
        ?assertMatch({ok, {_, _}, pending}, SubRes),
        {ok, SubId, pending} = SubRes,

        {ok, J, WLock} = couch_jobs:accept(T),
        ?assertMatch(timeout, couch_jobs:wait_job_state(SubId, finished, 50)),
        ?assertMatch({T, J, running}, couch_jobs:wait_job_state(SubId, 5000)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 1}, WLock)
        end)),

        % Make sure we get intermediate `running` updates
        ?assertMatch({T, J, running}, couch_jobs:wait_job_state(SubId, 5000)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{}, WLock)
        end)),
        ?assertMatch({T, J, finished}, couch_jobs:wait_job_state(SubId,
            5000)),

        ?assertEqual(timeout, couch_jobs:wait_job_state(SubId, 50)),
        ?assertEqual(finished, couch_jobs:subscribe(T, J)),

        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


subscribe_callback(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        TestPid = self(),
        SomeRef = make_ref(),
        Cbk = fun(Type, JobId, JobState) ->
            TestPid ! {SomeRef, Type, JobId, JobState}
        end,

        SubRes = couch_jobs:subscribe(T, J, Cbk),
        ?assertMatch({ok, {_, _}, pending}, SubRes),
        {ok, _, pending} = SubRes,

        {ok, J, WLock} = couch_jobs:accept(T),
        receive {SomeRef, T, J, running} -> ok end,

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{}, WLock)
        end)),
        receive {SomeRef, T, J, finished} -> ok end
    end).


subscribe_errors(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        Cbk = fun(_Type, _JobId, _JobState) -> ok end,

        ?assertMatch({error, _}, couch_jobs:subscribe(<<"badtype">>, J)),
        ?assertMatch({error, _}, couch_jobs:subscribe(<<"badtype">>, J, Cbk)),

        ?assertEqual(not_found, couch_jobs:subscribe(T, <<"j5">>)),
        ?assertEqual(not_found, couch_jobs:subscribe(T, <<"j5">>, Cbk)),

        {ok, J, WLock} = couch_jobs:accept(T),
        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{}, WLock)
        end)),

        ?assertEqual(finished, couch_jobs:subscribe(T, J)),
        ?assertEqual(finished, couch_jobs:subscribe(T, J, Cbk))
    end).


enqueue_inactive(#{t1 := T, j1 := J, t1_timeout := Timeout}) ->
    {timeout, 15, ?_test(begin
        ok = couch_jobs:add(T, J, #{}),

        {ok, J, WLock} = couch_jobs:accept(T),

        {ok, SubId, running} = couch_jobs:subscribe(T, J),
        ?assertEqual({T, J, pending}, couch_jobs:wait_job_state(SubId,
            pending, 3 * Timeout * 1000)),

        ?assertMatch({ok, #{}, pending}, couch_jobs:get_job(T, J)),

        ?assertEqual(worker_conflict, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 1}, WLock)
        end)),


        ?assertEqual(worker_conflict, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{}, WLock)
        end)),


        ?assertEqual(worker_conflict, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:resubmit(Tx, T, J, undefined, WLock)
        end))
    end)}.


cancel_running_job(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),
        {ok, J, WLock} = couch_jobs:accept(T),
        ?assertEqual(canceled, couch_jobs:remove(T, J)),
        % Try again and it should be a no-op
        ?assertEqual(canceled, couch_jobs:remove(T, J)),

        ?assertEqual(canceled, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 1}, WLock)
        end)),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 2}, WLock)
        end)),

        ?assertMatch({ok, #{?DATA := 2}, finished}, couch_jobs:get_job(T, J)),

        ?assertEqual(ok, couch_jobs:remove(T, J))
    end).


stop_and_remove_running_job(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),
        {ok, J, WLock} = couch_jobs:accept(T),
        {_, Ref} = spawn_monitor(fun() ->
            exit({result, couch_jobs:stop_and_remove(T, J, 10000)})
        end),

        fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 1}, WLock)
        end),

        fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:update(Tx, T, J, #{?DATA => 2}, WLock)
        end),

        ?assertEqual(ok, fabric2_fdb:transactional(fun(Tx) ->
            couch_jobs:finish(Tx, T, J, #{?DATA => 3}, WLock)
        end)),

        Exit = receive {'DOWN', Ref, _, _, Reason} -> Reason end,
        ?assertEqual({result, ok}, Exit),

        ?assertMatch(not_found, couch_jobs:get_job(T, J))
    end).


clear_type_works(#{t1 := T, j1 := J}) ->
    ?_test(begin
        ok = couch_jobs:add(T, J, #{}),
        ?assertEqual([{J, pending, #{}}], couch_jobs:get_jobs(T)),
        couch_jobs_fdb:clear_type(T),
        ?assertEqual([], couch_jobs:get_jobs(T))
    end).
