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

-module(couch_db_purge_upgrade_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(USER, "couch_db_purge_upgrade_admin").
-define(PASS, "pass").
-define(AUTH, {basic_auth, {?USER, ?PASS}}).
-define(CONTENT_JSON, {"Content-Type", "application/json"}).


setup() ->
    DbDir = config:get("couchdb", "database_dir"),
    DbFileNames = [
        "db_without_purge_req",
        "db_with_1_purge_req",
        "db_with_2_purge_req"
    ],
    lists:map(fun(DbFileName) ->
        write_db_doc(list_to_binary(DbFileName)),
        OldDbFilePath = filename:join([?FIXTURESDIR, DbFileName ++ ".couch"]),
        NewDbFileName = DbFileName ++ ".1525663363.couch",
        NewDbFilePath = filename:join(
            [DbDir, "shards/00000000-ffffffff/", NewDbFileName]
        ),
        ok = filelib:ensure_dir(NewDbFilePath),
        file:delete(NewDbFilePath),
        file:copy(OldDbFilePath, NewDbFilePath),
        NewDbFilePath
    end, DbFileNames).


teardown(Files) ->
    lists:foreach(fun(File) -> file:delete(File) end, Files).


purge_upgrade_test_() ->
    {
        "Purge Upgrade tests",
        {
            setup,
            fun chttpd_test_util:start_couch/0,
            fun chttpd_test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun should_upgrade_legacy_db_without_purge_req/1,
                    fun should_upgrade_legacy_db_with_1_purge_req/1,
                    fun should_upgrade_legacy_db_with_N_purge_req/1
                ]
            }
        }
    }.


should_upgrade_legacy_db_without_purge_req(_Files) ->
    ?_test(begin
        config:set("cluster", "q", "1"),
        DbName = <<"db_without_purge_req">>,
        DbUrl = db_url(DbName),

        % 3 docs in legacy database before upgrade
        % and added 2 new doc to database
        {ok, _, _, ResultBody1} = create_doc(DbUrl, "doc4"),
        {ok, _, _, _ResultBody} = create_doc(DbUrl, "doc5"),
        {Json1} = ?JSON_DECODE(ResultBody1),
        {ok, _, _, ResultBody2} = test_request:get(DbUrl),
        {Json2} = ?JSON_DECODE(ResultBody2),
        Rev4 = couch_util:get_value(<<"rev">>, Json1, undefined),
        IdsRevsEJson = {[{<<"doc4">>, [Rev4]}]},
        ?assert(5 =:= couch_util:get_value(<<"doc_count">>, Json2)),

        IdsRevs = binary_to_list(?JSON_ENCODE(IdsRevsEJson)),
        {ok, Code, _, _ResultBody3} = test_request:post(DbUrl ++ "/_purge/",
            [?CONTENT_JSON], IdsRevs),
        ?assert(Code =:= 201),

        {ok, _, _, ResultBody4} = test_request:get(DbUrl),
        {Json4} = ?JSON_DECODE(ResultBody4),
        ?assert(4 =:= couch_util:get_value(<<"doc_count">>, Json4))
    end).


should_upgrade_legacy_db_with_1_purge_req(_Files) ->
    ?_test(begin
        config:set("cluster", "q", "1"),
        DbName = <<"db_with_1_purge_req">>,
        DbUrl = db_url(DbName),

        % 3 docs in legacy database and 1 of them were purged before upgrade
        % and adding 2 new docs to database
        {ok, _, _, ResultBody1} = create_doc(DbUrl, "doc4"),
        {ok, _, _, _ResultBody} = create_doc(DbUrl, "doc5"),
        {Json1} = ?JSON_DECODE(ResultBody1),
        {ok, _, _, ResultBody2} = test_request:get(DbUrl),
        {Json2} = ?JSON_DECODE(ResultBody2),
        Rev4 = couch_util:get_value(<<"rev">>, Json1, undefined),
        IdsRevsEJson = {[{<<"doc4">>, [Rev4]}]},
        ?assert(4 =:= couch_util:get_value(<<"doc_count">>, Json2)),

        IdsRevs = binary_to_list(?JSON_ENCODE(IdsRevsEJson)),
        {ok, Code, _, _ResultBody3} = test_request:post(DbUrl ++ "/_purge/",
            [?CONTENT_JSON], IdsRevs),
        ?assert(Code =:= 201),

        {ok, _, _, ResultBody4} = test_request:get(DbUrl),
        {Json4} = ?JSON_DECODE(ResultBody4),
        ?assert(3 =:= couch_util:get_value(<<"doc_count">>, Json4)),
        PurgeSeq = couch_util:get_value(<<"purge_seq">>, Json4),
        [SeqNumber | _Rest] = binary:split(PurgeSeq, <<"-">>, [global]),
        ?assert(<<"2">> =:= SeqNumber)
    end).


should_upgrade_legacy_db_with_N_purge_req(_Files) ->
    ?_test(begin
        config:set("cluster", "q", "1"),
        DbName = <<"db_with_2_purge_req">>,
        DbUrl = db_url(DbName),

        % 3 docs in legacy database and 2 of them were purged before upgrade
        % and adding 2 new doc to database
        {ok, _, _, ResultBody1} = create_doc(DbUrl, "doc4"),
        {ok, _, _, _ResultBody} = create_doc(DbUrl, "doc5"),
        {Json1} = ?JSON_DECODE(ResultBody1),
        {ok, _, _, ResultBody2} = test_request:get(DbUrl),
        {Json2} = ?JSON_DECODE(ResultBody2),
        Rev4 = couch_util:get_value(<<"rev">>, Json1, undefined),
        IdsRevsEJson = {[{<<"doc4">>, [Rev4]}]},
        ?assert(3 =:= couch_util:get_value(<<"doc_count">>, Json2)),

        IdsRevs = binary_to_list(?JSON_ENCODE(IdsRevsEJson)),
        {ok, Code, _, _ResultBody3} = test_request:post(DbUrl ++ "/_purge/",
            [?CONTENT_JSON], IdsRevs),
        ?assert(Code =:= 201),

        {ok, _, _, ResultBody4} = test_request:get(DbUrl),
        {Json4} = ?JSON_DECODE(ResultBody4),
        ?assert(2 =:= couch_util:get_value(<<"doc_count">>, Json4)),
        PurgeSeq = couch_util:get_value(<<"purge_seq">>, Json4),
        [SeqNumber | _Rest] = binary:split(PurgeSeq, <<"-">>, [global]),
        ?assert(<<"3">> =:= SeqNumber)
    end).


db_url(DbName) ->
    Addr = config:get("httpd", "bind_address", "127.0.0.1"),
    Port = integer_to_list(mochiweb_socket_server:get(chttpd, port)),
    "http://" ++ Addr ++ ":" ++ Port ++ "/" ++ ?b2l(DbName).

create_doc(Url, Id) ->
    test_request:put(Url ++ "/" ++ Id,
        [?CONTENT_JSON], "{\"mr\": \"rockoartischocko\"}").

write_db_doc(Id) ->
    DbName = ?l2b(config:get("mem3", "shards_db", "_dbs")),
    Doc = couch_doc:from_json_obj({[
        {<<"_id">>, Id},
        {<<"shard_suffix">>, ".1525663363"},
        {<<"changelog">>,
            [[<<"add">>, <<"00000000-ffffffff">>, <<"nonode@nohost">>]]
        },
        {<<"by_node">>, {[{<<"nonode@nohost">>, [<<"00000000-ffffffff">>]}]}},
        {<<"by_range">>, {[{<<"00000000-ffffffff">>, [<<"nonode@nohost">>]}]}}
    ]}),
    write_db_doc(DbName, Doc, true).

write_db_doc(DbName, #doc{id=Id, body=Body} = Doc, ShouldMutate) ->
    {ok, Db} = couch_db:open(DbName, [?ADMIN_CTX]),
    try couch_db:open_doc(Db, Id, [ejson_body]) of
        {ok, #doc{body = Body}} ->
            % the doc is already in the desired state, we're done here
            ok;
        {not_found, _} when ShouldMutate ->
            try couch_db:update_doc(Db, Doc, []) of
                {ok, _} ->
                    ok
            catch conflict ->
                % check to see if this was a replication race or a different edit
                write_db_doc(DbName, Doc, false)
            end;
        _ ->
            % the doc already exists in a different state
            conflict
    after
        couch_db:close(Db)
    end.
