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

-module(fabric2_db).


-export([
    open/2,
    init/3,

    name/1,

    with_tx/2,

    pack/2,
    unpack/2,
    range_bounds/2,

    get/2,
    get_range/3,
    get_range/4,
    get_range_startswith/2,
    get_range_startswith/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric.hrl").


open(DbName, Options) when is_binary(DbName), is_list(Options) ->
    BaseDb = #{
        name => DbName,
        tx => undefined,
        dir => undefined,
        user_ctx => #user_ctx{},
        validate_doc_update => undefined
    },
    lists:foldl(fun({K, V}, DbAcc) ->
        maps:put(K, V, DbAcc)
    end, BaseDb, Options).


name(#{name := Name}) ->
    Name.


init(DbName, Tx, DbDir) ->
    TxDb = open(DbName, [{tx, Tx}, {dir, DbDir}]),
    Defaults = [
        {{<<"meta">>, <<"config">>, <<"revs_limit">>}, ?uint2bin(1000)},
        {{<<"meta">>, <<"config">>, <<"security_doc">>}, <<"{}">>},
        {{<<"meta">>, <<"stats">>, <<"doc_count">>}, ?uint2bin(0)},
        {{<<"meta">>, <<"stats">>, <<"doc_del_count">>}, ?uint2bin(0)},
        {{<<"meta">>, <<"stats">>, <<"doc_design_count">>}, ?uint2bin(0)},
        {{<<"meta">>, <<"stats">>, <<"doc_local_count">>}, ?uint2bin(0)},
        {{<<"meta">>, <<"stats">>, <<"size">>}, ?uint2bin(2)}
    ],
    lists:foreach(fun({K, V}) ->
        erlfdb:set(Tx, pack(TxDb, K), V)
    end, Defaults).


with_tx(Db, Fun) when is_function(Fun, 1) ->
    DbName = maps:get(name, Db),
    DbsDir = fabric_server:get_dir(dbs),
    fabric_server:transactional(fun(Tx) ->
        % We'll eventually want to cache this in the
        % fabric_server ets table.
        DbDir = try
            erlfdb_directory:open(Tx, DbsDir, DbName)
        catch error:{erlfdb_directory, {open_error, path_missing, _}} ->
            erlang:error(database_does_not_exist)
        end,
        TxDb = Db#{tx := Tx, dir := DbDir},
        Fun(TxDb)
    end).


pack(#{dir := undefined} = Db, _Tuple) ->
    erlang:error({no_directory, Db});

pack(Db, Tuple) ->
    #{
        dir := Dir
    } = Db,
    erlfdb_directory:pack(Dir, Tuple).


unpack(#{dir := undefined} = Db, _Key) ->
    erlang:error({no_directory, Db});

unpack(Db, Key) ->
    #{
        dir := Dir
    } = Db,
    erlfdb_directory:unpack(Dir, Key).


range_bounds(#{dir := undefined} = Db, _Key) ->
    erlang:error({no_directory, Db});

range_bounds(Db, Key) ->
    #{
        dir := Dir
    } = Db,
    erlfdb_directory:range(Dir, Key).



get(#{tx := undefined} = Db, _Key) ->
    erlang:error({invalid_tx_db, Db});

get(Db, Key) ->
    #{
        tx := Tx,
        dir := Dir
    } = Db,
    BinKey = erlfdb_directory:pack(Dir, Key),
    erlfdb:get(Tx, BinKey).


get_range(Db, StartKey, EndKey) ->
    get_range(Db, StartKey, EndKey, []).


get_range(#{tx := undefined} = Db, _, _, _) ->
    erlang:error({invalid_tx_db, Db});

get_range(Db, StartKey, EndKey, Options) ->
    #{
        tx := Tx,
        dir := Dir
    } = Db,
    BinStartKey = erlfdb_directory:pack(Dir, StartKey),
    BinEndKey= erlfdb_directory:pack(Dir, EndKey),
    erlfdb:get_range(Tx, BinStartKey, BinEndKey, Options).


get_range_startswith(Db, Prefix) ->
    get_range_startswith(Db, Prefix, []).


get_range_startswith(#{tx := undefined} = Db, _, _) ->
    erlang:error({invalid_tx_db, Db});

get_range_startswith(Db, Prefix, Options) ->
    #{
        tx := Tx,
        dir := Dir
    } = Db,
    BinPrefix = erlfdb_directory:pack(Dir, Prefix),
    erlfdb:get_range_startswith(Tx, BinPrefix, Options).