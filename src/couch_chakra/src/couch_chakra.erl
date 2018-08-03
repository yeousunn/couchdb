% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
%
% You may obtain a copy of the License at
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
% either express or implied.
%
% See the License for the specific language governing permissions
% and limitations under the License.

-module(couch_chakra).
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/0,
    set_timeout/2,
    prompt/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include_lib("couch/include/couch_db.hrl").


-record(st, {
    runtime,
    ddocs = [],
    conv_ctx,
    map_ctx,
    query_config = [],
    timeout = 5000,
    idle
}).


-define(DEF_MEM_LIMIT, 52428800). % 50 MiB


start_link() ->
    gen_server:start_link(?MODULE, [], []).


set_timeout(Pid, TimeOut) ->
    gen_server:call(Pid, {set_timeout, TimeOut}).


prompt(Pid, Data) when is_list(Data) ->
    gen_server:call(Pid, {prompt, Data}).


init([]) ->
    MemLimit = config:get_integer("chakra", "memory_limit", ?DEF_MEM_LIMIT),
    V = config:get_integer("query_server_config", "os_process_idle_limit", 300),
    Idle = V * 1000,
    {ok, Rt} = chakra:create_runtime([
        {memory_limit, MemLimit},
        disable_background_work,
        allow_script_interrupt,
        disable_native_code_generation
    ]),
    {ok, Ctx} = chakra:create_context(Rt),
    init_conv_context(Ctx),
    St = #st{
        runtime = Rt,
        conv_ctx = Ctx,
        idle = Idle
    },
    {ok, St, St#st.idle}.


terminate(_Reason, _State) ->
    ok.


handle_call({set_timeout, TimeOut}, _From, St) ->
    {reply, ok, St#st{timeout = TimeOut}, St#st.idle};

handle_call({prompt, Data}, _From, St) ->
    {Resp, NewSt} = try
        run(Data, St)
    catch
        throw:{error, Why} ->
            {St, [<<"error">>, Why, Why]}
    end,

    case Resp of
        {error, Reason} ->
            Msg = io_lib:format("couch chakra error: ~p", [Reason]),
            MsgBin = iolist_to_binary(Msg),
            Error = [<<"error">>, <<"chakra_query_server">>, MsgBin],
            {reply, Error, NewSt, St#st.idle};
        [<<"error">> | Rest] ->
            {reply, [<<"error">> | Rest], NewSt, St#st.idle};
        [<<"fatal">> | Rest] ->
            {stop, fatal, [<<"error">> | Rest], NewSt};
        _ ->
            {reply, Resp, NewSt, St#st.idle}
    end.


handle_cast(garbage_collect, St) ->
    ok = chakra:gc(St#st.runtime),
    erlang:garbage_collect(),
    {noreply, St, St#st.idle};

handle_cast(stop, St) ->
    {stop, normal, St};

handle_cast(_Msg, St) ->
    {noreply, St, St#st.idle}.


handle_info(timeout, St) ->
    gen_server:cast(couch_proc_manager, {os_proc_idle, self()}),
    ok = chakra:gc(St#st.runtime),
    erlang:garbage_collect(),
    {noreply, St, St#st.idle};

handle_info({'EXIT', _, Reason}, St) ->
    {stop, Reason, St}.


code_change(_OldVersion, State, _Extra) ->
    {ok, State}.


run([<<"reset">>], St) ->
    {ok, Ctx} = create_map_context(St),
    {true, St#st{map_ctx = Ctx}};

run([<<"reset">>, QueryConfig], St) ->
    {ok, Ctx} = create_map_context(St),
    {true, St#st{map_ctx = Ctx, query_config = QueryConfig}};

run([<<"add_lib">>, Lib], St) ->
    {ok, _} = chakra:call(St#st.map_ctx, add_lib, [Lib]),
    {true, St};

run([<<"add_fun">>, BinFun], St) ->
    Converted = convert_function(St, BinFun),
    {ok, _} = chakra:call(St#st.map_ctx, add_fun, [Converted]),
    {true, St};

run([<<"map_doc">>, Doc], St) ->
    {ok, [Logs, Results]} = chakra:call(St#st.map_ctx, map_doc, [Doc]),
    lists:foreach(fun(Log) ->
        couch_log:info("couch_chakra ~p :: ~s", [self(), Log])
    end, Logs),
    {[true, Results], St};

run([<<"reduce">>, Funs, KVs], St) ->
    run_reduce(reduce, Funs, KVs, St);

run([<<"rereduce">>, Funs, KVs], St) ->
    run_reduce(rereduce, Funs, KVs, St);

run([<<"ddoc">>, <<"new">>, DDocId, DDoc], St) ->
    #st{
        ddocs = DDocs
    } = St,
    {ok, Ctx} = create_ddoc_context(St, DDoc),
    NewDDocs = [{DDocId, Ctx} | DDocs],
    {true, St#st{ddocs = NewDDocs}};

run([<<"ddoc">>, DDocId | Rest], St) ->
    #st{
        ddocs = DDocs
    } = St,
    {DDocId, DDocCtx} = lists:keyfind(DDocId, 1, DDocs),
    {ok, Source} = chakra:call(DDocCtx, get_source, [DDocId | Rest]),
    io:format(standard_error, "~s~n", [Source]),
    Resp = convert_function(St, Source),
    io:format(standard_error, "~p~n", [Resp]),
    {ok, Converted} = Resp,
    io:format(standard_error, "~s~n", [Converted]),
    {ok, [Logs, Resp]} = chakra:call(DDocCtx, run, [DDocId | Rest]),
    lists:foreach(fun(Log) ->
        couch_log:info("couch_chakra ~p :: ~s", [self(), Log])
    end, Logs),
    {[true, Resp], St};

run(_, Unknown) ->
    couch_log:error("couch_chakra: Unknown command: ~p~n", [Unknown]),
    throw({error, unknown_command}).


run_reduce(Type, Funs, Vals, St) ->
    {ok, Ctx} = create_reduce_context(St),
    ConvFuns = lists:map(fun(F) ->
        {ok, C} = convert_function(St, F),
        C
    end, Funs),
    {ok, [Logs, Results]} = chakra:call(Ctx, Type, [ConvFuns, Vals]),
    lists:foreach(fun(Log) ->
        couch_log:info("couch_chakra ~p :: ~s", [self(), Log])
    end, Logs),
    {[true, Results], St}.


convert_function(#st{conv_ctx = Ctx}, Script) ->
    io:format(standard_error, "NORMALIZING! ~s~n", [Script]),
    {ok, NewScript} = chakra:call(Ctx, 'normalizeFunction', [Script]),
    NewScript.


init_conv_context(Ctx) ->
    Scripts = [
        "conv/escodegen.js",
        "conv/esprima.js",
        "conv/normalizeFunction.js"
    ],
    lists:foreach(fun(Script) ->
        load_script(Ctx, Script)
    end, Scripts).


create_map_context(#st{runtime = Rt}) ->
    {ok, Ctx} = chakra:create_context(Rt),
    Scripts = [
        "util.js",
        "map.js"
    ],
    lists:foreach(fun(Script) ->
        load_script(Ctx, Script)
    end, Scripts),
    {ok, Ctx}.


create_reduce_context(#st{runtime = Rt}) ->
    {ok, Ctx} = chakra:create_context(Rt),
    Scripts = [
        "util.js",
        "reduce.js"
    ],
    lists:foreach(fun(Script) ->
        load_script(Ctx, Script)
    end, Scripts),
    {ok, Ctx}.


create_ddoc_context(#st{runtime = Rt}, DDoc) ->
    {ok, Ctx} = chakra:create_context(Rt),
    Scripts = [
        "conv/escodegen.js",
        "conv/esprima.js",
        "conv/normalizeFunction.js",
        "util.js",
        "ddoc.js"
    ],
    lists:foreach(fun(Script) ->
        load_script(Ctx, Script)
    end, Scripts),
    {ok, _} = chakra:call(Ctx, set_ddoc, [DDoc]),
    {ok, Ctx}.


load_script(Ctx, ScriptName) ->
    {ok, Script} = get_priv_file(ScriptName),
    BinScriptName = list_to_binary(ScriptName),
    {ok, _} = chakra:eval(Ctx, Script, [{source_url, BinScriptName}]).


get_priv_file(FileName) ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end,
    FilePath = filename:join(PrivDir, FileName),
    file:read_file(FilePath).
