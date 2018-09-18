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

-module(couch_lru).


-export([
    new/0,
    push/2,
    pop/1,
    peek_newest/1,
    update/2,

    % Test functions
    to_list/1
]).


-record(node, {
    dbname,
    prev,
    next
}).


new() ->
    TableOpts = [protected, set, {keypos, #node.dbname}],
    {ets:new(?MODULE, TableOpts), undefined, undefined}.


push(DbName, T0) when is_binary(DbName) ->
    {Table, Head, Tail} = remove(DbName, T0),
    case {Head, Tail} of
        {undefined, undefined} ->
            % Empty LRU
            ok = add_node(Table, #node{dbname = DbName}),
            {Table, DbName, DbName};
        {Head, Head} ->
            % Single element LRU
            ok = add_node(Table, #node{dbname = DbName, next = Head}),
            ok = set_prev(Table, Head, DbName),
            {Table, DbName, Head};
        {Head, Tail} ->
            ok = add_node(Table, #node{dbname = DbName, next = Head}),
            ok = set_prev(Table, Head, DbName),
            {Table, DbName, Tail}
    end.


pop({_Table, undefined, undefined} = T0) ->
    {undefined, T0};
pop({_Table, _Head, Tail} = T0) when is_binary(Tail) ->
    {Tail, remove(Tail, T0)}.


peek_newest({_Table, Head, _Tail}) ->
    Head.


update(DbName, {Table, _, _} = T0) when is_binary(DbName) ->
    case get_node(Table, DbName) of
        undefined ->
            % We closed this database beore processing the update. Ignore
            T0;
        _ ->
            push(DbName, T0)
    end.


to_list({_, undefined, undefined}) ->
    [];
to_list({_Table, Head, Head}) when is_binary(Head) ->
    [Head];
to_list({Table, Head, Tail}) when is_binary(Head), is_binary(Tail) ->
    to_list(Table, Head, []).


to_list(Table, undefined, Nodes) ->
    true = length(Nodes) == ets:info(Table, size),
    lists:reverse(Nodes);
to_list(Table, Curr, Nodes) when is_binary(Curr) ->
    false = lists:member(Curr, Nodes),
    Node = get_node(Table, Curr),
    to_list(Table, Node#node.next, [Curr | Nodes]).


% Internal

remove(DbName, {Table, Head, Tail}) when is_binary(DbName) ->
    case get_node(Table, DbName) of
        undefined ->
            {Table, Head, Tail};
        Node ->
            ok = set_next(Table, Node#node.prev, Node#node.next),
            ok = set_prev(Table, Node#node.next, Node#node.prev),
            ok = del_node(Table, Node),
            NewHead = if DbName /= Head -> Head; true ->
                Node#node.next
            end,
            NewTail = if DbName /= Tail -> Tail; true ->
                Node#node.prev
            end,
            {Table, NewHead, NewTail}
    end.


get_node(_Table, #node{} = Node) ->
    Node;
get_node(Table, DbName) ->
    case ets:lookup(Table, DbName) of
        [] ->
            undefined;
        [Node] ->
            Node
    end.


%% get_next(Table, #node{next = undefined}) ->
%%     undefined;
%% get_next(Table, #node{next = DbName}) ->
%%     [Node] = ets:lookup(Table, DbName),
%%     Node;
%% get_next(Table, DbName) when is_binary(DbName) ->
%%     Node = #node{} = get_node(Table, DbName),
%%     get_next(Table, Node).
%%
%%
%% get_prev(Table, #node{prev = undefined}) ->
%%     undefined;
%% get_prev(Table, #node{prev = DbName}) ->
%%     [Node] = ets:lookup(Table, DbName),
%%     Node;
%% get_prev(Table, DbName) when is_binary(DbName) ->
%%     Node = #node{} = get_node(Table, DbName),
%%     get_prev(Table, Node).


add_node(Table, #node{} = Node) ->
    true = ets:insert_new(Table, Node),
    ok.


del_node(Table, #node{} = Node) ->
    MSpec = {Node, [], [true]},
    1 = ets:select_delete(Table, [MSpec]),
    ok.


set_next(_, undefined, _) ->
    ok;
set_next(Table, #node{dbname = DbName}, Next) ->
    set_next(Table, DbName, Next);
set_next(Table, Node, #node{dbname = Next}) ->
    set_next(Table, Node, Next);
set_next(Table, NodeDbName, NextDbName) when is_binary(NodeDbName) ->
    true = ets:update_element(Table, NodeDbName, {#node.next, NextDbName}),
    ok.


set_prev(_, undefined, _) ->
    ok;
set_prev(Table, #node{dbname = DbName}, Prev) when is_binary(DbName) ->
    set_prev(Table, DbName, Prev);
set_prev(Table, Node, #node{dbname = DbName}) ->
    set_prev(Table, Node, DbName);
set_prev(Table, NodeDbName, PrevDbName) when is_binary(NodeDbName) ->
    true = ets:update_element(Table, NodeDbName, {#node.prev, PrevDbName}),
    ok.
