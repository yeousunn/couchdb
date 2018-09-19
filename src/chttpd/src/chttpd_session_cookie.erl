% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(chttpd_session_cookie).
-on_load(bump_server_epoch/0).
-include_lib("couch/include/couch_db.hrl").
-export([
    cookie_authentication_handler/1,
    handle_session_req/1
]).

-import(couch_httpd, [send_json/3, send_json/4]).

-define(COOKIE_NAME, "AuthSession").

-define(UNAUTHORIZED, {[
    {error, <<"unauthorized">>},
    {reason, <<"Name or password is incorrect.">>}
]}).

-define(COOKIE_INVALID,
    {unauthorized, <<"session cookie is invalid.">>}).


cookie_authentication_handler(#httpd{mochi_req=MochiReq} = Req) ->
    CookieValue = MochiReq:get_cookie_value(?COOKIE_NAME),
    Key = get_cookie_key(),
    AAD = aad(Req),
    case decode_cookie(Key, AAD, CookieValue) of
        false ->
            Req;
        Data ->couch_log:notice("decoded ~p", [Data]),
            UserName = proplists:get_value(u, Data),
            Expiration = proplists:get_value(x, Data),
            Expired = Expiration < system_time(),
            {ok, UserProps} = get_user_props(Req, UserName),
            ExpectedUserEpoch = list_to_integer(proplists:get_value(
                <<"session_epoch">>, UserProps, "0")),
            ActualUserEpoch = proplists:get_value(e, Data, 0),
            UserEpochMatch = ActualUserEpoch == ExpectedUserEpoch,
            if
                Expired ->
                    throw(?COOKIE_INVALID);
                not UserEpochMatch ->
                    throw(?COOKIE_INVALID);
                true ->
                    Req#httpd{user_ctx = #user_ctx{
                        name = UserName,
                        roles = proplists:get_value(<<"roles">>, UserProps)
                    }}
            end
    end.


handle_session_req(#httpd{method = 'POST'} = Req) ->
    MochiReq = Req#httpd.mochi_req,
    ReqBody = MochiReq:recv_body(),
    Form = case MochiReq:get_primary_header_value("content-type") of
        "application/x-www-form-urlencoded" ++ _ ->
            mochiweb_util:parse_qs(ReqBody);
        "application/json" ++ _ ->
            {Props} = jiffy:decode(ReqBody),
            Props
    end,
    UserName = ?l2b(proplists:get_value("username", Form)),
    Password = ?l2b(proplists:get_value("password", Form)),
    {ok, UserProps} = get_user_props(Req, UserName),
    case couch_httpd_auth:authenticate(Password, UserProps) of
        true ->
            couch_httpd_auth:verify_totp(UserProps, Form),
            AAD = aad(Req),
            CookieHeader = new_cookie_from_props(UserName, AAD, UserProps),
            Roles = proplists:get_value(<<"roles">>, UserProps, []),
            Body = {[{ok, true}, {name, UserName}, {roles, Roles}]},
            send_json(Req, 200, [CookieHeader], Body);
        false ->
            send_json(Req, 401, ?UNAUTHORIZED)
    end;

handle_session_req(#httpd{method = 'GET'} = Req) ->
    send_json(Req, 200, {[
        {ok, true},
        {<<"userCtx">>, {[
            {name, Req#httpd.user_ctx#user_ctx.name},
            {roles, Req#httpd.user_ctx#user_ctx.roles}
        ]}}
    ]}).


new_cookie_from_props(UserName, AAD, UserProps) ->
    UserEpoch = list_to_integer(
        proplists:get_value(<<"session_epoch">>, UserProps, "0")),
    Key = get_cookie_key(),
    TimeStamp = system_time(),
    MaxAge = max_age(),
    Data = [
        {u, UserName},
        {e, UserEpoch},
        {x, TimeStamp + MaxAge}],
    Options = [{path, "/"}, {max_age, MaxAge}],
    new_cookie(Key, AAD, Data, Options).


new_cookie(Key, AAD, Data, Options) ->
    PlainText = term_to_binary(Data),
    IV = new_iv(),
    CipherText = encrypt(Key, IV, AAD, PlainText),
    EncodedText = couch_util:encodeBase64Url(CipherText),
    mochiweb_cookies:cookie(?COOKIE_NAME, [$+, EncodedText], Options).


%% New-style cookies start with + as it does not appear in base64url
%% alphabet.
decode_cookie(Key, AAD, [$+ | EncodedText]) ->
    CipherText = couch_util:decodeBase64Url(EncodedText),
    PlainText = decrypt(Key, AAD, CipherText),
    binary_to_term(PlainText, [safe]);

% undefined or empty or old-style session cookie.
decode_cookie(_Key, _AAD, _CookieValue) ->
    false.


new_iv() ->
    HostPos = host_pos(),
    Epoch = config:get_integer("chttpd_auth", "cookie_epoch", 0),
    true = Epoch < 16#10000000000000000,
    Counter = erlang:unique_integer([positive]),
    true = Counter < 16#10000000000000000,
    <<HostPos:8, Epoch:22, Counter:66>>.


host_pos() ->
    host_pos(node(), mem3:nodes(), 1).


host_pos(N, [N | _Rest], Pos) ->
    Pos;
host_pos(N, [_ | Rest], Pos) ->
    host_pos(N, Rest, Pos + 1).


% use AAD to tie the cookie's validity to the host header.
aad(#httpd{} = Req) ->
    ?l2b((Req#httpd.mochi_req):get_header_value("Host")).


encrypt(Key, IV, AAD, PlainText)
  when is_binary(Key), bit_size(IV) == 96, is_binary(AAD)  ->
    {CipherText, CipherTag} = crypto:block_encrypt(
       aes_gcm, Key, IV, {AAD, PlainText, 16}),
    <<CipherTag/binary, IV/binary, CipherText/binary>>.


decrypt(Key, AAD, <<CipherTag:16/binary, IV:12/binary, CipherText/binary>>)
  when is_binary(Key), is_binary(AAD), is_binary(IV)  ->
    crypto:block_decrypt(
      aes_gcm, Key, IV, {AAD, CipherText, CipherTag}).


get_cookie_key() ->
    CookieKey = config:get("chttpd_auth", "cookie_key"),
    base64:decode(CookieKey).


system_time() ->
    os:system_time(second).


max_age() ->
    config:get_integer("chttpd_auth", "cookie_max_age", 600).


get_user_props(Req, UserName) ->
    {ok, UserProps, _AuthCtx} = chttpd_auth_cache:get_user_creds(Req, UserName),
    {ok, UserProps}.


bump_server_epoch() ->
    %% Bump the IV epoch to ensure uniqueness across reboots.
    Epoch = config:get_integer("chttpd_auth", "cookie_epoch", 0),
    config:set_integer("chttpd_auth", "cookie_epoch", Epoch + 1).
