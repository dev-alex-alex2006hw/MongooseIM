%%==============================================================================
%% Copyright 2017 Erlang Solutions Ltd.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================

-module(mod_global_distrib_mapping).
-author('konrad.zemek@erlang-solutions.com').

-behaviour(gen_mod).

-include("ejabberd.hrl").
-include("jlib.hrl").

-record(global_distrib_session, {
    key :: {_, _, _} | binary(),
    bare :: {_, _, _} | undefined,
    host :: binary(),
    stamp :: integer()
}).

-export([start/2, stop/1]).
-export([for_domain/1, insert_for_domain/3, delete_for_domain/3, all_domains/0]).
-export([for_jid/1, insert_for_jid/3, delete_for_jid/3]).
-export([register_subhost/2, unregister_subhost/2, user_present/2, user_not_present/5]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

insert_for_domain(Domain, From, Stamp) when is_binary(Domain), is_binary(From), is_integer(Stamp) ->
    do_add(Domain, undefined, From, Stamp).

insert_for_jid(#jid{} = Jid, From, Stamp) ->
    insert_for_jid(jid:to_lower(Jid), From, Stamp);
insert_for_jid({_, _, Resource} = FullJid, From, Stamp)
  when byte_size(Resource) > 0, is_binary(From), is_integer(Stamp) ->
    BareJid = jid:to_bare(FullJid),
    do_add(FullJid, BareJid, From, Stamp).

delete_for_domain(Domain, From, Stamp) when is_binary(Domain), is_binary(From), is_integer(Stamp) ->
    do_delete(Domain, From, Stamp).

delete_for_jid(#jid{} = Jid, From, Stamp) ->
    delete_for_jid(jid:to_lower(Jid), From, Stamp);
delete_for_jid({_, _, Resource} = FullJid, From, Stamp)
  when byte_size(Resource) > 0, is_binary(From), is_integer(Stamp) ->
    do_delete(FullJid, From, Stamp).

for_domain(Domain) when is_binary(Domain) ->
    case mnesia:dirty_read(global_distrib_session, Domain) of
        [] -> error;
        [#global_distrib_session{host = Host}] -> {ok, Host}
    end.

for_jid(#jid{} = Jid) ->
    for_jid(jid:to_lower(Jid));
for_jid({_, _, <<>>} = BareJid) ->
    case mnesia:dirty_index_read(global_distrib_session, BareJid, bare) of
        [] -> error;
        [#global_distrib_session{host = Host} | _] -> {ok, Host}
    end;
for_jid({_, _, _} = FullJid) ->
    case mnesia:dirty_read(global_distrib_session, FullJid) of
        [] -> for_jid(jid:to_bare(FullJid));
        [#global_distrib_session{host = Host}] -> {ok, Host}
    end.

start(Host, Opts) ->
    mod_global_distrib_utils:start(?MODULE, Host, Opts, fun start/0).

stop(Host) ->
    mod_global_distrib_utils:stop(?MODULE, Host, fun stop/0).

all_domains() ->
    DomainSessions = mnesia:dirty_index_read(global_distrib_session, undefined, bare),
    {ok, [Domain || #global_distrib_session{key = Domain} <- DomainSessions]}.

%%--------------------------------------------------------------------
%% Hooks implementation
%%--------------------------------------------------------------------

-spec user_present(Acc :: map(), UserJID :: ejabberd:jid()) -> ok.
user_present(Acc, #jid{} = Jid) ->
    Now = stamp(),
    UserJid = jid:to_lower(Jid),
    insert_for_jid(UserJid, opt(local_host), Now),
    mod_global_distrib_sender:send_all({insert_for_jid, UserJid, opt(local_host), Now}),
    Acc.

-spec user_not_present(Acc :: map(),
                       User     :: ejabberd:luser(),
                       Server   :: ejabberd:lserver(),
                       Resource :: ejabberd:lresource(),
                       _Status :: any()) -> ok.
user_not_present(Acc, User, Host, Resource, _Status) ->
    Now = stamp(),
    UserJid = jid:to_lower({User, Host, Resource}),
    delete_for_jid(UserJid, opt(local_host), Now),
    mod_global_distrib_sender:send_all({delete_for_jid, UserJid, opt(local_host), Now}),
    Acc.

register_subhost(_, SubHost) ->
    Now = stamp(),
    IsSubhostOf =
        fun(Host) ->
                case binary:match(SubHost, Host) of
                    {Start, Length} -> Start + Length == byte_size(SubHost);
                    _ -> false
                end
        end,

    GlobalHost = opt(global_host),
    case lists:filter(IsSubhostOf, ?MYHOSTS) of
        [GlobalHost] ->
            insert_for_domain(SubHost, opt(local_host), Now),
            mod_global_distrib_sender:send_all({insert_for_domain, SubHost, opt(local_host), Now});

        _ ->
            ok
    end.

unregister_subhost(_, SubHost) ->
    Now = stamp(),
    delete_for_domain(SubHost, opt(local_host), Now),
    mod_global_distrib_sender:send_all({delete_for_domain, SubHost, opt(local_host), Now}).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

start() ->
    Host = opt(global_host),

    mnesia:create_table(global_distrib_session,
                        [{ram_copies, [node()]}, {index, [bare, host]},
                         {attributes, record_info(fields, global_distrib_session)}]),
    mnesia:add_table_copy(global_distrib_session, node(), ram_copies),

    ejabberd_hooks:add(register_subhost, global, ?MODULE, register_subhost, 90),
    ejabberd_hooks:add(unregister_subhost, global, ?MODULE, unregister_subhost, 90),
    ejabberd_hooks:add(user_available_hook, Host, ?MODULE, user_present, 90),
    ejabberd_hooks:add(unset_presence_hook, Host, ?MODULE, user_not_present, 90).

stop() ->
    Host = opt(global_host),

    ejabberd_hooks:delete(unset_presence_hook, Host, ?MODULE, user_not_present, 90),
    ejabberd_hooks:delete(user_available_hook, Host, ?MODULE, user_present, 90),
    ejabberd_hooks:delete(unregister_subhost, global, ?MODULE, unregister_subhost, 90),
    ejabberd_hooks:delete(register_subhost, global, ?MODULE, register_subhost, 90),

    mnesia:delete_table(global_distrib_session).

do_add(Key, Bare, From, Stamp) ->
    mnesia:transaction(fun() ->
        case mnesia:read(global_distrib_session, Key, write) of
            [#global_distrib_session{stamp = OldStamp}] when OldStamp > Stamp -> ok;
            _ -> mnesia:write(#global_distrib_session{key = Key, bare = Bare,
                                                      host = From, stamp = Stamp})
        end
    end).

do_delete(Key, From, Stamp) ->
    mnesia:transaction(fun() ->
        case mnesia:read(global_distrib_session, Key, write) of
            [#global_distrib_session{host = From, stamp = OldStamp}] when OldStamp =< Stamp ->
                mnesia:delete({global_distrib_session, Key});
            _ ->
                ok
        end
    end).

opt(Key) ->
    mod_global_distrib_utils:opt(?MODULE, Key).

stamp() ->
    p1_time_compat:system_time().
