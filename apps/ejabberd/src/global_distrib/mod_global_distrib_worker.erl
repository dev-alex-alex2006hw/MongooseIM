-module(mod_global_distrib_worker).

-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

init(_) ->
    {ok, state}.

handle_call({data, Data}, From, State) ->
    gen_server:reply(From, ok),
    handle_cast({data, Data}, State).

handle_cast({data, Data}, State) ->
    case erlang:binary_to_term(Data) of
        {From, To, Acc} -> ejabberd_router:route(From, To, Acc);
        {insert_for_domain, Domain, PeerHost, Stamp} ->
            mod_global_distrib_mapping:insert_for_domain(Domain, PeerHost, Stamp);
        {delete_for_domain, Domain, PeerHost, Stamp} ->
            mod_global_distrib_mapping:delete_for_domain(Domain, PeerHost, Stamp);
        {insert_for_jid, Jid, PeerHost, Stamp} ->
            mod_global_distrib_mapping:insert_for_jid(Jid, PeerHost, Stamp);
        {delete_for_jid, Jid, PeerHost, Stamp} ->
            mod_global_distrib_mapping:delete_for_jid(Jid, PeerHost, Stamp)
    end,
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
