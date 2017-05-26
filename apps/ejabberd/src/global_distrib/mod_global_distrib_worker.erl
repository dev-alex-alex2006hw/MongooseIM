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
        {_, _, _, _} = Command -> execute_command(Command);
        Commands when is_list(Commands) -> lists:foreach(fun execute_command/1, Commands)
    end,
    {noreply, State}.

execute_command({insert, Domain, PeerHost, Stamp}) when is_binary(Domain) ->
    mod_global_distrib_mapping:insert_for_domain(Domain, PeerHost, Stamp);
execute_command({delete, Domain, PeerHost, Stamp}) when is_binary(Domain) ->
    mod_global_distrib_mapping:delete_for_domain(Domain, PeerHost, Stamp);
execute_command({insert, Jid, PeerHost, Stamp}) ->
    mod_global_distrib_mapping:insert_for_jid(Jid, PeerHost, Stamp);
execute_command({delete, Jid, PeerHost, Stamp}) ->
    mod_global_distrib_mapping:delete_for_jid(Jid, PeerHost, Stamp).

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
