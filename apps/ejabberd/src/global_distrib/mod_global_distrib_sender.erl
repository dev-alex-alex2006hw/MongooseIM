-module(mod_global_distrib_sender).

-behaviour(gen_mod).
-behaviour(supervisor).

-include("ejabberd.hrl").
-include("jlib.hrl").

-export([start/2, stop/1, subscribe_to_all/0, poolname_to_server/1, send_all/1, send/2, start_link/0, init/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start(Host, Opts0) ->
    Opts = [{listen_port, 5555}, {num_of_connections, 1} | Opts0],
    mod_global_distrib_utils:start(?MODULE, Host, Opts, fun start/0).

stop(Host) ->
    mod_global_distrib_utils:stop(?MODULE, Host, fun stop/0).

start() ->
    Names = [Name || {Name, _Args} <- connection_args()],
    ets:insert(?MODULE, {server_names, Names}),
    ChildSpec = {?MODULE, {?MODULE, start_link, []}, transient, 1000, supervisor, [?MODULE]},
    {ok, _} = supervisor:start_child(ejabberd_sup, ChildSpec).

stop() ->
    supervisor:terminate_child(ejabberd_sup, ?MODULE),
    supervisor:delete_child(ejabberd_sup, ?MODULE),
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

subscribe_to_all() ->
    [{Name, cpool:subscribe(Name)} || Name <- opt(server_names)].

send_all(Data) when is_binary(Data) ->
    lists:foreach(fun(Name) ->
        Conn = cpool:get_connection(Name, no_wait_for_reconnect),
        mod_global_distrib_utils:cast_or_call(gen_server, Conn, {data, Data})
    end, opt(server_names));
send_all(Term) ->
    send_all(term_to_binary(Term)).

send(Server, {From, _To, Acc} = Packet) ->
    BinPacket = term_to_binary(Packet),
    send(Server, BinPacket),
    ejabberd_hooks:run(global_distrib_send_packet, [From, mongoose_acc:get(to_send, Acc)]),
    ok;
send(Server, Term) when not is_binary(Term) ->
    send(Server, term_to_binary(Term));
send(Server, Data) when is_binary(Server) ->
    send(server_to_atom(Server), Data);
send(PoolName, Data) when is_atom(PoolName), is_binary(Data) ->
    Conn = cpool:get_connection(PoolName),
    mod_global_distrib_utils:cast_or_call(gen_server, Conn, {data, Data}).

init(_) ->
    Children =
        lists:map(
            fun({Name, {_Server, Addr, Port}}) ->
                cpool:pool_spec(Name, {mod_global_distrib_connection, start_link, [Addr, Port]},
                                [{pool_size, opt(num_of_connections)}])
            end,
            connection_args()),

    {ok, {{one_for_one, 5, 60}, Children}}.

server_to_atom(Server) when is_binary(Server) ->
    binary_to_atom(base64:encode(Server), latin1);
server_to_atom(Server) ->
    server_to_atom(unicode:characters_to_binary(Server)).

poolname_to_server(Name) ->
    base64:decode(atom_to_binary(Name, latin1)).

connection_args() ->
    LocalHostList = unicode:characters_to_list(opt(local_host)),
    lists:filtermap(
        fun
            ({Server, Addr, Port}) when is_list(Server), Server =/= LocalHostList ->
                {true, {server_to_atom(Server), {Server, Addr, Port}}};
            (Server) when is_list(Server), Server =/= LocalHostList ->
                {true, {server_to_atom(Server), {Server, Server, opt(listen_port)}}};
            (_) ->
                false
        end,
        opt(servers)).

opt(Key) ->
    mod_global_distrib_utils:opt(?MODULE, Key).
