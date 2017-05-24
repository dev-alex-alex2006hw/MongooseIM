-module(mod_global_distrib_sender).

-behaviour(gen_mod).
-behaviour(supervisor).

-include("ejabberd.hrl").
-include("jlib.hrl").

-export([start/2, stop/1, send_all/1, send/2, start_link/0, init/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start(Host, Opts0) ->
    Opts = [{listen_port, 5555}, {num_of_connections, 1} | Opts0],
    mod_global_distrib_utils:start(?MODULE, Host, Opts, fun start/0).

stop(Host) ->
    mod_global_distrib_utils:stop(?MODULE, Host, fun stop/0).

start() ->
    LocalHost = opt(local_host),
    Servers = lists:map(fun({Server, _, _}) -> Server; (Server) -> Server end, opt(servers)),
    Names = [server_to_atom(Server) || Server <- Servers, Server =/= LocalHost],
    ets:insert(?MODULE, {server_names, Names}),
    ChildSpec = {?MODULE, {?MODULE, start_link, []}, transient, 1000, supervisor, [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop() ->
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


send_all(Data) when is_binary(Data) ->
    lists:foreach(fun(Name) ->
        Worker = wpool_pool:best_worker(Name),
        mod_global_distrib_utils:cast_or_call(wpool_process, Worker, {data, Data})
    end, opt(server_names));
send_all(Term) ->
    send_all(term_to_binary(Term)).

send(Server, {From, _To, Acc} = Packet) ->
    BinPacket = term_to_binary(Packet),
    send(Server, BinPacket),
    ejabberd_hooks:run(global_distrib_send_packet, [From, mongoose_acc:get(to_send, Acc)]),
    ok;
send(Server, Data) when is_binary(Data) ->
    Worker = get_process_for(Server),
    ok = mod_global_distrib_utils:cast_or_call(wpool_process, Worker, {data, Data}).

get_process_for(Server) ->
    Name = server_to_atom(Server),
    wpool_pool:best_worker(Name).

init(_) ->
    LocalHost = opt(local_host),
    Servers = opt(servers),
    WorkerArgs = lists:filtermap(
        fun
            ({Server, Addr, Port}) when Server =/= LocalHost ->
                {true, {server_to_atom(Server), [Server, Addr, Port]}};
            (Server) when Server =/= LocalHost ->
                {true, {server_to_atom(Server), [Server, Server, opt(listen_port)]}};
            (_) ->
                false
        end,
        Servers),

    Children = lists:map(
        fun({Name, Args}) ->
            PoolArgs = [Name, [{workers, opt(num_of_connections)},
                               {worker, {mod_global_distrib_connection, Args}}]],
            {wpool_pool, {wpool_pool, start_link, PoolArgs}, permanent, 5000, worker, dynamic}
        end,
        WorkerArgs),

    {ok, {{one_for_one, 1000000, 1}, Children}}.

server_to_atom(Server) ->
    binary_to_atom(base64:encode(Server), latin1).

opt(Key) ->
    mod_global_distrib_utils:opt(?MODULE, Key).
