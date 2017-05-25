-module(mod_global_distrib_connection).

-behaviour(ecpool_worker).
-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

-export([connect/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

connect(Opts) ->
    {_, Addr} = lists:keyfind(host, 1, Opts),
    {_, Port} = lists:keyfind(port, 1, Opts),
    gen_server:start_link(?MODULE, {Addr, Port}, []).

init({Addr, Port}) ->
    try
        proc_lib:init_ack({ok, self()}),
        {ok, Socket} = gen_tcp:connect(Addr, Port, [binary, {active, false}]),
        {ok, TLSSocket} = fast_tls:tcp_to_tls(Socket, [{certfile, opt(certfile)}, {cafile, opt(cafile)}, connect]),
        fast_tls:setopts(TLSSocket, [{active, once}]),
        {ok, TLSSocket}
    catch
        error:{badmatch, Reason} -> {stop, Reason}
    end.

handle_call({data, Data}, From, Socket) ->
    gen_server:reply(From, ok),
    handle_cast({data, Data}, Socket).

handle_cast({data, Data}, Socket) ->
    Annotated = <<(byte_size(Data)):32, Data/binary>>,
    ok = fast_tls:send(Socket, Annotated),
    {noreply, Socket}.

handle_info({tcp, _Socket, TLSData}, Socket) ->
    ok = fast_tls:setopts(Socket, [{active, once}]),
    fast_tls:recv_data(Socket, TLSData),
    {noreply, Socket};
handle_info({tcp_closed, _}, Socket) ->
    fast_tls:close(Socket),
    {stop, normal, Socket};
handle_info(_, Socket) ->
    {noreply, Socket}.

terminate(_Reason, _State) ->
    ignore.

opt(Key) ->
    mod_global_distrib_utils:opt(mod_global_distrib_sender, Key).
