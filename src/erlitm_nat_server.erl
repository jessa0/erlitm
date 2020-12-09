-module(erlitm_nat_server).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
-export([start_link/1, send/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).

-define(QUEUE_LEN, 1024).
-define(MAX_PORTS, 1024).

-type proto() :: udp.
-type ancillary_data() :: #{ttl := 0..255}.

-record(session,
        {dst  :: {inet:ip_address(), inet:port_number()},
         src  :: {inet:ip_address(), inet:port_number()},
         sock :: {inet:ip_address(), inet:port_number()}}).

-define(SOCKETS, #{udp => #{}}).
-record(state,
        {sessions :: #{proto() := ets:tid()},
         sockets  :: #{proto() := #{{inet:ip_address(), inet:port_number()} := inet:socket()}},
         tag      :: term(),
         handler  :: pid()}).

%%
%% public API
%%

-spec start_link(term()) -> {ok, pid()} | {error, any()}.
start_link(Tag) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Tag, self()}, []).

-spec send(pid(), proto(), {inet:ip_address(), inet:port_number()}, {inet:ip_address(), inet:port_number()}, ancillary_data(), iolist()) -> ok.
send(Pid, Proto, SrcAddr, DstAddr, AncData, Data) ->
    gen_server:cast(Pid, {send, Proto, SrcAddr, DstAddr, AncData, Data}).

%%
%% gen_server callbacks
%%

init({Tag, HandlerPid}) ->
    ?LOG_INFO(?MODULE_STRING " starting on node ~1000p", [node()]),
    UdpSessions = ets:new(?MODULE, [bag, private, {keypos, #session.dst}]),
    Sessions = #{udp => UdpSessions},
    Sockets = #{udp => #{}},
    {ok, #state{sessions = Sessions, sockets = Sockets, tag = Tag, handler = HandlerPid}}.

handle_call(Request, From, State) ->
    ?LOG_WARNING("unknown call from ~1000p: ~1000p", [From, Request]),
    {reply, unknown_call, State}.

handle_cast({send, Proto, SrcAddr, DstAddr, AncData, Data}, State) ->
    handle_send(Proto, SrcAddr, DstAddr, AncData, Data, State);

handle_cast(Message, State) ->
    ?LOG_WARNING("unknown cast: ~1000p", [Message]),
    {noreply, State}.

handle_info({udp, Sock, SrcAddr, SrcPort, AncData, Data}, State) ->
    handle_recv(udp, Sock, SrcAddr, SrcPort, AncData, Data, State);

handle_info(Message, State) ->
    ?LOG_WARNING("unknown message: ~1000p", [Message]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO(?MODULE_STRING " stopping on ~1000p: ~1000p", [node(), Reason]),
    ok.

%%
%% internal
%%

handle_send(Proto, {SrcAddr, SrcPort}, {DstAddr, DstPort}, AncData, Data, State) ->
    case get_or_create_session(Proto, {SrcAddr, SrcPort}, {DstAddr, DstPort}, State) of
        {ok, {Session, NewState}} ->
            ok = session_send(Proto, AncData, Data, Session, NewState),
            {noreply, NewState};
        {error, Err} ->
            ?LOG_WARNING("error creating session for ~1000p:~b -> ~1000p:~b proto ~s: ~1000p", [SrcAddr, SrcPort, DstAddr, DstPort, Proto, Err]),
            {noreply, State}
    end.

handle_recv(Proto, Sock, SrcAddr, SrcPort, [{ttl, TTL}], Data, State) ->
    #state{sessions = #{Proto := SessionsTab}, sockets = #{Proto := Sockets}} = State,
    ok = inet:setopts(Sock, [{active, 1}]),
    case ets:lookup(SessionsTab, {SrcAddr, SrcPort}) of
        [] ->
            %% we dont know about this host/port; drop
            {noreply, State};
        Sessions ->
            [Session] = [Session || Session <- Sessions, maps:get(Session#session.sock, Sockets) =:= Sock],
            #session{src = {SessionSrcAddr, SessionSrcPort}} = Session,
            State#state.handler ! {State#state.tag, {udp, {SrcAddr, SrcPort}, {SessionSrcAddr, SessionSrcPort}, TTL, Data}},
            {noreply, State}
    end.

get_or_create_session(Proto, Src, Dst, State) ->
    #state{sessions = #{Proto := SessionsTab}} = State,
    Sessions = ets:lookup(SessionsTab, Dst),
    case [Session || Session <- Sessions, Session#session.src =:= Src] of
        [Session] ->
            {ok, {Session, State}};
        [] ->
            create_session(Proto, Src, Dst, Sessions, State)
    end.

create_session(Proto, Src, Dst, DstSessions, State) ->
    case ensure_socket(Proto, DstSessions, State) of
        {ok, {{SockAddr, SockPort}, NewState}} ->
            #state{sessions = #{Proto := SessionsTab}} = State,
            Session = #session{src = Src, dst = Dst, sock = {SockAddr, SockPort}},
            true = ets:insert(SessionsTab, Session),
            {ok, {Session, NewState}};
        {error, Err} ->
            {error, Err}
    end.

ensure_socket(Proto, ExistingSessions, State) ->
    UsedSockAddrs = [UsedSockAddr || #session{sock = UsedSockAddr} <- ExistingSessions],
    #state{sockets = #{Proto := Sockets}} = State,
    case maps:to_list(maps:without(UsedSockAddrs, Sockets)) of
        [{{SockAddr, SockPort}, _Sock}|_] ->
            {ok, {{SockAddr, SockPort}, State}};
        [] ->
            create_socket(Proto, State)
    end.

create_socket(Proto, State) ->
    #state{sockets = #{Proto := Sockets}} = State,
    create_socket(Proto, Sockets, State).

create_socket(_Proto, Sockets, _State) when map_size(Sockets) >= ?MAX_PORTS ->
    {error, too_many_ports};
create_socket(udp, UdpSockets, State) ->
    case gen_udp:open(0, [binary, {active, ?QUEUE_LEN}, {recvttl, true}]) of
        {ok, Sock} ->
            {ok, {SockAddr, SockPort}} = inet:sockname(Sock),
            ?LOG_INFO("opened new udp socket on ~1000p:~b", [SockAddr, SockPort]),
            NewSockets = (State#state.sockets)#{udp => UdpSockets#{{SockAddr, SockPort} => Sock}},
            {ok, {{SockAddr, SockPort}, State#state{sockets = NewSockets}}};
        {error, Err} ->
            ?LOG_INFO("error opening new udp socket: ~1000p", [Err]),
            {error, {open_error, Err}}
    end.

session_send(udp, AncData, Data, Session, State) ->
    #session{dst = {DstAddr, DstPort}, sock = {SockAddr, SockPort}} = Session,
    #state{sockets = #{udp := #{{SockAddr, SockPort} := Sock}}} = State,
    case gen_udp:send(Sock, DstAddr, DstPort, maps:to_list(AncData), Data) of
        ok -> ok;
        {error, Err} ->
            ?LOG_WARNING("error sending ~1000p:~b -> ~1000p:~b proto udp len ~b: ~1000p", [SockAddr, SockPort, DstAddr, DstPort, iolist_size(Data), Err]),
            ok
    end.
