-module(erlitm_tproxy_server).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
-export([start_link/1, forward/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(RCVBUF, 2048).
-define(RCVCTRLBUF, 1024).

-define(PROTO_UDP, 17).

-type proto() :: udp.

-record(select_info,
        {tag :: socket:select_tag(),
         ref :: socket:select_ref()}).

-record(ip_hdr,
        {version       :: 4 | 6,
         hdr_len  = 5  :: 5..16,
         dscp     = 0  :: 0..64,
         ecn      = 0  :: 0..3,
         len      = 0  :: 0..65535,
         id       = 0  :: 0..65535,
         flags    = 0  :: 0..7,
         frag_off = 0  :: 0..8192,
         ttl      = 64 :: 0..255,
         proto         :: 0..255,
         cksum    = 0  :: 0..65535,
         src           :: socket:ip4_address() | socket:ip6_address(),
         dst           :: socket:ip4_address() | socket:ip6_address()}).
-record(udp_hdr,
        {src_port  :: 0..65535,
         dst_port  :: 0..65535,
         len       :: 0..65535,
         cksum = 0 :: 0..65535}).

-record(state,
        {recv_udp_sock :: socket:socket(),
         fwd_udp_sock  :: socket:socket(),
         recv_udp      :: #select_info{} | undefined,
         tag           :: term(),
         handler       :: pid()}).

%%
%% public API
%%

-spec start_link(term()) -> {ok, pid()} | {error, any()}.
start_link(Tag) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Tag, self()}, []).

-spec forward(pid(), proto(), {inet:ip_address(), inet:port_number()}, {inet:ip_address(), inet:port_number()}, 0..255, iolist()) -> ok.
forward(Pid, Proto, SrcAddr, DstAddr, TTL, Data) ->
    gen_server:cast(Pid, {forward, Proto, SrcAddr, DstAddr, TTL, Data}).

%%
%% gen_server callbacks
%%

init({Tag, HandlerPid}) ->
    ?LOG_INFO(?MODULE_STRING " starting on node ~1000p", [node()]),
    RecvSock = recv_udp_sock(),
    SendSock = fwd_udp_sock(),
    gen_server:cast(self(), recv_udp),

    {ok, #state{recv_udp_sock = RecvSock, fwd_udp_sock = SendSock, tag = Tag, handler = HandlerPid}}.

handle_call(Request, From, State) ->
    ?LOG_WARNING("unknown call from ~1000p: ~1000p", [From, Request]),
    {reply, unknown_call, State}.

handle_cast(recv_udp, #state{recv_udp = undefined}=State) ->
    recv_udp(State);

handle_cast({forward, udp, SrcAddr, DstAddr, TTL, Data}, State) ->
    forward_udp(SrcAddr, DstAddr, TTL, Data, State);

handle_cast(Message, State) ->
    ?LOG_WARNING("unknown cast: ~1000p", [Message]),
    {noreply, State}.

handle_info({'$socket', Sock, select, SelectRef}, #state{recv_udp_sock = Sock}=State) ->
    handle_select(SelectRef, State);

handle_info({'$socket', Sock, abort, AbortInfo}, #state{recv_udp_sock = Sock}=State) ->
    handle_select_abort(AbortInfo, State);

handle_info(Message, State) ->
    ?LOG_WARNING("unknown message: ~1000p", [Message]),
    {noreply, State}.

terminate(Reason, State) ->
    ?LOG_INFO(?MODULE_STRING " stopping on ~1000p: ~1000p", [node(), Reason]),
    ok = socket:close(State#state.recv_udp_sock),
    ok.

%%
%% internal
%%

recv_udp_sock() ->
    {ok, Sock} = socket:open(inet, dgram, udp),
    {ok, _Port} = socket:bind(Sock, #{family => inet, addr => loopback, port => amongerl_app:tproxy_port()}),
    ok = socket:setopt(Sock, otp, rcvbuf, ?RCVBUF),
    ok = socket:setopt(Sock, otp, rcvctrlbuf, ?RCVCTRLBUF),
    ok = socket:setopt(Sock, socket, reuseaddr, true),
    ok = socket:setopt(Sock, socket, reuseport, true),
    ok = socket:setopt(Sock, ip, recvorigdstaddr, true),
    ok = socket:setopt(Sock, ip, recvttl, true),
    case socket:setopt(Sock, ip, transparent, true) of
        ok             -> ok;
        {error, eperm} -> ?LOG_WARNING("permission denied enabling IP_TRANSPARENT flag on proxy socket", [])
    end,
    Sock.

recv_udp(State) ->
    case socket:recvmsg(State#state.recv_udp_sock, nowait) of
        {ok, MsgHdr} ->
            NewState = handle_message(MsgHdr, State#state{recv_udp = undefined}),
            gen_server:cast(self(), recv_udp),
            {noreply, NewState};
        {select, SelectInfo} ->
            {noreply, State#state{recv_udp = SelectInfo}};
        {error, Err} ->
            {stop, {socket_error, Err}, State#state{recv_udp = undefined}}
    end.

handle_select(SelectRef, #state{recv_udp = #select_info{ref = SelectRef}}=State) ->
    recv_udp(State).

handle_select_abort({SelectRef, Reason}, #state{recv_udp = #select_info{ref = SelectRef}}=State) ->
    ?LOG_WARNING("udp recv aborted: ~1000p", [Reason]),
    recv_udp(State).

handle_message(MsgHdr, State) ->
    #{addr := #{addr := SrcAddr, port := SrcPort}, ctrl := MsgCtrl, iov := Data} = MsgHdr,
    #{{ip, origdstaddr} := #{addr := DstAddr, port := DstPort}, {ip, ttl} := TTL} = decode_msg_ctrl(MsgCtrl, #{}),
    State#state.handler ! {State#state.tag, {udp, {SrcAddr, SrcPort}, {DstAddr, DstPort}, TTL, Data}},
    State.

decode_msg_ctrl([#{level := Level, type := Type, data := Data} | Rest], Acc) ->
    decode_msg_ctrl(Rest, Acc#{{Level, Type} => Data});
decode_msg_ctrl([], Acc) ->
    Acc.

fwd_udp_sock() ->
    {ok, Sock} = socket:open(inet, raw, udp),
    ok = socket:setopt(Sock, ip, hdrincl, true),
    Sock.

forward_udp({SrcAddr, SrcPort}, {DstAddr, DstPort}, TTL, Data, State) ->
    DataLen = iolist_size(Data),

    IpHdr = #ip_hdr{version = 4, ttl = TTL, proto = ?PROTO_UDP, src = SrcAddr, dst = DstAddr},
    UdpHdr = #udp_hdr{src_port = SrcPort, dst_port = DstPort, len = 8 + DataLen},

    SendMsgHdr = #{addr => sockaddr({DstAddr, DstPort}), iov => [ip_hdr(IpHdr), udp_hdr(UdpHdr), Data], ctrl => [], flags => []},
    case socket:sendmsg(State#state.fwd_udp_sock, SendMsgHdr) of
        ok              -> ok;
        {ok, Remaining} -> ?LOG_WARNING("sendmsg truncated ~b bytes", [erlang:iolist_size(Remaining)])
    end,
    {noreply, State}.

ip_hdr(Hdr) ->
    << (Hdr#ip_hdr.version):4, (Hdr#ip_hdr.hdr_len):4,
       (Hdr#ip_hdr.dscp):6, (Hdr#ip_hdr.ecn):2,
       (Hdr#ip_hdr.len):16,
       (Hdr#ip_hdr.id):16,
       (Hdr#ip_hdr.flags):3, (Hdr#ip_hdr.frag_off):13,
       (Hdr#ip_hdr.ttl),
       (Hdr#ip_hdr.proto),
       (Hdr#ip_hdr.cksum):16,
       (ip_addr(Hdr#ip_hdr.src))/binary,
       (ip_addr(Hdr#ip_hdr.dst))/binary >>.

ip_addr({A,B,C,D}) ->
    << A, B, C, D >>.

udp_hdr(Hdr) ->
    << (Hdr#udp_hdr.src_port):16,
       (Hdr#udp_hdr.dst_port):16,
       (Hdr#udp_hdr.len):16,
       (Hdr#udp_hdr.cksum):16 >>.

sockaddr({Addr, Port}) when tuple_size(Addr) =:= 4 ->
    #{family => inet, addr => Addr, port => Port};
sockaddr({Addr, Port}) when tuple_size(Addr) =:= 16 ->
    #{family => inet6, addr => Addr, port => Port}.
