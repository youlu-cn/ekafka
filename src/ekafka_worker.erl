%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 十二月 2015 15:00
%%%-------------------------------------------------------------------
-module(ekafka_worker).
-author("luyou").

-include("ekafka.hrl").

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-compile(export_all).

-record(request_state, {type, from, api}).

-record(state, {sock,
                topic,
                partition      :: #partition{},
                role,
                trace_id,
                dict           :: dict:dict(int32(), #request_state{}),
                offset     = 0 :: int64()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Name :: string(), Partition :: #partition{}, Role :: producer | consumer) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Name, Partition, Role) ->
    gen_server:start_link(?MODULE, {Name, Partition, Role}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init({Name, Partition, Role}) ->
    erlang:send(self(), start_connection),
    {ok, #state{topic = Name, partition = Partition, role = Role, trace_id = 0, dict = dict:new()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
        State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({produce, Type, KVList}, From, #state{topic = Topic, partition = Part, role = Role, trace_id = Trace, sock = Sock} = State) ->
    case Role of
        producer ->
            Request = handle_produce(Topic, Part, KVList),
            ?DEBUG("[W] produce request data: ~p~n", [Request]),
            NewTrace = get_trace_id(Trace),
            %% Type :: atom(), sync | async
            erlang:spawn(?MODULE, send_request, [Sock, {Type, From, self(), NewTrace, Request}]),
            {noreply, State#state{trace_id = NewTrace}};
        _ ->
            {reply, {error, invalid_operation}, State}
    end;
handle_call({consume, Type}, From, #state{topic = Topic, partition = Part, role = Role, trace_id = Trace, sock = Sock, offset = Offset} = State) ->
    case Role of
        consumer ->
            Request = handle_consume(Type, Topic, Part, Offset),
            NewTrace = get_trace_id(Trace),
            %% Type :: atom(), sync | async
            erlang:spawn(?MODULE, send_request, [Sock, {Type, From, self(), NewTrace, Request}]),
            {noreply, State#state{trace_id = NewTrace}};
        _ ->
            {reply, {error, invalid_operation}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(start_connection, #state{topic = Name, partition = #partition{id = ID, lead = Lead}} = State) ->
    ?INFO("[W] start worker connection, topic: ~p, partition: ~p~n", [Name, ID]),
    case connect_to_lead_broker(Lead) of
        undefined ->
            ?ERROR("[W] init failed to connect broker, exit..~n", []),
            {stop, broker_not_connected, State};
        {ok, Sock} ->
            erlang:send(self(), init_begin_offset),
            {noreply, State#state{sock = Sock}}
    end;
handle_info(init_begin_offset, #state{sock = Sock, topic = Name, role = Role, partition = #partition{id = PartID}} = State) ->
    case Role of
        producer ->
            {noreply, State};
        consumer ->
            Offset = get_init_offset(Sock, Name, PartID),
            erlang:send_after(5000, self(), sync_group_offset),
            {noreply, State#state{offset = Offset}}
    end;
handle_info(sync_group_offset, #state{topic = Name, partition = #partition{id = PartID}, offset = Offset} = State) ->
    case sync_message_offset(Name, PartID) of
        -1 ->
            ?DEBUG("[W] no group consume offset for ~p:~p, use local: ~p~n", [Name, PartID, Offset]),
            {noreply, State};
        Res when Res > Offset ->
            ?INFO("[W] got group consume offset, ~p:~p, value: ~p~n", [Name, PartID, Res]),
            {noreply, State#state{offset = Res}}
    end;

%% handle socket
handle_info({tcp, Sock, Data}, #state{sock = Sock, dict = Dict} = State) ->
    ?DEBUG("[W] tcp data received, ~p, ~p~n", [self(), Data]),
    erlang:spawn(?MODULE, decode_response, [{?MODULE, query_response_api, Dict}, self(), Data]),
    {noreply, State};
handle_info({re_connect, Reason, Times}, #state{topic = Name, partition = #partition{id = ID, lead = Lead}} = State) ->
    ?WARNING("[W] ~p reconnecting to broker, reason: ~p, topic: ~p, partition: ~p~n", [Times, Reason, Name, ID]),
    case connect_to_lead_broker(Lead) of
        undefined ->
            erlang:send_after(1000, self(), {re_connect, Reason, Times + 1}),
            {noreply, State};
        {ok, Sock} ->
            ?INFO("[W] broker reconnected, ~p:~p~n", [Name, ID]),
            {noreply, State#state{sock = Sock}}
    end;
handle_info({tcp_closed, _Sock}, State) ->
    %%TODO: don't assign works before re-connected
    ?WARNING("[W] server closed connection, reconnect later~n", []),
    erlang:send_after(100, self(), {re_connect, tcp_closed, 1}),
    {noreply, State};
handle_info({tcp_error, _Sock, Reason}, State) ->
    %%TODO: don't assign works before re-connected
    ?ERROR("[W] tcp error occurs, ~p, reconnect later~n", [Reason]),
    erlang:send_after(100, self(), {re_connect, tcp_error, 1}),
    {noreply, State};

%% handle request callback
handle_info({request_sent, Type, From, Trace, API}, #state{dict = Dict} = State) ->
    ?DEBUG("[W] request has been sent, type ~p, trace: ~p, api: ~p~n", [Type, Trace, API]),
    case Type of
        async -> gen_server:reply(From, ok);
        _     -> ok
    end,
    RS = #request_state{type = Type, from = From, api = API},
    {noreply, State#state{dict = dict:store(Trace, RS, Dict)}};
handle_info({request_failed, Type, From, Reason}, State) ->
    case Type of
        internal ->
            ?ERROR("[W] internal error: ~p~n", [Reason]);
        _ -> %% sync or async
            gen_server:reply(From, {error, Reason})
    end,
    {noreply, State};

%% handle response callback
handle_info({response_received, CorrId, Response}, #state{topic = Name, dict = Dict, partition = #partition{id = PartID}} = State) ->
    #request_state{type = Type, from = From} = dict:fetch(CorrId, Dict),
    case Response of
        #produce_response{} ->
            handle_produce_response(Type, From, Response),
            {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
        #fetch_response{} ->
            case handle_consume_response(Type, From, Response) of
                0 ->
                    {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
                Offset ->
                    handle_message_consumed(Name, PartID, Offset + 1),
                    {noreply, State#state{dict = dict:erase(CorrId, Dict), offset = Offset + 1}}
            end;
        _ ->
            {stop, error_unknown_response, State}
    end;
handle_info({response_error, CorrId}, #state{dict = Dict} = State) ->
    #request_state{type = Type, from = From} = dict:fetch(CorrId, Dict),
    case Type of
        internal ->
            ?ERROR("[W] internal error~n", []);
        sync ->
            gen_server:reply(From, {error, server_error});
        async ->
            {Pid, _} = From,
            erlang:send(Pid, {ekafka, error, server_error})
    end,
    {noreply, State#state{dict = dict:erase(CorrId, Dict)}};

%% Others
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
        State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
        Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect_to_lead_broker(Lead) ->
    case lists:keyfind(Lead, 1, ekafka_util:get_conf(brokers)) of
        false ->
            ?ERROR("[W] cannot found lead ~p broker in: ~p~n", [Lead, ekafka_util:get_conf(brokers)]),
            undefined;
        {Lead, {Host, Port}} ->
            case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
                {ok, Sock} ->
                    {ok, Sock};
                {error, Reason} ->
                    ?ERROR("[W] connect to broker ~p failed: ~p~n", [Host, Reason]),
                    undefined
            end
    end.

get_trace_id(Trace) ->
    case Trace of
        2147483647 -> 0;
        _          -> Trace + 1
    end.

handle_server_errors(Error) ->
    %%TODO: check error code when broker changed
    ?ERROR("[W] server error ~p [~p]~n", [Error, ekafka_util:get_error_message(Error)]).

query_response_api(Dict, CorrId) ->
    case dict:find(CorrId, Dict) of
        error ->
            -1;
        {ok, #request_state{api = API}} ->
            API
    end.

decode_response({M,F,A}, Worker, Data) ->
    case ekafka_protocol:decode_response({M,F,A}, Data) of
        {CorrId, undefined} ->
            erlang:send(Worker, {response_error, CorrId});
        {CorrId, Response} ->
            erlang:send(Worker, {response_received, CorrId, Response})
    end.

send_request(Sock, {Type, From, Worker, Trace, Request}) ->
    {API, Bin} = ekafka_protocol:encode_request(Trace, Worker, Request),
    case gen_tcp:send(Sock, Bin) of
        {error, Reason} ->
            ?ERROR("[W] ~p send to server failed, ~p~n", [Worker, Reason]),
            erlang:send(Worker, {request_failed, Type, From, Reason});
        ok ->
            erlang:send(Worker, {request_sent, Type, From, Trace, API})
    end.

handle_produce(Name, #partition{id = ID}, KVList) ->
    Messages =
        lists:foldr(fun({Key, Value}, L) ->
            Body = #message_body{key = Key, value = Value},
            [#message{offset = 0, body = Body} | L]
        end, [], KVList),
    Partition = #produce_req_partition{id = ID, message_set = #message_set{messages = Messages}},
    Topic = #produce_req_topic{name = Name, partitions = [Partition]},
    Acks =
        case ekafka_util:get_wait_all_servers() of
            true -> ?ACKS_WAIT_ALL;
            _    -> ?ACKS_WAIT_ONE
        end,
    #produce_request{acks = Acks, topics = [Topic]}.

handle_produce_response(Type, From, #produce_response{topics = [#produce_res_topic{name = Name, partitions = [Partition]}]}) ->
    #produce_res_partition{id = ID, error = Error, offset = Offset} = Partition,
    ?INFO("[W] got produce response, topic: ~p, partition: ~p, error: ~p, offset: ~p~n", [Name, ID, Error, Offset]),
    case Error of
        ?NO_ERROR ->
            case Type of
                sync ->
                    gen_server:reply(From, ok);
                async ->
                    %% TODO: for async response
                    ok
            end;
        _ ->
            ?ERROR("[W] produce server error, topic: ~p, partition: ~p, error: ~p~n", [Name, ID, Error]),
            case Type of
                sync ->
                    gen_server:reply(From, {error, Error});
                async ->
                    %% TODO: for async response
                    ok
            end
    end.

handle_consume(Type, Name, #partition{id = ID}, Offset) ->
    Partition = #fetch_req_partition{id = ID, offset = Offset, max_bytes = ekafka_util:get_max_message_size()},
    Topic = #fetch_req_topic{name = Name, partitions = [Partition]},
    BlockTime =
        case Type of
            sync -> 100;
            _    -> ekafka_util:get_fetch_max_timeout()
        end,
    #fetch_request{max_wait = BlockTime, topics = [Topic]}.

handle_consume_response(Type, {Pid, _} = From, #fetch_response{topics = Topics}) ->
    {Consumed, MsgList} =
        case Topics of
            [] ->
                {0, []};
            [#fetch_res_topic{partitions = Partitions}] ->
                case Partitions of
                    [] ->
                        {0, []};
                    [#fetch_res_partition{id = ID, error = Error, message_set = #message_set{messages = Messages}}] ->
                        case Error of
                            ?NO_ERROR ->
                                ?DEBUG("[W] message set received, ~p~n", [Messages]),
                                lists:foldr(fun(#message{offset = Offset, body = #message_body{key = Key, value = Value}}, {M1, L}) ->
                                    M2 =
                                        if
                                            Offset > M1 -> Offset;
                                            true        -> M1
                                        end,
                                    {M2, [{{ID, Offset}, {Key, Value}} | L]}
                                end, {0, []}, Messages);
                            _ ->
                                handle_server_errors(Error),
                                {0, []}
                        end
                end
        end,
    ?DEBUG("[W] messages received, ~p~n", [MsgList]),
    case Type of
        sync ->
            gen_server:reply(From, {ok, MsgList});
        async ->
            erlang:send(Pid, {ekafka, fetched, MsgList})
    end,
    Consumed.

sync_message_offset(Name, PartID) ->
    case gen_server:call(ekafka_util:get_topic_offset_mgr_name(Name), {get_partition_offset, PartID}) of
        {ok, Offset} ->
            Offset;
        _ ->
            -1
    end.

get_init_offset(Sock, Name, PartID) ->
    Time =
        case ekafka_util:get_conf(consume_from_beginning) of
            false -> ?REQ_LATEST_OFFSET;
            _     -> ?REQ_EARLIEST_OFFSET
        end,
    Topic = #offset_req_topic{name = Name, partitions = [#offset_req_partition{time = Time, id = PartID}]},
    Request = #offset_request{topics = [Topic]},
    case ekafka_util:send_to_server_sync(Sock, Request) of
        undefined ->
            0;
        #offset_response{topics = [#offset_res_topic{partitions = [#offset_res_partition{error = Error, offsets = Offsets}]}]} ->
            [Offset|_] = Offsets,
            case Error of
                ?NO_ERROR ->
                    ?INFO("[W] the begin offset of ~p:~p is ~p~n", [Name, PartID, Offset]),
                    Offset;
                _ ->
                    handle_server_errors(Error),
                    0
            end
    end.

handle_message_consumed(Topic, PartID, Offset) ->
    gen_server:cast(ekafka_util:get_topic_offset_mgr_name(Topic), {message_consumed, PartID, Offset}).
