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
            Request = handle_produce_request(Topic, Part, KVList),
            %?DEBUG("[W] produce request data: ~p~n", [Request]),
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
            Request = handle_consume_request(Type, Topic, Part, Offset),
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
handle_cast({kafka_broker_down, PartitionList}, #state{partition = Partition, sock = Sock} = State) ->
    ?DEBUG("[W] handle kafka broker down message, brokers ~p~n", [PartitionList]),
    #partition{id = PartID, lead = Lead} = Partition,
    case lists:keyfind(PartID, 2, PartitionList) of
        false ->
            {noreply, State};
        #partition{lead = Lead} ->
            ?DEBUG("[W] lead broker not changed??~n", []),
            {noreply, State};
        NewPartition ->
            ?INFO("[W] the broker of partition ~p changed~n", [PartID]),
            gen_tcp:close(Sock),
            erlang:send(self(), start_connection),
            {noreply, State#state{partition = NewPartition}}
    end;
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
handle_info(start_connection, #state{topic = Name, partition = #partition{id = ID, host = Host, port = Port}} = State) ->
    ?INFO("[W] start worker connection, topic: ~p, partition: ~p~n", [Name, ID]),
    case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
        {ok, Sock} ->
            erlang:send(self(), init_begin_offset),
            {noreply, State#state{sock = Sock}};
        {error, econnrefused} ->
            gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down),
            {noreply, State};
        {error, Error} ->
            ?ERROR("[W] connect to broker ~p:~p failed: ~p~n", [Host, Port, Error]),
            {stop, Error, State}
    end;
handle_info(init_begin_offset, #state{sock = Sock, topic = Name, role = Role, partition = #partition{id = PartID}} = State) ->
    case Role of
        producer ->
            handle_worker_status_changed(Name, PartID, up),
            {noreply, State};
        consumer ->
            Offset = get_init_offset(Sock, Name, PartID),
            erlang:send_after(5000, self(), sync_group_offset),
            {noreply, State#state{offset = Offset}}
    end;
handle_info(sync_group_offset, #state{topic = Name, partition = #partition{id = PartID}, offset = Offset} = State) ->
    case gen_server:call(ekafka_util:get_topic_offset_mgr_name(Name), {get_partition_offset, PartID}) of
        {ok, GroupOffset} when GroupOffset > Offset ->
            ?INFO("[W] got group consume offset, ~p:~p, value: ~p~n", [Name, PartID, GroupOffset]),
            handle_worker_status_changed(Name, PartID, up),
            {noreply, State#state{offset = GroupOffset}};
        _ ->
            ?DEBUG("[W] no group consume offset for ~p:~p, use local: ~p~n", [Name, PartID, Offset]),
            handle_worker_status_changed(Name, PartID, up),
            {noreply, State}
    end;

%% handle socket
handle_info({tcp, Sock, Data}, #state{sock = Sock, dict = Dict} = State) ->
    %?DEBUG("[W] tcp data received, ~p, ~p~n", [self(), Data]),
    erlang:spawn(?MODULE, decode_response, [{?MODULE, query_response_api, Dict}, self(), Data]),
    {noreply, State};
handle_info({re_connect, Reason, Times}, #state{topic = Name, partition = #partition{id = ID, host = Host, port = Port}} = State) ->
    ?WARNING("[W] ~p reconnecting to broker, reason: ~p, topic: ~p, partition: ~p~n", [Times, Reason, Name, ID]),
    case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
        {ok, Sock} ->
            ?INFO("[W] broker reconnected, ~p:~p~n", [Name, ID]),
            case Times of
                1 -> ok;
                _ -> handle_worker_status_changed(Name, ID, up)
            end,
            {noreply, State#state{sock = Sock}};
        {error, econnrefused} ->
            handle_worker_status_changed(Name, ID, down),
            gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down),
            {noreply, State};
        {error, Error} ->
            ?ERROR("[W] reconnect failed ~p, retried times ~p~n", [Error, Times]),
            erlang:send_after(5000, self(), {re_connect, Reason, Times + 1}),
            handle_worker_status_changed(Name, ID, down),
            {noreply, State}
    end;
handle_info({tcp_closed, Sock}, State) ->
    ?WARNING("[W] server closed connection, reconnect later~n", []),
    gen_tcp:close(Sock),
    erlang:send(self(), {re_connect, tcp_closed, 1}),
    {noreply, State};
handle_info({tcp_error, Sock, Reason}, #state{topic = Name, partition = #partition{id = PartID}} = State) ->
    ?ERROR("[W] tcp error occurs, ~p, reconnect later~n", [Reason]),
    gen_tcp:close(Sock),
    handle_worker_status_changed(Name, PartID, down),
    erlang:send_after(1000, self(), {re_connect, tcp_error, 2}),
    {noreply, State};

%% handle request callback
handle_info({request_sent, Type, From, Trace, API}, #state{dict = Dict} = State) ->
    ?DEBUG("[W] request has been sent, type ~p, trace: ~p, api: ~p~n", [Type, Trace, API]),
    case Type of
        async -> gen_server:reply(From, ok);
        _     -> ok
    end,
    Record = #request_state{type = Type, from = From, api = API},
    {noreply, State#state{dict = dict:store(Trace, Record, Dict)}};
handle_info({request_failed, _Type, From, Reason}, State) ->
    gen_server:reply(From, {error, Reason}),
    {noreply, State};

%% handle response callback
handle_info({response_received, CorrId, Response}, #state{topic = Name, dict = Dict, partition = #partition{id = PartID}} = State) ->
    #request_state{type = Type, from = From, api = API} = dict:fetch(CorrId, Dict),
    case API of
        ?PRODUCE_REQUEST ->
            handle_produce_response(Type, From, Response),
            {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
        ?FETCH_REQUEST ->
            case handle_consume_response(Type, From, Response) of
                0 ->
                    {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
                Offset ->
                    gen_server:cast(ekafka_util:get_topic_offset_mgr_name(Name), {message_consumed, PartID, Offset}),
                    {noreply, State#state{dict = dict:erase(CorrId, Dict), offset = Offset + 1}}
            end;
        _ ->
            {stop, error_unknown_response, State}
    end;
handle_info({response_error, CorrId}, #state{dict = Dict} = State) ->
    #request_state{type = Type, from = From} = dict:fetch(CorrId, Dict),
    case Type of
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
handle_worker_error(Name, Error) ->
    case Error of
        E when E =:= ?BROKER_NOT_AVAILABLE; E =:= ?NOT_LEADER_FOR_PARTITION ->
            ?ERROR("[W] broker changed, cast message to manager~n", []),
            gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down);
        _ ->
            ok
    end.

handle_worker_status_changed(Topic, PartID, Up_Or_Down) ->
    case Up_Or_Down of
        up ->
            gen_server:cast(ekafka_util:get_topic_manager_name(Topic), {worker_started, PartID, self()});
        down ->
            gen_server:cast(ekafka_util:get_topic_manager_name(Topic), {worker_interrupted, PartID, self()})
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
                    ?ERROR("[W] get topic ~p:~p initial offset failed ~p~n", [Name, PartID, Error]),
                    0
            end
    end.

%% handle requests
handle_produce_request(Name, #partition{id = ID}, KVList) ->
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

handle_consume_request(Type, Name, #partition{id = ID}, Offset) ->
    Partition = #fetch_req_partition{id = ID, offset = Offset, max_bytes = ekafka_util:get_max_message_size()},
    Topic = #fetch_req_topic{name = Name, partitions = [Partition]},
    BlockTime =
        case Type of
            sync -> 100;
            _    -> ekafka_util:get_fetch_max_timeout()
        end,
    #fetch_request{max_wait = BlockTime, topics = [Topic]}.

%% handle response
handle_produce_response(Type, {Pid, _} = From, #produce_response{topics = [#produce_res_topic{name = Name, partitions = [Partition]}]}) ->
    #produce_res_partition{id = ID, error = Error, offset = Offset} = Partition,
    ?INFO("[W] got produce response, topic: ~p, partition: ~p, error: ~p, offset: ~p~n", [Name, ID, Error, Offset]),
    case Error of
        ?NO_ERROR ->
            case Type of
                sync ->
                    gen_server:reply(From, ok);
                async ->
                    ok
            end;
        _ ->
            ?ERROR("[W] produce server error, topic: ~p, partition: ~p, error: ~p~n", [Name, ID, Error]),
            handle_worker_error(Name, Error),
            case Type of
                sync ->
                    gen_server:reply(From, {error, Error});
                async ->
                    erlang:send(Pid, {ekafka, error, error_produce_error})
            end
    end.

handle_consume_response(Type, {Pid, _} = From, #fetch_response{topics = Topics}) ->
    {Consumed, MsgList} =
        case Topics of
            [] ->
                {0, []};
            [#fetch_res_topic{name = Name, partitions = Partitions}] ->
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
                                handle_worker_error(Name, Error),
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


%% encode and decode data
send_request(Sock, {Type, From, Worker, Trace, Request}) ->
    {API, Bin} = ekafka_protocol:encode_request(Trace, Worker, Request),
    case gen_tcp:send(Sock, Bin) of
        {error, Reason} ->
            ?ERROR("[W] ~p send to server failed, ~p~n", [Worker, Reason]),
            erlang:send(Worker, {request_failed, Type, From, Reason});
        ok ->
            erlang:send(Worker, {request_sent, Type, From, Trace, API})
    end.

decode_response({M,F,A}, Worker, Data) ->
    case ekafka_protocol:decode_response({M,F,A}, Data) of
        {CorrId, undefined} ->
            ?ERROR("[W] decode response failed, ~p~n", [Data]),
            erlang:send(Worker, {response_error, CorrId});
        {CorrId, Response} ->
            erlang:send(Worker, {response_received, CorrId, Response})
    end.


%% Util functions
get_trace_id(Trace) ->
    case Trace of
        2147483647 -> 0;
        _          -> Trace + 1
    end.

query_response_api(Dict, CorrId) ->
    case dict:find(CorrId, Dict) of
        error ->
            -1;
        {ok, #request_state{api = API}} ->
            API
    end.
