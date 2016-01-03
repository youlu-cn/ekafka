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
                dict           :: dict(#request_state{}),
                offset}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Name :: string(), Partition :: #metadata_res_partition{}, Role :: producer | consumer) ->
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
            Request = handle_consume(Topic, Part, Offset),
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
handle_info(start_connection, #state{partition = #metadata_res_partition{leader = Lead}} = State) ->
    case lists:keyfind(Lead, 1, ekafka_util:get_conf(brokers)) of
        false ->
            {stop, broker_not_found, State};
        {Lead, {Host, Port}} ->
            case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
                {ok, Sock} ->
                    erlang:send_after(5000, self(), sync_offset),
                    {noreply, State#state{sock = Sock}};
                {error, Reason} ->
                    ?ERROR("[W] connect to broker ~p failed", [Host]),
                    {stop, Reason, State}
            end
    end;
handle_info(sync_offset, #state{topic = Name, partition = #partition{id = PartID}} = State) ->
    case sync_message_offset(Name, PartID) of
        {ok, undefined} ->
            erlang:send_after(5000, self(), sync_offset),
            {noreply, State};
        {ok, Offset} ->
            {noreply, State#state{offset = Offset}}
    end;
handle_info({tcp, Sock, Data}, #state{sock = Sock, dict = Dict} = State) ->
    erlang:spawn(?MODULE, decode_response, [{?MODULE, query_response_api, Dict}, self(), Data]),
    {noreply, State};
handle_info({request_sent, Type, From, Trace, API}, #state{dict = Dict} = State) ->
    RS = #request_state{type = Type, from = From, api = API},
    {noreply, State#state{dict = dict:store(Trace, RS, Dict)}};
handle_info({request_failed, Type, From, Reason}, State) ->
    case Type of
        internal ->
            ?ERROR("[W] internal error", []);
        sync ->
            gen_server:reply(From, {error, Reason});
        async ->
            erlang:send(From, {ekafka, error, Reason})
    end,
    {noreply, State};
handle_info({response_received, CorrId, Response}, #state{topic = Name, dict = Dict, partition = #partition{id = PartID}} = State) ->
    #request_state{type = Type, from = From} = dict:fetch(CorrId, Dict),
    case Response of
        #produce_response{} ->
            handle_produce_response(Type, From, Response),
            {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
        #fetch_response{} ->
            Consumed = handle_consume_response(Type, From, Response),
            handle_message_consumed(Name, PartID, Consumed),
            {noreply, State#state{dict = dict:erase(CorrId, Dict), offset = Consumed}};
        _ ->
            {stop, error_unknown_response, State}
    end;
handle_info({response_error, CorrId}, #state{dict = Dict} = State) ->
    #request_state{type = Type, from = From} = dict:fetch(CorrId, Dict),
    case Type of
        internal ->
            ?ERROR("[W] internal error", []);
        sync ->
            gen_server:reply(From, {error, server_error});
        async ->
            erlang:send(From, {ekafka, error, server_error})
    end,
    {noreply, State#state{dict = dict:erase(CorrId, Dict)}};
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
get_trace_id(Trace) ->
    case Trace of
        2147483647 -> 0;
        _          -> Trace + 1
    end.

handle_server_errors(Error) ->
    %%TODO: check error code when broker changed
    ?ERROR("[W] server error ~p [~p]", [Error, ekafka_util:get_error_message(Error)]).

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
            ?ERROR("[W] ~p send to server failed, ~p", [Worker, Reason]),
            erlang:send(Worker, {request_failed, Type, From, Reason});
        ok ->
            erlang:send(Worker, {request_sent, Type, From, Trace, API})
    end.

handle_produce(Name, #metadata_res_partition{id = ID}, KVList) ->
    Messages =
        lists:foldr(fun({Key, Value}, L) ->
            Body = #message_body{key = Key, value = Value},
            [#message{offset = 0, body = Body} | L]
        end, [], KVList),
    Partition = #produce_req_partition{id = ID, message_set = #message_set{messages = Messages}},
    Topic = #produce_req_topic{name = Name, partitions = [Partition]},
    #produce_request{topics = [Topic]}.

handle_produce_response(Type, From, _Response) ->
    case Type of
        sync ->
            gen_server:reply(From, ok);
        async ->
            ok
    end.

handle_consume(Name, #metadata_res_partition{id = ID}, Offset) ->
    Partition = #fetch_req_partition{id = ID, offset = Offset, max_bytes = ekafka_util:get_max_message_size()},
    Topic = #fetch_req_topic{name = Name, partitions = [Partition]},
    #fetch_request{topics = [Topic]}.

handle_consume_response(Type, From, #fetch_response{topics = Topics}) ->
    {Consumed, MsgList} =
        case Topics of
            [] ->
                {0, []};
            [#fetch_res_topic{partitions = Partitions}] ->
                case Partitions of
                    [] ->
                        {0, []};
                    [#fetch_res_partition{id = ID, error = Error, message_set = Messages}] ->
                        case Error of
                            ?NO_ERROR ->
                                lists:foldr(fun(#message{offset = Offset, body = #message_body{key = Key, value = Value}}, {M, L}) ->
                                    Max =
                                        if
                                            Offset > M -> Offset;
                                            true       -> M
                                        end,
                                    [{{ID, Offset}, {Key, Value}} | L]
                                end, {Max, []}, Messages);
                            _ ->
                                handle_server_errors(Error),
                                {0, []}
                        end
                end
        end,
    case Type of
        sync ->
            gen_server:reply(From, {ok, MsgList});
        async ->
            erlang:send(From, {ekafka, fetched, MsgList})
    end,
    Consumed.

sync_message_offset(Topic, PartID) ->
    gen_server:call(ekafka_util:get_topic_offset_mgr_name(Topic), {get_partition_offset, PartID}).

handle_message_consumed(Topic, PartID, Offset) ->
    gen_server:cast(ekafka_util:get_topic_offset_mgr_name(Topic), {message_consumed, PartID, Offset}).
