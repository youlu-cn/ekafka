%%%-------------------------------------------------------------------
%%% @author yuriy
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 一月 2016 下午2:50
%%%-------------------------------------------------------------------
-module(ekafka_offset_mgr).
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

-record(state, {topic          :: string(),
                group          :: string(),
                partitions     :: [#partition{}],
                offsets   = [] :: [{int32(), int64()}],
                hosts          :: {string(), integer()},  %% only for Kafka offset commit
                port           :: port() | pid()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Topic :: string(), Group :: string(), Partitions :: list(#partition{})) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Topic, Group, Partitions) ->
    gen_server:start_link({local, ekafka_util:get_topic_offset_mgr_name(Topic)}, ?MODULE, {Topic, Group, Partitions}, []).

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
init({Topic, Group, Partitions}) ->
    erlang:send(self(), start_connection),
    {ok, #state{topic = Topic, group = Group, partitions = Partitions}}.

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
handle_call({get_partition_offset, PartID}, _From, #state{topic = Name, offsets = Offsets} = State) ->
    Offset =
        case lists:keyfind(PartID, 1, Offsets) of
            false ->
                -1;
            {PartID, R} ->
                R
        end,
    ?DEBUG("[O] get partition offset ~p:~p, offset: ~p~n", [Name, PartID, Offset]),
    {reply, {ok, Offset}, State};
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
handle_cast({message_consumed, PartID, Offset}, #state{topic = Name, offsets = Offsets} = State) ->
    case lists:keyfind(PartID, 1, Offsets) of
        false ->
            ?ERROR("[O] invalid partition id: ~p~n", [PartID]),
            {noreply, State};
        _ ->
            NewOffsets = lists:keyreplace(PartID, 1, Offsets, {PartID, Offset}),
            ?DEBUG("[O] message consumed ~p:~p, old: ~p, new: ~p~n", [Name, PartID, Offsets, NewOffsets]),
            {noreply, State#state{offsets = NewOffsets}}
    end;
handle_cast({kafka_broker_down, _PartitionList}, #state{group = Group} = State) ->
    ?DEBUG("[O] handle kafka broker down message~n", []),
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            case start_kafka_offset_connection(Group) of
                undefined ->
                    {noreply, State};
                {Sock, Hosts} ->
                    ?DEBUG("[O] offset manager reconnected to ~p~n", [Hosts]),
                    {noreply, State#state{port = Sock, hosts = Hosts}}
            end;
        _ ->
            {noreply, State}
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
handle_info(start_connection, #state{group = Group} = State) ->
    case start_offset_connection(Group) of
        undefined ->
            {stop, error_group_coordinator, State};
        {Sock, Hosts} ->
            ?DEBUG("[O] group coordinator ~p connected~n", [Hosts]),
            erlang:send(self(), sync_consume_offset),
            {noreply, State#state{port = Sock, hosts = Hosts}};
        ZkPid ->
            ?DEBUG("[O] zookeeper connected~n", []),
            erlang:send(self(), sync_consume_offset),
            {noreply, State#state{port = ZkPid}}
    end;
handle_info(sync_consume_offset, #state{group = Group, topic = Name, partitions = Partitions, port = Port} = State) ->
    NewState =
        case sync_consumed_offset(Port, Group, Name, Partitions) of
            undefined ->
                State;
            [] ->
                State;
            NewPartitions ->
                Offsets =
                    lists:foldl(fun(#partition{id = ID, offset = Offset}, L) ->
                        [{ID, Offset} | L]
                    end, [], NewPartitions),
                State#state{offsets = Offsets, partitions = NewPartitions}
        end,
    erlang:send_after(ekafka_util:get_offset_auto_commit_timeout(), self(), auto_commit_offset),
    {noreply, NewState};
handle_info(auto_commit_offset, #state{group = Group, topic = Name, partitions = Partitions, offsets = Offsets, port = Port} = State) ->
    NewPartitions =
        lists:foldl(fun(#partition{id = PartID, offset = Offset} = Partition, L) ->
            case lists:keyfind(PartID, 1, Offsets) of
                false ->
                    ?DEBUG("[O] no message consumed for ~p:~p~n", [Name, PartID]),
                    [Partition | L];
                {PartID, Offset} ->
                    ?DEBUG("[O] offset not changed ~p:~p, ~p~n", [Name, PartID, Offsets]),
                    [Partition | L];
                {PartID, NewOffset} ->
                    ?DEBUG("[O] offset changed ~p:~p, old: ~p, new: ~p~n", [Name, PartID, Offset, NewOffset]),
                    commit_offset(Port, Group, Name, PartID, NewOffset),
                    [Partition#partition{offset = NewOffset} | L]
            end
        end, [], Partitions),
    erlang:send_after(ekafka_util:get_offset_auto_commit_timeout(), self(), auto_commit_offset),
    {noreply, State#state{partitions = NewPartitions}};

%% Kafka socket callback
handle_info({re_connect, Reason, Times}, #state{topic = Name, group = Group, hosts = {Host, Port}} = State) ->
    ?WARNING("[O] ~p reconnecting to broker, reason: ~p, topic: ~p, group: ~p~n", [Times, Reason, Name, Group]),
    case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
        {ok, Sock} ->
            ?INFO("[O] broker reconnected, ~p:~p~n", [Group, Name]),
            {noreply, State#state{port = Sock}};
        {error, econnrefused} ->
            gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down),
            {noreply, State};
        {error, Error} ->
            ?ERROR("[O] reconnect failed ~p, retried times ~p~n", [Error, Times]),
            erlang:send_after(5000, self(), {re_connect, Reason, Times + 1}),
            {noreply, State}
    end;
handle_info({tcp_closed, Sock}, State) ->
    ?WARNING("[O] server closed connection, reconnect later~n", []),
    gen_tcp:close(Sock),
    erlang:send(self(), {re_connect, tcp_closed, 1}),
    {noreply, State};
handle_info({tcp_error, Sock, Reason}, #state{topic = Name, group = Group} = State) ->
    ?ERROR("[O] ~p:~p connection error occurs, ~p, reconnect later~n", [Group, Name, Reason]),
    gen_tcp:close(Sock),
    erlang:send_after(1000, self(), {re_connect, tcp_error, 2}),
    {noreply, State};

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
terminate(_Reason, #state{port = Port}) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            gen_tcp:close(Port);
        _ ->
            ezk:end_connection(Port, shutdown)
    end,
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

%%
start_offset_connection(Group) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            start_kafka_offset_connection(Group);
        ZkConf ->
            start_zookeeper_offset_connection(ZkConf)
    end.

sync_consumed_offset(Port, Group, Topic, Partitions) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            sync_consumed_offset_from_kafka(Port, Group, Topic, Partitions);
        _ ->
            sync_consumed_offset_from_zookeeper(Port, Group, Topic, Partitions)
    end.

commit_offset(Port, Group, Name, PartID, Offset) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            commit_offset_to_kafka(Port, Group, Name, PartID, Offset);
        _ ->
            commit_offset_to_zookeeper(Port, Group, Name, PartID, Offset)
    end.


%% Kafka Offset Management
start_kafka_offset_connection(Group) ->
    case request_group_coordinator(ekafka_util:get_conf(brokers), Group) of
        undefined ->
            undefined;
        #group_coordinator_response{error = Error, host = Host, port = Port} ->
            ?INFO("[O] group coordinator res: ~p, hosts: ~p~n", [Error, {Host, Port}]),
            case Error of
                ?NO_ERROR ->
                    case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
                        {error, Reason} ->
                            ?ERROR("[O] connect to group coordinator ~p error: ~p~n", [Host, Reason]),
                            undefined;
                        {ok, Sock} ->
                            {Sock, {Host, Port}}
                    end;
                _ ->
                    ?ERROR("[O] group coordinator response error: ~p~n", [ekafka_util:get_error_message(Error)]),
                    undefined
            end
    end.

sync_consumed_offset_from_kafka(Port, Group, Topic, Partitions) ->
    Request = request_fetch_offset(Group, Topic, Partitions),
    case ekafka_util:send_to_server_sync(Port, Request) of
        undefined ->
            ?ERROR("[O] fetch consumer offset error for group: ~p, topic: ~p~n", [Group, Topic]),
            undefined;
        #offset_fetch_response{topics = [#offset_fetch_res_topic{partitions = PartOffsets}]} ->
            NewPartitions =
                lists:foldl(fun(#offset_fetch_res_partition{id = ID, offset = Offset}, L) ->
                    Partition = lists:keyfind(ID, 2, Partitions),
                    [Partition#partition{offset = Offset} | L]
                end, [], PartOffsets),
            ?DEBUG("[O] sync offset over, topic: ~p, partitions: ~p~n", [Topic, NewPartitions]),
            NewPartitions
    end.

commit_offset_to_kafka(Port, Group, Name, PartID, Offset) ->
    Partition = #offset_commit_req_partition{id = PartID, offset = Offset},
    Topic = #offset_commit_req_topic{name = Name, partitions = [Partition]},
    Request = #offset_commit_request{group_id = Group, topics = [Topic]},
    case ekafka_util:send_to_server_sync(Port, Request) of
        undefined ->
            ?WARNING("[O] commit offset failed, ~p:~p, group: ~p~n", [Name, PartID, Group]),
            gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down);
        #offset_commit_response{} ->
            ?DEBUG("[O] offset committed ~p:~p, group: ~p, offset: ~p~n", [Name, PartID, Group, Offset])
    end.

request_group_coordinator([], _Group) ->
    ?ERROR("[O] all brokers are not available~n", []),
    undefined;
request_group_coordinator([{_,{IP, Port}} | Others], Group) ->
    case gen_tcp:connect(IP, Port, ekafka_util:get_tcp_options()) of
        {error, econnrefused} ->
            request_group_coordinator(Others, Group);
        {error, Reason} ->
            ?ERROR("[O] connect to broker ~p error: ~p~n", [IP, Reason]),
            undefined;
        {ok, Sock} ->
            Request = #group_coordinator_request{id = Group},
            Res = ekafka_util:send_to_server_sync(Sock, Request),
            gen_tcp:close(Sock),
            Res
    end.

request_fetch_offset(Group, Topic, Parts) ->
    Partitions =
        lists:foldr(fun(#partition{id = ID}, L) ->
            [ID | L]
        end, [], Parts),
    #offset_fetch_request{group_id = Group, topics = [#offset_fetch_req_topic{name = Topic, partitions = Partitions}]}.


%% zookeeper Offset Management
start_zookeeper_offset_connection(ZkConf) ->
    {ok, Pid} = ezk:start_connection(ZkConf),
    ?DEBUG("[O] zookeeper connected ~p~n", [Pid]),
    Pid.

sync_consumed_offset_from_zookeeper(Port, Group, Topic, Partitions) ->
    NewPartitions =
        lists:foldl(fun(#partition{id = PartID} = Partition, L) ->
            Path = lists:concat(["/consumers/", Group, "/offsets/", Topic, "/", PartID]),
            case ezk:get(Port, lists:flatten(Path)) of
                {ok, {Offset, _}} ->
                    [Partition#partition{offset = ekafka_util:to_integer(Offset)} | L];
                _ ->
                    [Partition | L]
            end
        end, [], Partitions),
    ?DEBUG("[O] sync offset over, topic: ~p, partitions: ~p~n", [Topic, NewPartitions]),
    NewPartitions.

commit_offset_to_zookeeper(Port, Group, Name, PartID, Offset) ->
    Path = lists:concat(["/consumers/", Group, "/offsets/", Name, "/", PartID]),
    case ezk:set(Port, lists:flatten(Path), ekafka_util:to_binary(Offset)) of
        {ok, _} ->
            ?DEBUG("[O] offset commited ~p:~p, offset: ~p~n", [Name, PartID, Offset]);
        {error, no_dir} ->
            ?DEBUG("[O] create node for group: ~p, topic: ~p, partition: ~p, offset: ~p~n", [Group, Name, PartID, Offset]),
            ezk:create(Port, lists:flatten(Path), ekafka_util:to_binary(Offset));
        Error ->
            ?ERROR("[O] commit offset failed ~p, ~p:~p~n", [Error, Name, PartID])
    end.
