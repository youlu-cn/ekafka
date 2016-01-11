%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 18:41
%%%-------------------------------------------------------------------
-module(ekafka_manager).
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

-record(state, {topic         :: string(),
                partitions    :: [#partition{}],
                role          :: atom(),
                group         :: string(),
                sup           :: pid(),
                monitors = [] :: [pid()],
                workers  = [] :: [{integer(), [pid()]}]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Topic :: string(), Role :: atom(), Group :: string()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Topic, Role, Group) ->
    gen_server:start_link({local, ekafka_util:get_topic_manager_name(Topic)}, ?MODULE, {Topic, Role, Group}, []).

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
init({Topic, Role, Group}) ->
    erlang:send(self(), start_worker_sup),
    {ok, #state{topic = Topic, role = Role, group = Group}}.

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
handle_call({pick_worker, Arg}, _From, #state{role = Role, workers = Workers} = State) ->
    ?DEBUG("[MGR] all workers list: ~p~n", [Workers]),
    PartID =
        case Role of
            producer ->
                case ekafka_util:get_partition_assignment() of
                    true -> erlang:phash2(Arg, erlang:length(Workers));
                    _    -> undefined
                end;
            consumer ->
                Arg
        end,
    case handle_pick_worker(PartID, Workers) of
        {undefined, _} ->
            {reply, {error, workers_not_ready}, State};
        {Pid, NewWorkers} ->
            {reply, {ok, Pid}, State#state{workers = NewWorkers}}
    end;
handle_call(get_partition_list, _From, #state{partitions = Partitions} = State) ->
    IDList =
        lists:foldl(fun(#partition{id = ID}, L) ->
            [ID | L]
        end, [], Partitions),
    {reply, {ok, IDList}, State};
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
%% worker started, add to workers list
handle_cast({worker_started, PartID, Pid}, #state{workers = Workers} = State) ->
    %?DEBUG("[MGR] worker ~p for partition ~p started, workers list: ~p~n", [Pid, PartID, Workers]),
    case lists:keyfind(PartID, 1, Workers) of
        false ->
            {noreply, State#state{workers = [{PartID, [Pid]} | Workers]}};
        {PartID, PidList} ->
            Partition = {PartID, [Pid | PidList]},
            {noreply, State#state{workers = lists:keyreplace(PartID, 1, Workers, Partition)}}
    end;
handle_cast({worker_interrupted, PartID, Pid}, #state{workers = Workers} = State) ->
    %?DEBUG("[MGR] worker ~p for partition ~p down, workers list: ~p~n", [Pid, PartID, Workers]),
    case lists:keyfind(PartID, 1, Workers) of
        false ->
            {noreply, State};
        {PartID, PidList} ->
            Partition = {PartID, [P || P <- PidList, P =/= Pid]},
            {noreply, State#state{workers = lists:keyreplace(PartID, 1, Workers, Partition)}}
    end;

%% kafka broker down
handle_cast(kafka_broker_down, #state{topic = Name, monitors = PidList, partitions = OldPartitions, workers = Workers} = State) ->
    ?WARNING("[MGR] kafka broker down, re-get metadata for topic ~p~n", [Name]),
    case get_topic_metadata(Name) of
        undefined ->
            ?ERROR("[MGR] this should not occur, wait for next down message to retry~n", []),
            {noreply, State};
        {error, Error} ->
            ?ERROR("[MGR] get topic ~p metadata failed ~p~n", [Name, Error]),
            {stop, Error, State};
        Partitions ->
            ?INFO("[MGR] got topic ~p metadata, partitions: ~p~n", [Name, Partitions]),
            case check_which_broker_down(OldPartitions, Partitions) of
                [] ->
                    {noreply, State};
                PartitionList ->
                    NewWorkers =
                        lists:foldl(fun(#partition{id = PartID}, L) ->
                            lists:keydelete(PartID, 1, L)
                        end, Workers, PartitionList),
                    ?DEBUG("[MGR] cast message to all workers ~p~n", [PidList]),
                    lists:foreach(fun(Pid) ->
                        gen_server:cast(Pid, {kafka_broker_down, PartitionList})
                    end, PidList),
                    {noreply, State#state{partitions = Partitions, workers = NewWorkers}}
            end
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
handle_info(start_worker_sup, #state{topic = Name} = State) ->
    ?DEBUG("[MGR] starting topic ~p worker supervisor~n", [Name]),
    {ok, Pid} = ekafka_topic_sup:start_worker_sup(Name),
    erlang:send(self(), get_topic_metadata),
    {noreply, State#state{sup = Pid}};
handle_info(get_topic_metadata, #state{topic = Name} = State) ->
    ?DEBUG("[MGR] get_topic_metadata() for topic ~p~n", [Name]),
    case get_topic_metadata(Name) of
        undefined ->
            ?WARNING("[MGR] partition leader is in election, retry 5 secs later~n", []),
            erlang:send_after(5000, self(), get_topic_metadata),
            {noreply, State};
        {error, Error} ->
            ?ERROR("[MGR] get topic ~p metadata failed ~p~n", [Name, Error]),
            {stop, Error, State};
        Partitions ->
            ?INFO("[MGR] got topic ~p metadata, partitions: ~p~n", [Name, Partitions]),
            erlang:send(self(), start_offset_mgr),
            {noreply, State#state{partitions = Partitions}}
    end;
handle_info(start_offset_mgr, #state{topic = Name, group = Group, partitions = Partitions, role = Role} = State) ->
    case Role of
        consumer ->
            ekafka_topic_sup:start_offset_manager(Name, Group, Partitions);
        _ ->
            ok
    end,
    erlang:send(self(), start_workers),
    {noreply, State};
handle_info(start_workers, #state{sup = Sup, topic = Name, partitions = Partitions, role = Role} = State) ->
    Max = ekafka_util:get_worker_process_count(Role),
    ?INFO("[MGR] starting workers for topic ~p, count: ~p, role: ~p~n", [Name, Max, Role]),
    PidList =
        lists:foldl(fun(Partition, L1) ->
            L =
                lists:foldl(fun(_, L2) ->
                    {ok, Pid} = supervisor:start_child(Sup, [Name, Partition, Role]),
                    _Ref = erlang:monitor(process, Pid),
                    [Pid | L2]
                end, [], lists:seq(1, Max)),
            L1 ++ L
        end, [], Partitions),
    ?DEBUG("[MGR] worker pids ~p~n", [PidList]),
    {noreply, State#state{monitors = PidList}};
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

%% @return
%%  {Pid, NewWorkers}
handle_pick_worker(_, []) ->
    {undefined, []};
handle_pick_worker(undefined, [{PartID, []} | OtherParts]) ->
    case pick_worker_from(OtherParts, [{PartID, []}]) of
        {Pid, NewWorkers} ->
            {Pid, NewWorkers};
        _ ->
            {undefined, []}
    end;
handle_pick_worker(undefined, [{PartID, [Pid | Left]} | OtherParts]) ->
    Partition = {PartID, Left ++ [Pid]},
    NewWorkers = OtherParts ++ [Partition],
    {Pid, NewWorkers};
handle_pick_worker(PartID, Workers) ->
    case lists:keyfind(PartID, 1, Workers) of
        false ->
            {undefined, []};
        {PartID, []} ->
            {undefined, []};
        {PartID, [Pid | Left]} ->
            Partition = {PartID, Left ++ [Pid]},
            NewWorkers = lists:keyreplace(PartID, 1, Workers, Partition),
            {Pid, NewWorkers}
    end.

pick_worker_from([], L) ->
    L;
pick_worker_from([{PartID, []} | OtherParts], L) ->
    pick_worker_from(OtherParts, L ++ [{PartID, []}]);
pick_worker_from([{PartID, [Pid | Left]} | OtherParts], L) ->
    NewWorkers = L ++ OtherParts ++ [{PartID, Left ++ [Pid]}],
    {Pid, NewWorkers}.

get_leader_hosts(Lead) ->
    case lists:keyfind(Lead, 1, ekafka_util:get_conf(brokers)) of
        false ->
            ?ERROR("[MGR] cannot found lead ~p broker in: ~p~n", [Lead, ekafka_util:get_conf(brokers)]),
            undefined;
        {Lead, Hosts} ->
            Hosts
    end.

check_which_broker_down(OldPartitions, NewPartitions) ->
    ?DEBUG("[MGR] kafka server changed, old partitions: ~p, new: ~p~n", [OldPartitions, NewPartitions]),
    lists:foldl(fun(#partition{id = PartID, lead = Lead}, L) ->
        case lists:keyfind(PartID, 2, NewPartitions) of
            false ->
                L;
            #partition{lead = Lead} ->
                L;
            #partition{lead = NewLead, host = NewHost} = Partition ->
                ?WARNING("[MGR] broker ~p down, use ~p, host: ~p~n", [Lead, NewLead, NewHost]),
                [Partition | L]
        end
    end, [], OldPartitions).

%% @return
%%  {error, Reason} - Fail
%%  undefined - creating topic, retry later
%%  [#partition{}]
get_topic_metadata(Name) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            get_topic_metadata_by_kafka(Name);
        ZkConf ->
            get_topic_metadata_by_zookeeper(ZkConf, Name)
    end.

%% Topic metadata from kafka
get_topic_metadata_by_kafka(Name) ->
    case request_topic_metadata(ekafka_util:get_conf(brokers), Name) of
        undefined ->
            {error, error_socket_error};
        #metadata_response{topics = [#metadata_res_topic{error = Err1, partitions = Partitions}], brokers = Brokers} ->
            set_brokers(Brokers),
            case Err1 of
                ?NO_ERROR ->
                    Err3 =
                        lists:foldl(fun(#metadata_res_partition{error = Err2}, Err) ->
                            case {Err2, Err} of
                                {?NO_ERROR, ?NO_ERROR}     -> ?NO_ERROR;
                                {?LEADER_NOT_AVAILABLE, _} -> ?LEADER_NOT_AVAILABLE;
                                _                          -> Err2
                            end
                        end, ?NO_ERROR, Partitions),
                    case Err3 of
                        E when E =:= ?NO_ERROR; E =:= ?REPLICA_NOT_AVAILABLE ->
                            to_partitions(Partitions);
                        ?LEADER_NOT_AVAILABLE ->
                            ?INFO("[MGR] topic ~p is creating, try later~n", [Name]),
                            undefined;
                        _ ->
                            ?ERROR("[MGR] Kafka response error: ~p~n", [ekafka_util:get_error_message(Err3)]),
                            {error, Err3}
                    end;
                _ ->
                    ?ERROR("[MGR] Kafka response topic error: ~p~n", [ekafka_util:get_error_message(Err1)]),
                    {error, Err1}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

request_topic_metadata([], _Name) ->
    ?ERROR("[O] all brokers are not available~n", []),
    {error, cannot_connect};
request_topic_metadata([{_,{IP, Port}} | Others], Name) ->
    case gen_tcp:connect(IP, Port, ekafka_util:get_tcp_options()) of
        {error, econnrefused} ->
            request_topic_metadata(Others, Name);
        {error, Reason} ->
            ?ERROR("[MGR] connect to broker ~p error: ~p~n", [IP, Reason]),
            {error, Reason};
        {ok, Sock} ->
            Request = #metadata_request{topics = [Name]},
            Res = ekafka_util:send_to_server_sync(Sock, Request),
            gen_tcp:close(Sock),
            Res
    end.

set_brokers(BrokerList) ->
    Brokers =
        lists:foldr(fun(#metadata_res_broker{id = ID, host = Host, port = Port}, L) ->
            [{ID, {Host, Port}} | L]
        end, [], BrokerList),
    ekafka_util:set_conf(brokers, Brokers).

to_partition(#metadata_res_partition{id = ID, leader = Lead, isr = Isr}) ->
    {Host, Port} = get_leader_hosts(Lead),
    #partition{id = ID, lead = Lead, isr = Isr, host = Host, port = Port}.
to_partitions([#metadata_res_partition{}|_] = List) ->
    lists:foldr(fun(Partition, L) ->
        [to_partition(Partition) | L]
    end, [], List).


%% Topic metadata from zookeeper
get_topic_metadata_by_zookeeper(ZkConf, Name) ->
    {ok, Pid} = ezk:start_connection(ZkConf),
    Response =
        case get_partition_list(Pid, Name) of
            undefined ->
                ?INFO("[MGR] topic ~p not created, creating...~n", [Name]),
                %% request topic metadata can create the topic
                case request_topic_metadata(ekafka_util:get_conf(brokers), Name) of
                    undefined ->
                        ?INFO("[MGR] failed to create topic ~p~n", [Name]),
                        {error, error_create_topic_failed};
                    _ ->
                        undefined
                end;
            [] ->
                ?ERROR("[MGR] zookeeper error", []),
                {error, error_zookeeper_error};
            Partitions ->
                ?INFO("[MGR] topic ~p metadata, partitions: ~p~n", [Name, Partitions]),
                Partitions
        end,
    ezk:end_connection(Pid, stop),
    Response.

get_partition_list(Pid, Topic) ->
    Path = lists:concat(["/brokers/topics/", Topic, "/partitions"]),
    case ezk:ls(Pid, lists:flatten(Path)) of
        {error, no_dir} ->
            undefined;
        {error, _Error} ->
            [];
        {ok, IDList} ->
            get_partitions(Pid, Topic, IDList)
    end.

%% @return
%%  [] - no topic metadata
%%  [#partition{}]
get_partitions(Pid, Name, PartitionIDs) ->
    lists:foldl(fun(ID, L) ->
        Partition = ekafka_util:to_integer(ID),
        Path = lists:concat(["/brokers/topics/", Name, "/partitions/", Partition, "/state"]),
        case ezk:get(Pid, lists:flatten(Path)) of
            {error, _Error} ->
                L;
            {ok, {Bin,_}} ->
                Bin1 = binary:replace(Bin, [<<"{">>, <<"}">>, <<"\"">>], <<>>, [global]),
                Leader =
                    lists:foldl(fun(Bin2, LeadID) ->
                        case Bin2 of
                            <<"leader:", IDBin/binary>> ->
                                ekafka_util:to_integer(IDBin);
                            _ ->
                                LeadID
                        end
                    end, 0, binary:split(Bin1, <<",">>, [global, trim_all])),
                {Host, Port} = get_leader_hosts(Leader),
                [#partition{id = ekafka_util:to_integer(ID), lead = Leader, isr = get_isr_list(Bin1), host = Host, port = Port} | L]
        end
    end, [], PartitionIDs).

get_isr_list(Bin) ->
    [_H, T] = binary:split(Bin, <<"isr:">>),
    [IsrBinL|_] = binary:split(T, [<<"[">>, <<"]">>], [global, trim_all]),
    lists:foldr(fun(ID, L) ->
        [ekafka_util:to_integer(ID) | L]
    end, [], binary:split(IsrBinL, <<",">>, [global, trim_all])).
