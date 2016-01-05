%%%-------------------------------------------------------------------
%%% @author yuriy
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 一月 2016 下午3:07
%%%-------------------------------------------------------------------
-module(ekafka_zk_manager).
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

-record(state, {topic         :: string(),
                partitions    :: [#partition{}],
                role          :: atom(),
                group         :: string(),
                sup           :: pid(),
                zk            :: pid(),
                workers}).

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
handle_call({pick_produce_worker, Key}, _From, #state{role = Role, workers = Workers} = State) ->
    case Role of
        producer ->
            case {ekafka_util:get_partition_assignment(), Key} of
                {true, undefined} ->
                    {reply, {error, error_invalid_key}, State};
                {true, _} ->
                    PartID = erlang:phash2(Key, erlang:length(Workers)),
                    {PartID, [Pid1 | Tail1]} = lists:keyfind(PartID, 1, Workers),
                    NewWorkers = lists:keyreplace(PartID, 1, Workers, {PartID, Tail1 ++ [Pid1]}),
                    {reply, {ok, Pid1}, State#state{workers = NewWorkers}};
                _ ->
                    [{ID2, [Pid2 | Tail2]} | Others] = Workers,
                    {reply, {ok, Pid2}, State#state{workers = Others ++ [{ID2, Tail2 ++ [Pid2]}]}}
            end;
        _ ->
            {reply, {error, invalid_operation}, State}
    end;
handle_call({pick_consume_worker, PartID}, _From, #state{role = Role, workers = Workers} = State) ->
    case Role of
        consumer ->
            case PartID of
                undefined ->
                    [{ID, [Pid1]} | Others] = Workers,
                    {reply, {ok, Pid1}, State#state{workers = Others ++ [{ID, [Pid1]}]}};
                _ ->
                    {PartID, [Pid]} = lists:keyfind(PartID, 1, Workers),
                    {reply, {ok, Pid}, State}
            end;
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
handle_info(start_worker_sup, #state{topic = Name} = State) ->
    ?DEBUG("[MGR] starting topic ~p worker supervisor~n", [Name]),
    {ok, Pid} = ekafka_topic_sup:start_worker_sup(Name),
    erlang:send(self(), get_topic_metadata),
    {noreply, State#state{sup = Pid}};
handle_info(get_topic_metadata, #state{topic = Name} = State) ->
    ?DEBUG("[MGR] get_topic_metadata() for topic ~p~n", [Name]),
    ZKConf = ekafka_util:get_conf(zookeeper),
    {ok, Pid} = ezk:start_connection(ZKConf),
    case get_partition_list(Pid, Name) of
        [] ->
            ?ERROR("[MGR] zookeeper error", []),
            {stop, error_zookeeper_error, State};
        Partitions ->
            ?DEBUG("[MGR] topic ~p metadata, partitions: ~p~n", [Name, Partitions]),
            erlang:send(self(), start_offset_mgr),
            {noreply, State#state{zk = Pid, partitions = Partitions}}
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
    ?DEBUG("[MGR] starting workers for topic ~p, count: ~p, role: ~p~n", [Name, Max, Role]),
    Workers =
        lists:foldl(fun(#partition{id = ID} = Partition, L1) ->
            PartWorkers =
                lists:foldl(fun(_, L2) ->
                    {ok, Pid} = supervisor:start_child(Sup, [Name, Partition, Role]),
                    _Ref = erlang:monitor(process, Pid),
                    [Pid | L2]
                end, [], lists:seq(1, Max)),
            [{ID, PartWorkers} | L1]
        end, [], Partitions),
    {noreply, State#state{workers = Workers}};
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
get_partition_list(Pid, Topic) ->
    Path = lists:concat(["/brokers/topics/", Topic, "/partitions"]),
    case ezk:ls(Pid, Path) of
        {error, _Error} ->
            [];
        {ok, IDList} ->
            get_partitions(Pid, Topic, IDList)
    end.

get_partitions(Pid, Name, PartitionIDs) ->
    lists:foldl(fun(ID, L) ->
        Partition = ekafka_util:to_integer(ID),
        Path = lists:concat(["/brokers/topics/", Name, "/partitions/", Partition, "/state"]),
        case ezk:get(Pid, Path) of
            {error, _Error} ->
                L;
            {ok, {Bin,_}} ->
                Bin1 = binary:replace(Bin, [<<"{">>, <<"}">>, <<"\"">>], <<>>, [global]),
                {Leader, Isr} =
                    lists:foldl(fun(Bin2, {LeadID, Is}) ->
                        case Bin2 of
                            <<"leader:", IDBin/binary>> ->
                                {ekafka_util:to_integer(IDBin), Is};
                            <<"isr:", ISRBin/binary>> ->
                                {LeadID, get_isr_list(ISRBin)};
                            _ ->
                                {LeadID, Is}
                        end
                    end, {0, []}, binary:split(Bin1, <<",">>, [global, trim_all])),
                [#partition{id = ekafka_util:to_integer(ID), lead = Leader, isr = Isr} | L]
        end
    end, [], PartitionIDs).

get_isr_list(Bin) ->
    lists:foldr(fun(ID, L) ->
        [ekafka_util:to_integer(ID) | L]
    end, [], binary:split(Bin, [<<",">>, <<"[">>, <<"]">>], [global, trim_all])).
