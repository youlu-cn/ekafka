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

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {topic,
                part,
                role,
                zk,
                sup :: pid(),
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
-spec(start_link(Topic :: string(), Role :: atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Topic, Role) ->
    Name = lists:concat([Topic, "_mgr"]),
    gen_server:start_link({local, ekafka_util:to_atom(Name)}, ?MODULE, {Topic, Role}, []).

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
init({Topic, Role}) ->
    erlang:send(self(), start_worker_sup),
    {ok, #state{topic = Topic, role = Role}}.

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
handle_info(start_worker_sup, #state{topic = Topic} = State) ->
    {ok, Pid} = ekafka_topic_sup:start_worker_sup(Topic),
    erlang:send(self(), {find_partitions, Topic}),
    {noreply, State#state{sup = Pid}};
handle_info({find_partitions, Topic}, State) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            %%TODO: get partition metadata from kafka
            {noreply, State};
        ZKConf ->
            {ok, Pid} = ezk:start_connection(ZKConf),
            Partitions = get_partition_list(Pid, Topic),
            erlang:send(self(), start_workers),
            {noreply, State#state{zk = Pid, part = Partitions}}
    end;
handle_info(start_workers, #state{sup = Sup, topic = Topic, part = Partitions, role = Role} = State) ->
    Max = ekafka_util:get_max_workers(Role),
    Workers =
        lists:foldl(fun({Partition, Hosts}, L) ->
            WorkerList =
                lists:foldl(fun(_, WL) ->
                    {ok, Pid} = supervisor:start_child(Sup, [Topic, Partition, Role, Hosts]),
                    _Ref = erlang:monitor(process, Pid),
                    [Pid | WL]
                end, [], lists:seq(1, Max)),
            [{Partition, WorkerList} | L]
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
                Leader =
                    lists:foldl(fun(Bin2, LeadID) ->
                        case Bin2 of
                            <<"leader:", IDBin/binary>> ->
                                ekafka_util:to_integer(IDBin);
                            _ ->
                                LeadID
                        end
                    end, 0, binary:split(Bin1, <<",">>, [global, trim_all])),
                [{Partition, get_leader_hosts(Leader)} | L]
        end
    end, [], PartitionIDs).

get_leader_hosts(ID) ->
    case ekafka_util:get_conf(brokers) of
        undefined ->
            undefined;
        Brokers ->
            case lists:keyfind(ID, 1, Brokers) of
                {ID, Hosts} -> Hosts;
                _           -> undefined
            end
    end.
