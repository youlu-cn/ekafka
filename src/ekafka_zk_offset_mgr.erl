%%%-------------------------------------------------------------------
%%% @author yuriy
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 一月 2016 下午2:49
%%%-------------------------------------------------------------------
-module(ekafka_zk_offset_mgr).
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

-record(state, {topic           :: string(),
                group           :: string(),
                partitions      :: [#partition{}],
                offsets    = [] :: [{int32(), int64()}],
                zk              :: pid()}).

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
handle_call({get_partition_offset, PartID}, _From, #state{offsets = Offsets} = State) ->
    Offset =
        case lists:keyfind(PartID, 1, Offsets) of
            false ->
                -1;
            {PartID, R} ->
                R
        end,
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
handle_cast({message_consumed, PartID, Offset}, #state{offsets = Offsets} = State) ->
    case lists:keyfind(PartID, 1, Offsets) of
        false ->
            ?ERROR("[O] invalid partition id: ~p~n", [PartID]),
            {noreply, State};
        _ ->
            NewOffsets = lists:keyreplace(PartID, 1, {PartID, Offset}, Offsets),
            {noreply, State#state{offsets = NewOffsets}}
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
handle_info(start_connection, State) ->
    ZKConf = ekafka_util:get_conf(zookeeper),
    {ok, Pid} = ezk:start_connection(ZKConf),
    erlang:send(self(), sync_consume_offset),
    {noreply, State#state{zk = Pid}};
handle_info(sync_consume_offset, #state{group = Group, topic = Name, partitions = Partitions, zk = Pid} = State) ->
    {Offsets, NewPartitions} =
        lists:foldl(fun(#partition{id = PartID} = Partition, {L1, L2}) ->
            Path = lists:concat(["/consumers/", Group, "/offsets/", Name, "/", PartID]),
            case ezk:get(Pid, Path) of
                {ok, {Offset, _}} ->
                    {[{PartID, Offset} | L1], [Partition#partition{offset = ekafka_util:to_integer(Offset)} | L2]};
                _ ->
                    {[{PartID, 0} | L1], [Partition | L2]}
            end
        end, {[], []}, Partitions),
    erlang:send_after(ekafka_util:get_offset_auto_commit_timeout(), self(), auto_commit_offset),
    {noreply, State#state{offsets = Offsets, partitions = NewPartitions}};
handle_info(auto_commit_offset, #state{topic = Name, group = Group, zk = Pid, partitions = Partitions, offsets = Offsets} = State) ->
    NewPartitions =
        lists:foldl(fun(#partition{id = PartID, offset = Offset} = Partition, L) ->
            case lists:keyfind(PartID, 1, Offsets) of
                {PartID, Offset} ->
                    [Partition | L];
                {PartID, NewOffset} ->
                    commit_offset(Pid, Name, Group, PartID, NewOffset),
                    [Partition#partition{offset = NewOffset} | L]
            end
        end, [], Partitions),
    {noreply, State#state{partitions = NewPartitions}};
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
commit_offset(Pid, Name, Group, PartID, Offset) ->
    Path = lists:concat(["/consumers/", Group, "/offsets/", Name, "/", PartID]),
    case ezk:set(Pid, Path, ekafka_util:to_binary(Offset)) of
        {ok, _} ->
            ?DEBUG("[O] offset commited~n", []);
        {error, no_dir} ->
            ?DEBUG("[O] create node for group: ~p, topic: ~p, partition:~p~n", [Group, Name, PartID]),
            ezk:create(Pid, Path, ekafka_util:to_binary(Offset));
        Error ->
            ?ERROR("[O] commit offset failed ~p, ~p:~p~n", [Error, Name, PartID])
    end.
