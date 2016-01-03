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

-record(state, {topic       :: string(),
                group       :: string(),
                partitions  :: [#partition{}],
                offsets     :: [#offset_fetch_res_partition{}],
                sock        :: port()}).

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
        lists:foldl(fun(#offset_fetch_res_partition{id = ID, offset = R}, Ost) ->
            case ID of
                PartID -> R;
                _      -> Ost
            end
        end, undefined, Offsets),
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
handle_cast({message_consumed, PartID, Offset}, #state{partitions = Partitions} = State) ->
    NewPartitions =
        lists:foldr(fun(#partition{id = ID} = Partition, L) ->
            case ID of
                PartID ->
                    [Partition#partition{offset = Offset} | L];
                _ ->
                    [Partition | L]
            end
        end, [], Partitions),
    {noreply, State#state{partitions = NewPartitions}};
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
    case get_group_coordinator(Group) of
        undefined ->
            {stop, error_group_coordinator, State};
        Sock ->
            erlang:send(self(), sync_consume_offset),
            {noreply, State#state{sock = Sock}}
    end;
handle_info(sync_consume_offset, #state{group = Group, topic = Name, partitions = Partitions, sock = Sock} = State) ->
    Request = request_fetch_offset(Group, Name, Partitions),
    case ekafka_util:send_to_server_sync(Sock, Request) of
        undefined ->
            ?ERROR("[O] fetch offset error", []),
            {noreply, State};
        #offset_fetch_response{topics = [#offset_fetch_res_topic{partitions = PartOffset}]} ->
            {noreply, State#state{offsets = PartOffset}}
    end;
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
get_group_coordinator(Group) ->
    [{_,Host}|_T] = ekafka_util:get_conf(brokers),
    case request_group_coordinator(Host, Group) of
        undefined ->
            undefined;
        #group_coordinator_response{error = Error, host = Host, port = Port} ->
            ?INFO("[O] group coordinator res: ~p, hosts: ~p", [Error, {Host, Port}]),
            case Error of
                ?NO_ERROR ->
                    case gen_tcp:connect(Host, Port, ekafka_util:get_tcp_options()) of
                        {error, Reason} ->
                            ?ERROR("[O] connect to group coordinator ~p error: ~p", [Host, Reason]),
                            undefined;
                        {ok, Sock} ->
                            Sock
                    end;
                _ ->
                    undefined
            end
    end.

request_group_coordinator({IP, Port} = Host, Group) ->
    case gen_tcp:connect(IP, Port, ekafka_util:get_tcp_options()) of
        {error, Reason} ->
            ?ERROR("[O] connect to broker ~p error: ~p", [Host, Reason]),
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
