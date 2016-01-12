%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 19:36
%%%-------------------------------------------------------------------
-module(ekafka_broker_mgr).
-author("luyou").

-include("ekafka.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {zk          :: pid() | undfined,
                topics = [] :: [string()]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    erlang:send(self(), initialize),
    {ok, #state{}}.

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
handle_cast({topic_added, Topic}, #state{topics = Topics} = State) ->
    {noreply, State#state{topics = [Topic | Topics]}};
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
handle_info(initialize, State) ->
    ets:new(?EKAFKA_CONF, [set, public, named_table, {read_concurrency, true}, {keypos, #ekafka_conf.key}]),
    ekafka_util:set_conf(broker_mgr_pid, self()),
    case application:get_env(ekafka, conf) of
        undefined ->
            {stop, error_no_configuration, State};
        {ok, Options} ->
            lists:map(fun({Key, Value}) ->
                ekafka_util:set_conf(ekafka_util:to_atom(Key), Value)
            end, Options),
            %% get brokers from zookeeper or configuration
            case ekafka_util:get_conf(brokers) of
                undefined ->
                    erlang:send(self(), find_brokers);
                _ ->
                    ok
            end,
            {noreply, State}
    end;
handle_info(find_brokers, State) ->
    case ekafka_util:get_conf(zookeeper) of
        undefined ->
            {stop, error_no_zookeeper_or_brokers, State};
        ZKConf ->
            {ok, Pid} = ezk:start_connection(ZKConf),
            case get_brokers_list(Pid) of
                [] ->
                    {stop, error_zookeeper_error, State};
                Brokers ->
                    ekafka_util:set_conf(brokers, Brokers),
                    {noreply, State#state{zk = Pid}}
            end
    end;

%% zookeeper callbacks
handle_info({broker_changed, _}, #state{zk = Pid, topics = Topics} = State) ->
    ?ERROR("[B] kafka broker changed~n", []),
    case get_brokers_list(Pid) of
        [] ->
            {stop, error_zookeeper_error, State};
        Brokers ->
            ekafka_util:set_conf(brokers, Brokers),
            lists:foreach(fun(Name) ->
                gen_server:cast(ekafka_util:get_topic_manager_name(Name), kafka_broker_down)
            end, Topics),
            {noreply, State}
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
terminate(_Reason, #state{zk = Pid}) ->
    case Pid of
        undefined ->
            ok;
        _ ->
            ezk:end_connection(Pid, shutdown),
            ok
    end.

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
get_brokers_list(Pid) ->
    case ezk:ls(Pid, "/brokers/ids", self(), broker_changed) of
        {error, _Error} ->
            [];
        {ok, IDList} ->
            get_brokers(Pid, IDList)
    end.

get_brokers(Pid, BrokerIDs) ->
    lists:foldl(fun(ID, L) ->
        Broker = ekafka_util:to_integer(ID),
        Path = lists:concat(["/brokers/ids/", Broker]),
        case ezk:get(Pid, lists:flatten(Path)) of
            {error, _Error} ->
                L;
            {ok, {Bin,_}} ->
                Bin1 = binary:replace(Bin, [<<"{">>, <<"}">>, <<"\"">>], <<>>, [global]),
                Host =
                    lists:foldl(fun(Bin2, {IP, Port}) ->
                        case Bin2 of
                            <<"host:", IPBin/binary>> ->
                                {ekafka_util:to_list(IPBin), Port};
                            <<"port:", PortBin/binary>> ->
                                {IP, ekafka_util:to_integer(PortBin)};
                            _ ->
                                {IP, Port}
                        end
                    end, {undefined, undefined}, binary:split(Bin1, <<",">>, [global])),  %% old erlang version not support trim_all
                [{Broker, Host} | L]
        end
    end, [], BrokerIDs).
