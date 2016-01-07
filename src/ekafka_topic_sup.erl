%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 18:40
%%%-------------------------------------------------------------------
-module(ekafka_topic_sup).
-author("luyou").

-include("ekafka.hrl").

-behaviour(supervisor).

%% API
-export([start_link/3, start_worker_sup/1, start_offset_manager/3, consume/3]).

-export([produce/3]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Topic :: string(), Role :: atom(), Group :: string()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Topic, Role, Group) ->
    supervisor:start_link({local, ekafka_util:get_topic_supervisor_name(Topic)}, ?MODULE, {Topic, Role, Group}).

-spec start_worker_sup(Topic :: string()) ->
    {ok, Pid :: pid()} | any().
start_worker_sup(Topic) ->
    WorkerSupSpec = {ekafka_worker_sup,
        {ekafka_worker_sup, start_link, []},
        temporary,
        10000,
        supervisor,
        [ekafka_worker_sup]},

    supervisor:start_child(ekafka_util:get_topic_supervisor_name(Topic), WorkerSupSpec).

-spec start_offset_manager(Topic :: string(), Group :: string(), Partitions :: list(#partition{})) ->
    {ok, Pid :: pid()} | any().
start_offset_manager(Topic, Group, Partitions) ->
    Restart = permanent,
    Shutdown = 5000,
    Type = worker,

    OffsetMgrSpec =
        {ekafka_offset_mgr, {ekafka_offset_mgr, start_link, [Topic, Group, Partitions]},
            Restart, Shutdown, Type, [ekafka_offset_mgr]},

    supervisor:start_child(ekafka_util:get_topic_supervisor_name(Topic), OffsetMgrSpec).

-spec produce(Type :: sync | async, Topic :: string(), KV :: list(tuple())) ->
    {error, any()} | ok.
produce(Type, Topic, [{Key, _V}|_] = KVList) ->
    case gen_server:call(ekafka_util:get_topic_manager_name(Topic), {pick_produce_worker, Key}) of
        {ok, Pid} ->
            gen_server:call(Pid, {produce, Type, KVList});
        {error, Error} ->
            {error, Error}
    end.

consume(Type, Topic, Partition) ->
    case gen_server:call(ekafka_util:get_topic_manager_name(Topic), {pick_consume_worker, Partition}) of
        {error, Error} ->
            {error, Error};
        {ok, Pid} ->
            Res = gen_server:call(Pid, {consume, Type}),
            case Type of
                async ->
                    receive
                        {ekafka, fetched, []} ->
                            consume(Type, Topic, Partition);
                        {ekafka, fetched, MsgList} ->
                            {ok, MsgList};
                        {ekafka, error, Error} ->
                            {error, Error}
                    end;
                _ ->
                    Res
            end
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init({Topic, Role, Group}) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 5000,
    Type = worker,

    MgrChild =
        {ekafka_manager, {ekafka_manager, start_link, [Topic, Role, Group]},
            Restart, Shutdown, Type, [ekafka_manager]},

    {ok, {SupFlags, [MgrChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
