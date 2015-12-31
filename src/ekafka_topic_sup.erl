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

-behaviour(supervisor).

%% API
-export([start_link/2, start_worker_sup/1]).

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
-spec(start_link(Topic :: string(), Role :: atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Topic, Role) ->
    supervisor:start_link({local, ekafka_util:to_atom(Topic)}, ?MODULE, {Topic, Role}).

start_worker_sup(Topic) ->
    WorkerSupSpec = {ekafka_worker_sup,
        {ekafka_worker_sup, start_link, []},
        temporary,
        10000,
        supervisor,
        [ekafka_worker_sup]},

    supervisor:start_child(ekafka_util:to_atom(Topic), WorkerSupSpec).

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
init({Topic, Role}) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 5000,
    Type = worker,

    MgrChild = {ekafka_manager, {ekafka_manager, start_link, [Topic, Role]},
        Restart, Shutdown, Type, [ekafka_manager]},

    {ok, {SupFlags, [MgrChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
