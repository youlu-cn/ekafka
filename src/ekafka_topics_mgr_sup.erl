%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 十二月 2015 10:26
%%%-------------------------------------------------------------------
-module(ekafka_topics_mgr_sup).
-author("luyou").

-behaviour(supervisor).

%% API
-export([start_link/0, add_topic/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec add_topic(Topic :: string(), Role :: producer | consumer, Group :: string()) ->
    {ok, pid()} | any().
add_topic(Topic, Role, Group) ->
    case erlang:whereis(ekafka_util:get_topic_supervisor_name(Topic)) of
        undefined ->
            TopicSupSpec = {Topic,
                {ekafka_topic_sup, start_link, [Topic, Role, Group]},
                permanent,
                10500,
                supervisor,
                [ekafka_topic_sup]},

            supervisor:start_child(?SERVER, TopicSupSpec),
            gen_server:cast(ekafka_broker_mgr, {topic_added, Topic}),
            ok;
        _ ->
            {error, already_started}
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
init([]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
