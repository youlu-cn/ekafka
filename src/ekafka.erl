%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 18:23
%%%-------------------------------------------------------------------
-module(ekafka).
-author("luyou").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([add_producer/1, add_consumer/2, produce/2, get_partition_list/1, consume/1, consume/2]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
        StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    ekafka_util:ensure_app_started(ezk),
    case ekafka_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.

%%%===================================================================
%%% Module APIs
%%%===================================================================

-spec add_producer(Topic :: string()) ->
    ok.
add_producer(Topic) ->
    ekafka_topics_mgr_sup:add_topic(Topic, producer, undefined).

-spec produce(Topic :: string(), tuple() | list(tuple())) ->
    {error, any()} | ok.
produce(Topic, {Key, Value}) ->
    produce(Topic, [{Key, Value}]);
produce(Topic, KVList) ->
    ekafka_util:check_topic_and_call(Topic,
        {ekafka_topic_sup, produce, {sync, Topic, KVList}}).

-spec add_consumer(Topic :: string(), Group :: string()) ->
    ok.
add_consumer(Topic, Group) ->
    ekafka_topics_mgr_sup:add_topic(Topic, consumer, Group).

-spec get_partition_list(Topic :: string()) ->
    {ok, list(integer())}.
get_partition_list(Topic) ->
    case erlang:whereis(ekafka_util:get_topic_supervisor_name(Topic)) of
        undefined ->
            {error, no_topic};
        _ ->
            gen_server:call(ekafka_util:get_topic_manager_name(Topic), get_partition_list)
    end.

-spec consume(Topic :: string()) ->
    {error, Error :: atom()} | {ok, MsgList :: list(tuple())}.
consume(Topic) ->
    consume(Topic, undefined).

-spec consume(Topic :: string(), Partition :: undefined | integer()) ->
    {error, Error :: atom()} | {ok, MsgList :: list(tuple())}.
consume(Topic, Partition) ->
    ekafka_util:check_topic_and_call(Topic,
        {ekafka_topic_sup, consume, {Topic, Partition}}).
