%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 十二月 2015 11:14
%%%-------------------------------------------------------------------
-module(ekafka_util).
-author("luyou").

-include("ekafka.hrl").

%% API
-compile(export_all).


%% ensure_app_started/1
ensure_app_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

check_topic_and_call(Topic, {M,F,A}) ->
    case erlang:whereis(get_topic_supervisor_name(Topic)) of
        undefined ->
            {error, invalid_operation};
        _ ->
            M:F(A)
    end.

%% to_atom/1
%% ====================================================================
%% @doc : covert data to atom
%%@return: atom
to_atom(undefined) ->
    undefined;
to_atom([]) ->
    undefined;
to_atom(Data) when erlang:is_number(Data) ->
    to_atom(erlang:integer_to_list(erlang:trunc(Data)));
to_atom(Data) when erlang:is_binary(Data) ->
    erlang:binary_to_atom(Data, utf8);
to_atom(Data) when erlang:is_list(Data) ->
    case erlang:length(Data) of
        L when L > 255 ->
            undefined;
        _ ->
            erlang:list_to_atom(Data)
    end;
to_atom(Data) ->
    Data.


%% to_list/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_list(undefined) ->
    "";
to_list([]) ->
    "";
to_list(Data) when erlang:is_number(Data) ->
    erlang:integer_to_list(erlang:trunc(Data));
to_list(Data) when erlang:is_atom(Data) ->
    erlang:atom_to_list(Data);
to_list(Data) when erlang:is_binary(Data) ->
    erlang:binary_to_list(Data);
to_list(Data) when erlang:is_pid(Data) ->
    erlang:pid_to_list(Data);
to_list(Data) when erlang:is_tuple(Data) ->
    erlang:tuple_to_list(Data);
to_list(Data) ->
    Data.

%% to_binary/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_binary(undefined) ->
    <<>>;
to_binary([]) ->
    <<>>;
to_binary(Data) when erlang:is_integer(Data) ->
    erlang:list_to_binary(erlang:integer_to_list(Data));
to_binary(Data) when erlang:is_number(Data) ->
    erlang:integer_to_binary(erlang:trunc(Data));
to_binary(Data) when erlang:is_list(Data) ->
    erlang:list_to_binary(Data);
to_binary(Data) when erlang:is_atom(Data) ->
    erlang:atom_to_binary(Data, utf8);
to_binary(Data) when erlang:is_tuple(Data) ->
    erlang:list_to_binary(erlang:tuple_to_list(Data));
to_binary(Data) ->
    Data.


%% to_integer/1
%% ====================================================================
%% @doc : covert data to list
%%@return: list
to_integer(undefined) ->
    0;
to_integer(<<"undefined">>) ->
    0;
to_integer("undefined") ->
    0;
to_integer(<<"undefine">>) ->
    0;
to_integer("undefine") ->
    0;
to_integer(<<>>) ->
    0;
to_integer([]) ->
    0;
to_integer(Data) when erlang:is_integer(Data) ->
    Data;
to_integer(Data) ->
    try
        erlang:list_to_integer(to_list(Data))
    catch
        _Error ->
            0;
        exit:_Reason ->
            0;
        error:_Reason ->
            0;
        _Class:_Reason ->
            0
    end.


%% set_conf/2
%% ====================================================================
%% @doc : set conf
%% @return: void
set_conf(Key, Value) ->
    Row = #ekafka_conf{key = Key, value = Value},
    ets:insert(?EKAFKA_CONF, Row).

%% get_conf/1
%% ====================================================================
%% @doc : retrieve conf
%% @return: value
get_conf(Key) ->
    case ets:lookup(?EKAFKA_CONF, Key) of
        [ConfData] ->
            #ekafka_conf{value = Value} = ConfData,
            Value;
        _Any ->
            undefined
    end.

get_tcp_options() ->
    [binary, {active, true}, {keepalive, true}, {packet, 4}].

get_worker_process_count(Role) ->
    case Role of
        producer ->
            case get_conf(produce_workers) of
                undefined -> ?DEFAULT_PRODUCER_PROCESSES;
                Value     -> Value
            end;
        consumer ->
            ?DEFAULT_CONSUMER_PROCESSES
    end.

get_max_message_size() ->
    case get_conf(max_message_bytes) of
        undefined -> ?MAX_MESSAGE_SET_SIZE;
        Value     -> Value
    end.

get_topic_supervisor_name(Topic) ->
    Name = lists:concat([Topic, "_topic_sup"]),
    to_atom(lists:flatten(Name)).

get_topic_manager_name(Topic) ->
    Name = lists:concat([Topic, "_mgr"]),
    to_atom(lists:flatten(Name)).

get_topic_offset_mgr_name(Topic) ->
    Name = lists:concat([Topic, "_offset_mgr"]),
    to_atom(lists:flatten(Name)).

get_partition_assignment() ->
    case get_conf(hash_partition_by_key) of
        undefined -> false;
        Value     -> Value
    end.

get_offset_auto_commit_timeout() ->
    case get_conf(auto_commit_timeout) of
        undefined -> 30000;
        Value     -> Value
    end.

get_fetch_max_timeout() ->
    case get_conf(consume_block_timeout) of
        undefined -> 500;
        Value     -> Value
    end.

get_wait_all_servers() ->
    case get_conf(wait_all_servers) of
        undefined -> true;
        Value     -> Value
    end.


send_to_server_sync(Sock, Request) ->
    Random = random:uniform(2000000000),
    {API, Bin} = ekafka_protocol:encode_request(Random, "sync_client", Request),
    F = fun(Trace) ->
            case Trace of
                Random -> API;
                _      -> -1
            end
        end,
    case gen_tcp:send(Sock, Bin) of
        {error, Reason} ->
            ?ERROR("[SYNC] send to broker error: ~p", [Reason]),
            undefined;
        ok ->
            receive
                {tcp, Sock, Data} ->
                    {_, Res} = ekafka_protocol:decode_response(F, Data),
                    Res
            after
                10000 ->
                    ?ERROR("[SYNC] timeout", []),
                    undefined
            end
    end.


get_error_message(Code) ->
    case Code of
        ?NO_ERROR ->
            "No error";
        ?OFFSET_OUT_OF_RANGE ->
            "The requested offset is outside the range of offsets maintained by the server for the given topic/partition";
        ?INVALID_MESSAGE ->
            "This indicates that a message contents does not match its CRC";
        ?UNKNOWN_TOPIC_OR_PARTITION ->
            "This request is for a topic or partition that does not exist on this broker";
        ?INVALID_MESSAGE_SIZE ->
            "The message has a negative size";
        ?LEADER_NOT_AVAILABLE ->
            "This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes";
        ?NOT_LEADER_FOR_PARTITION ->
            "This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date";
        ?REQUEST_TIMED_OUT ->
            "This error is thrown if the request exceeds the user-specified time limit in the request";
        ?BROKER_NOT_AVAILABLE ->
            "This is not a client facing error and is used mostly by tools when a broker is not alive";
        ?REPLICA_NOT_AVAILABLE ->
            "If replica is expected on a broker, but is not (this can be safely ignored)";
        ?MESSAGE_SIZE_TOO_LARGE ->
            "The server has a configurable maximum message size to avoid unbounded memory allocation." ++
                " This error is thrown if the client attempt to produce a message larger than this maximum";
        ?STALE_CONTROLLER_EPOCH_CODE ->
            "Internal error code for broker-to-broker communication";
        ?OFFSET_METADATA_TOO_LARGE_CODE ->
            "If you specify a string larger than configured maximum for offset metadata";
        ?GROUP_LOAD_IN_PROGRESS_CODE ->
            "The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition)," ++
                " or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator";
        ?GROUP_COORDINATOR_NOT_AVAILABLE_CODE ->
            "The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created," ++
                " or if the group coordinator is not active";
        ?NOT_COORDINATOR_FOR_GROUP_CODE ->
            "The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for";
        ?INVALID_TOPIC_CODE ->
            "For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic)";
        ?RECORD_LIST_TOO_LARGE_CODE ->
            "If a message batch in a produce request exceeds the maximum configured segment size";
        ?NOT_ENOUGH_REPLICAS_CODE ->
            "Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1";
        ?NOT_ENOUGH_REPLICAS_AFTER_APPEND_CODE ->
            "Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required";
        ?INVALID_REQUIRED_ACKS_CODE ->
            "Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0)";
        ?ILLEGAL_GENERATION_CODE ->
            "Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation";
        ?INCONSISTENT_GROUP_PROTOCOL_CODE ->
            "Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group";
        ?INVALID_GROUP_ID_CODE ->
            "Returned in join group when the groupId is empty or null";
        ?UNKNOWN_MEMBER_ID_CODE ->
            "Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation";
        ?INVALID_SESSION_TIMEOUT_CODE ->
            "Return in join group when the requested session timeout is outside of the allowed range on the broker";
        ?REBALANCE_IN_PROGRESS_CODE ->
            "Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group";
        ?INVALID_COMMIT_OFFSET_SIZE_CODE ->
            "This error indicates that an offset commit was rejected because of oversize metadata";
        ?TOPIC_AUTHORIZATION_FAILED_CODE ->
            "Returned by the broker when the client is not authorized to access the requested topic";
        ?GROUP_AUTHORIZATION_FAILED_CODE ->
            "Returned by the broker when the client is not authorized to access a particular groupId";
        ?CLUSTER_AUTHORIZATION_FAILED_CODE ->
            "Returned by the broker when the client is not authorized to use an inter-broker or administrative API";
        _ -> %%?UNKNOWN or others
            "An unexpected server error"
    end.

null(_) -> "".
null(_, _) -> "".
