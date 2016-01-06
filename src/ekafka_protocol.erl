%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 十二月 2015 15:02
%%%-------------------------------------------------------------------
-module(ekafka_protocol).
-author("luyou").

-include("ekafka.hrl").

%% API
-export([encode_request/3, decode_response/2]).

%% For debug
-compile(export_all).


-spec encode_request(TraceID :: int32(), Client :: any(),
        Request :: #metadata_request{}           |
                   #produce_request{}            |
                   #fetch_request{}              |
                   #offset_request{}             |
                   #group_coordinator_request{}  |
                   #offset_commit_request{}      |
                   #offset_fetch_request{}       |
                   #join_group_request{}         |
                   #sync_group_request{}         |
                   #heartbeat_request{}          |
                   #leave_group_request{}        |
                   #list_groups_request{}        |
                   #describe_groups_request{}) ->
    {API :: int16(), Data :: binary()}.
encode_request(TraceID, Client, Request) ->
    {API, Ver, ReqBin} =
        if
            erlang:is_record(Request, metadata_request) =:= true ->
                {?METADATA_REQUEST, 0, encode_metadata_request(Request)};
            erlang:is_record(Request, produce_request) =:= true ->
                {?PRODUCE_REQUEST, 0, encode_produce_request(Request)};
            erlang:is_record(Request, fetch_request) =:= true ->
                {?FETCH_REQUEST, 0, encode_fetch_request(Request)};
            erlang:is_record(Request, offset_request) =:= true ->
                {?OFFSET_REQUEST, 0, encode_offset_request(Request)};
            erlang:is_record(Request, group_coordinator_request) =:= true ->
                {?GROUP_COORDINATOR_REQUEST, 0, encode_group_coordinator_request(Request)};
            erlang:is_record(Request, offset_commit_request) =:= true ->
                {?OFFSET_COMMIT_REQUEST, 0, encode_offset_commit_request(Request)};
            erlang:is_record(Request, offset_fetch_request) =:= true ->
                {?OFFSET_FETCH_REQUEST, 0, encode_offset_fetch_request(Request)};
            erlang:is_record(Request, join_group_request) =:= true ->
                {?JOIN_GROUP_REQUEST, 0, encode_join_group_request(Request)};
            erlang:is_record(Request, sync_group_request) =:= true ->
                {?SYNC_GROUP_REQUEST, 0, encode_sync_group_request(Request)};
            erlang:is_record(Request, heartbeat_request) =:= true ->
                {?HEARTBEAT_REQUEST, 0, encode_heartbeat_request(Request)};
            erlang:is_record(Request, leave_group_request) =:= true ->
                {?LEAVE_GROUP_REQUEST, 0, encode_leave_group_request(Request)};
            erlang:is_record(Request, list_groups_request) =:= true ->
                {?LIST_GROUPS_REQUEST, 0, encode_list_groups_request(Request)};
            erlang:is_record(Request, describe_groups_request) =:= true ->
                {?DESCRIBE_GROUPS_REQUEST, 0, encode_describe_groups_request(Request)};
            true ->
                {-1, undefined}
        end,
    ClientBin = encode_string(ekafka_util:to_list(Client)),
    {API, <<API:16/?INT, Ver:16/?INT, TraceID:32/?INT, ClientBin/binary, ReqBin/binary>>}.

-spec decode_response(Args :: any(), In :: binary()) ->
    {CorrId :: int32(), Response :: undefined                     |
                                    #metadata_response{}          |
                                    #produce_response{}           |
                                    #fetch_response{}             |
                                    #offset_response{}            |
                                    #group_coordinator_response{} |
                                    #offset_commit_response{}     |
                                    #offset_fetch_response{}      |
                                    #join_group_response{}        |
                                    #sync_group_response{}        |
                                    #heartbeat_response{}         |
                                    #leave_group_response{}       |
                                    #list_groups_response{}       |
                                    #describe_groups_response{}}.
decode_response(Fun, <<CorrId:32/?INT, Data/binary>>) ->
    API =
        case Fun of
            {M,F,A} -> M:F(A, CorrId);
            _       -> Fun(CorrId)
        end,
    Response =
        case API of
            ?PRODUCE_REQUEST ->
                decode_produce_response(Data);
            ?FETCH_REQUEST ->
                decode_fetch_response(Data);
            ?OFFSET_REQUEST ->
                decode_offset_response(Data);
            ?METADATA_REQUEST ->
                decode_metadata_response(Data);
            ?OFFSET_COMMIT_REQUEST ->
                decode_offset_commit_response(Data);
            ?OFFSET_FETCH_REQUEST ->
                decode_offset_fetch_response(Data);
            ?GROUP_COORDINATOR_REQUEST ->
                decode_group_coordinator_response(Data);
            ?JOIN_GROUP_REQUEST ->
                decode_join_group_response(Data);
            ?HEARTBEAT_REQUEST ->
                decode_heartbeat_response(Data);
            ?LEAVE_GROUP_REQUEST ->
                decode_leave_group_response(Data);
            ?SYNC_GROUP_REQUEST ->
                decode_sync_group_response(Data);
            ?DESCRIBE_GROUPS_REQUEST ->
                decode_describe_groups_response(Data);
            ?LIST_GROUPS_REQUEST ->
                decode_list_groups_response(Data);
            _ ->
                ?ERROR("[PROTO] invalid API key", []),
                {undefined, <<>>}
        end,
    {CorrId, Response}.


%% Variable Length Primitives
%%  bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content.
%%  A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
encode_string(Str) when erlang:is_list(Str) ->
    Bin = ekafka_util:to_binary(Str),
    case erlang:size(Bin) of
        0   -> <<-1:16/?INT>>;
        Len -> <<Len:16/?INT, Bin/binary>>
    end;
encode_string(_) ->
    <<-1:16/?INT>>.

encode_bytes(Bytes) when erlang:is_binary(Bytes) ->
    case erlang:size(Bytes) of
        0   -> <<-1:32/?INT>>;
        Len -> <<Len:32/?INT, Bytes/binary>>
    end;
encode_bytes(_) ->
    <<-1:32/?INT>>.

%% Arrays
%%  This is a notation for handling repeated structures.
%%  These will always be encoded as an int32 size containing the length N followed by N repetitions of the structure
%%   which can itself be made up of other primitive types.
encode_array([H|_] = List) when erlang:is_binary(H) ->
    {Length, Data} =
        lists:foldr(fun(Bin, {Len, Acc}) ->
            {Len + 1, <<Bin/binary, Acc/binary>>}
        end, {0, <<>>}, List),
    <<Length:32/?INT, Data/binary>>;
encode_array(_) ->
    <<0:32/?INT>>.

encode_message(#message{offset = Offset, body = #message_body{magic = Magic, attributes = Attr, key = Key, value = Value}}) ->
    KeyBin = encode_bytes(Key),
    ValueBin = encode_bytes(Value),
    BodyBin = <<Magic:8/?INT, Attr:8/?INT, KeyBin/binary, ValueBin/binary>>,
    CRC = erlang:crc32(BodyBin),
    Size = erlang:size(BodyBin) + 4,
    <<Offset:64/?INT, Size:32/?INT, CRC:32/?INT, BodyBin/binary>>.

%% WARN: message set cannot be encoded as array!!!
encode_message_set(#message_set{messages = Messages}) ->
%%    MessagesBinL =
%%        lists:foldr(fun(Message, L) ->
%%            [encode_message(Message) | L]
%%        end, [], Messages),
%%    encode_array(MessagesBinL).
    lists:foldr(fun(Message, Bin) ->
        <<(encode_message(Message))/binary, Bin/binary>>
    end, <<>>, Messages).

encode_topic(#topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [<<Partition:32/?INT>> | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.


%% Note: If "auto.create.topics.enable" is set in the broker configuration,
%%  a topic metadata request will create the topic with the default replication factor and number of partitions.
encode_metadata_request(#metadata_request{topics = Topics}) ->
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_string(Topic) | L]
        end, [], Topics),
    encode_array(TopicsBinL).

parse_metadata_response() ->
    ok.

encode_produce_request(#produce_request{acks = Acks, timeout = Timeout, topics = Topics}) ->
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_produce_req_topic(Topic) | L]
        end, [], Topics),
    TopicsBin = encode_array(TopicsBinL),
    <<Acks:16/?INT, Timeout:32/?INT, TopicsBin/binary>>.

encode_produce_req_topic(#produce_req_topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [encode_produce_req_partition(Partition) | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.

encode_produce_req_partition(#produce_req_partition{id = ID, message_set = MessageSet}) ->
    MessageBin = encode_message_set(MessageSet),
    Size = erlang:size(MessageBin),
    <<ID:32/?INT, Size:32/?INT, MessageBin/binary>>.

encode_fetch_request(#fetch_request{max_wait = MaxWait, mini_bytes = MiniBytes, topics = Topics}) ->
    Replica = -1,
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_fetch_req_topic(Topic) | L]
        end, [], Topics),
    TopicsBin = encode_array(TopicsBinL),
    <<Replica:32/?INT, MaxWait:32/?INT, MiniBytes:32/?INT, TopicsBin/binary>>.

encode_fetch_req_topic(#fetch_req_topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [encode_fetch_req_partition(Partition) | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.

encode_fetch_req_partition(#fetch_req_partition{id = ID, offset = Offset, max_bytes = MaxBytes}) ->
    <<ID:32/?INT, Offset:64/?INT, MaxBytes:32/?INT>>.

encode_offset_request(#offset_request{topics = Topics}) ->
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_offset_req_topic(Topic) | L]
        end, [], Topics),
    Replica = -1,
    TopicsBin = encode_array(TopicsBinL),
    <<Replica:32/?INT, TopicsBin/binary>>.

encode_offset_req_topic(#offset_req_topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [encode_offset_req_partition(Partition) | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.

encode_offset_req_partition(#offset_req_partition{id = ID, time = Time, max_num = MaxNum}) ->
    <<ID:32/?INT, Time:64/?INT, MaxNum:32/?INT>>.

encode_group_coordinator_request(#group_coordinator_request{id = GroupID}) ->
    encode_string(GroupID).

encode_offset_commit_request(#offset_commit_request{group_id = GroupID, topics = Topics}) ->
    GroupBin = encode_string(GroupID),
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_offset_commit_req_topic(Topic) | L]
        end, [], Topics),
    TopicsBin = encode_array(TopicsBinL),
    <<GroupBin/binary, TopicsBin/binary>>.

encode_offset_commit_req_topic(#offset_commit_req_topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [encode_offset_commit_req_partition(Partition) | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.

encode_offset_commit_req_partition(#offset_commit_req_partition{id = ID, offset = Offset, metadata = Metadata}) ->
    MetadataBin = encode_string(Metadata),
    <<ID:32/?INT, Offset:64/?INT, MetadataBin/binary>>.

encode_offset_fetch_request(#offset_fetch_request{group_id = GroupID, topics = Topics}) ->
    GroupBin = encode_string(GroupID),
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_offset_fetch_req_topic(Topic) | L]
        end, [], Topics),
    TopicsBin = encode_array(TopicsBinL),
    <<GroupBin/binary, TopicsBin/binary>>.

encode_offset_fetch_req_topic(#offset_fetch_req_topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [<<Partition:32/?INT>> | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.

encode_join_group_request(#join_group_request{id = ID, timeout = Timeout, member = Member, proto_type = Type, protocols = Protocols}) ->
    MemberBin = encode_string(Member),
    TypeBin = encode_string(Type),
    ProtocolsBinL =
        lists:foldr(fun(Protocol, L) ->
            [encode_join_group_req_protocol(Protocol) | L]
        end, [], Protocols),
    ProtocolsBin = encode_array(ProtocolsBinL),
    <<ID:32/?INT, Timeout:32/?INT, MemberBin/binary, TypeBin/binary, ProtocolsBin/binary>>.

encode_join_group_req_protocol(#join_group_req_protocol{name = Name, version = Ver, subscription = Sub, userdata = UData}) ->
    NameBin = encode_string(Name),
    SubBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_string(Topic) | L]
        end, [], Sub),
    SubBin = encode_array(SubBinL),
    UDataBin = encode_bytes(UData),
    <<NameBin/binary, Ver:16/?INT, SubBin/binary, UDataBin/binary>>.

encode_sync_group_request(#sync_group_request{id = ID, generation = Gen, member_id = Member, assignment = Assigns}) ->
    IDBin = encode_string(ID),
    MemberBin = encode_string(Member),
    AssignsBinL =
        lists:foldr(fun(Assignment, L) ->
            [encode_sync_group_req_assignment(Assignment) | L]
        end, [], Assigns),
    AssignsBin = encode_array(AssignsBinL),
    <<IDBin/binary, Gen:32/?INT, MemberBin/binary, AssignsBin/binary>>.

encode_sync_group_req_assignment(#sync_group_req_assignment{id = ID, assignment = Assignment}) ->
    IDBin = encode_string(ID),
    AssignBin = encode_group_member_assignment(Assignment),
    <<IDBin/binary, AssignBin/binary>>.

encode_group_member_assignment(#group_member_assignment{version = Ver, partitions = Partitions, user_data = UData}) ->
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [encode_topic(Partition) | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    UDataBin = encode_bytes(UData),
    <<Ver:16/?INT, PartitionsBin/binary, UDataBin/binary>>.

encode_heartbeat_request(#heartbeat_request{group_id = ID, generation = Gen, member_id = Member}) ->
    IDBin = encode_string(ID),
    MemBin = encode_string(Member),
    <<IDBin/binary, Gen:32/?INT, MemBin/binary>>.

encode_leave_group_request(#leave_group_request{id = ID, member_id = Member}) ->
    IDBin = encode_string(ID),
    MemBin = encode_string(Member),
    <<IDBin/binary, MemBin/binary>>.

encode_list_groups_request(#list_groups_request{}) ->
    <<>>.

encode_describe_groups_request(#describe_groups_request{groups = Groups}) ->
    GroupsBinL =
        lists:foldr(fun(Group, L) ->
            [encode_string(Group) | L]
        end, [], Groups),
    encode_array(GroupsBinL).


%%
decode_array(<<Len:32/?INT, Data/binary>>) ->
    {Len, Data}.

decode_string(<<Len:16/?INT, Data/binary>>) ->
    case Len of
        -1 ->
            {undefined, Data};
        _ ->
            <<Str:Len/binary, Tail/binary>> = Data,
            {ekafka_util:to_list(Str), Tail}
    end.

decode_bytes(<<Len:32/?INT, Data/binary>>) ->
    case Len of
        -1 ->
            {undefined, Data};
        _ ->
            <<Bytes:Len/binary, Tail/binary>> = Data,
            {Bytes, Tail}
    end.

decode_message(<<>>, Acc) ->
    Acc;
decode_message(<<Offset:64/?INT, Size:32/?INT, Body:Size/binary, Tail/binary>>, Acc) ->
    <<CRC:32/?INT, T1/binary>> = Body,
    CRC32 = CRC band 16#FFFFFFFF,
    case erlang:crc32(T1) of
        CRC32 ->
            <<Magic:8/?INT, Attr:8/?INT, T2/binary>> = T1,
            {Key, T3} = decode_bytes(T2),
            {Value, <<>>} = decode_bytes(T3),
            Message = #message{offset = Offset, body = #message_body{magic = Magic, attributes = Attr, key = Key, value = Value}},
            %decode_message(Tail, [Message | Acc]);
            decode_message(Tail, Acc ++ [Message]);
        V ->
            ?ERROR("invalid message crc32, ~p:~p~n", [V, CRC32]),
            []
    end;
decode_message(<<Offset:64/?INT, Size:32/?INT, Tail/binary>>, Acc) ->
    ?DEBUG("discard offset ~p, because no enough data ~p:~p~n", [Offset, Size, erlang:size(Tail)]),
    Acc.

%% message set cannot be decoded by array
decode_message_set(Data) ->
%%    {Count, T1} = decode_array(Data),
%%    {Messages, T2} =
%%        lists:foldl(fun(_, {L1, TT1}) ->
%%            {Message, TT2} = decode_message(TT1),
%%            {[Message | L1], TT2}
%%        end, {[], T1}, lists:seq(1, Count)),
%%    {#message_set{messages = lists:reverse(Messages)}, T2}.
    Messages = decode_message(Data, []),
    %%#message_set{messages = lists:reverse(Messages)}.
    #message_set{messages = Messages}.


decode_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, <<ID:32/?INT, TT1/binary>>}) ->
            {[ID | L1], TT1}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.


decode_metadata_response(Data) ->
    {BrokerCount, T1} = decode_array(Data),
    {Brokers, T2} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Broker, TT2} = decode_metadata_res_broker(TT1),
            {[Broker | L1], TT2}
        end, {[], T1}, lists:seq(1, BrokerCount)),
    {TopicCount, T3} = decode_array(T2),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L2, TT3}) ->
            {Topic, TT4} = decode_metadata_res_topic(TT3),
            {[Topic | L2], TT4}
        end, {[], T3}, lists:seq(1, TopicCount)),
    #metadata_response{brokers = lists:reverse(Brokers), topics = lists:reverse(Topics)}.

decode_metadata_res_broker(<<ID:32/?INT, Tail/binary>>) ->
    {Host, <<Port:32/?INT, NewT/binary>>} = decode_string(Tail),
    {#metadata_res_broker{id = ID, host = Host, port = Port}, NewT}.

decode_metadata_res_topic(<<Error:16/?INT, Tail/binary>>) ->
    {Name, T1} = decode_string(Tail),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_metadata_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#metadata_res_topic{error = Error, name = Name,
        partitions = lists:reverse(Partitions)}, T3}.

decode_metadata_res_partition(<<Error:16/?INT, ID:32/?INT, Leader:32/?INT, Tail/binary>>) ->
    {ReplicaCount, T1} = decode_array(Tail),
    {Replicas, T2} =
        lists:foldl(fun(_, {L1, <<ReplicaID:32/?INT, TT1/binary>>}) ->
            {[ekafka_util:to_integer(ReplicaID) | L1], TT1}
        end, {[], T1}, lists:seq(1, ReplicaCount)),
    {IsrCount, T3} = decode_array(T2),
    {Isrs, T4} =
        lists:foldl(fun(_, {L2, <<IsrID:32/?INT, TT2/binary>>}) ->
            {[ekafka_util:to_integer(IsrID) | L2], TT2}
        end, {[], T3}, lists:seq(1, IsrCount)),
    {#metadata_res_partition{error = Error, id = ID, leader = Leader,
        replicas = lists:reverse(Replicas), isr = lists:reverse(Isrs)}, T4}.

decode_produce_response(Data) ->
    {TopicsCount, T1} = decode_array(Data),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_produce_res_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, TopicsCount)),
    #produce_response{topics = lists:reverse(Topics)}.

decode_produce_res_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_produce_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#produce_res_topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.

decode_produce_res_partition(<<ID:32/?INT, Error:16/?INT, Offset:64/?INT, Tail/binary>>) ->
    {#produce_res_partition{id = ID, error = Error, offset = Offset}, Tail}.

decode_fetch_response(Data) ->
    {TopicsCount, T1} = decode_array(Data),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_fetch_res_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, TopicsCount)),
    #fetch_response{topics = lists:reverse(Topics)}.

decode_fetch_res_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_fetch_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#fetch_res_topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.

decode_fetch_res_partition(<<ID:32/?INT, Error:16/?INT, HmOffset:64/?INT, Size:32/?INT, Messages:Size/binary, Tail/binary>>) ->
    MsgSet = decode_message_set(Messages),
    {#fetch_res_partition{id = ID, error = Error, hm_offset = HmOffset, message_set = MsgSet}, Tail}.

decode_offset_response(Data) ->
    {TopicsCount, T1} = decode_array(Data),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_offset_res_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, TopicsCount)),
    #offset_response{topics = lists:reverse(Topics)}.

decode_offset_res_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_offset_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#offset_res_topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.

decode_offset_res_partition(<<ID:32/?INT, Error:16/?INT, Tail/binary>>) ->
    {OffsetsCount, T1} = decode_array(Tail),
    {Offsets, T2} =
        lists:foldl(fun(_, {L1, <<Offset:64/?INT, TT1/binary>>}) ->
            {[Offset | L1], TT1}
        end, {[], T1}, lists:seq(1, OffsetsCount)),
    {#offset_res_partition{id = ID, error = Error, offsets = lists:reverse(Offsets)}, T2}.

decode_group_coordinator_response(<<Error:16/?INT, ID:32/?INT, Tail/binary>>) ->
    {Host, <<Port:32/?INT>>} = decode_string(Tail),
    #group_coordinator_response{error = Error, id = ID, host = Host, port = Port}.

decode_offset_commit_response(Data) ->
    {TopicsCount, T1} = decode_array(Data),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_offset_commit_res_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, TopicsCount)),
    #offset_commit_response{topics = lists:reverse(Topics)}.

decode_offset_commit_res_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_offset_commit_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#offset_commit_res_topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.

decode_offset_commit_res_partition(<<ID:32/?INT, Error:16/?INT, Tail/binary>>) ->
    {#offset_commit_res_partition{id = ID, error = Error}, Tail}.

decode_offset_fetch_response(Data) ->
    {TopicsCount, T1} = decode_array(Data),
    {Topics, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_offset_fetch_res_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, TopicsCount)),
    #offset_fetch_response{topics = lists:reverse(Topics)}.

decode_offset_fetch_res_topic(Data) ->
    {Name, T1} = decode_string(Data),
    {PartCount, T2} = decode_array(T1),
    {Partitions, T3} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Partition, TT2} = decode_offset_fetch_res_partition(TT1),
            {[Partition | L1], TT2}
        end, {[], T2}, lists:seq(1, PartCount)),
    {#offset_fetch_res_topic{name = Name, partitions = lists:reverse(Partitions)}, T3}.

decode_offset_fetch_res_partition(<<ID:32/?INT, Offset:64/?INT, Tail/binary>>) ->
    {MetaData, <<Error:16/?INT, T1/binary>>} = decode_string(Tail),
    {#offset_fetch_res_partition{id = ID, offset = Offset, metadata = MetaData, error = Error}, T1}.

decode_join_group_response(<<Error:16/?INT, Gen:32/?INT, Tail/binary>>) ->
    {Protocol, T1} = decode_string(Tail),
    {Leader, T2} = decode_string(T1),
    {MemID, T3} = decode_string(T2),
    {MemCount, T4} = decode_array(T3),
    {Members, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Member, TT2} = decode_join_group_res_member(TT1),
            {[Member | L1], TT2}
        end, {[], T4}, lists:seq(1, MemCount)),
    #join_group_response{error = Error, generation = Gen, protocol = Protocol, leader = Leader, id = MemID, members = lists:reverse(Members)}.

decode_join_group_res_member(Data) ->
    {MemID, T1} = decode_string(Data),
    {Metadata, T2} = decode_bytes(T1),
    {#join_group_res_member{id = MemID, metadata = Metadata}, T2}.

decode_sync_group_response(<<Error:16/?INT, Tail/binary>>) ->
    {Assignment, <<>>} = decode_group_member_assignment(Tail),
    #sync_group_response{error = Error, assignment = Assignment}.

decode_group_member_assignment(<<Ver:16/?INT, Tail/binary>>) ->
    {PartCount, T1} = decode_array(Tail),
    {Partitions, T2} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Topic, TT2} = decode_topic(TT1),
            {[Topic | L1], TT2}
        end, {[], T1}, lists:seq(1, PartCount)),
    {UserData, T3} = decode_bytes(T2),
    {#group_member_assignment{version = Ver, partitions = lists:reverse(Partitions), user_data = UserData}, T3}.

decode_heartbeat_response(<<Error:16/?INT>>) ->
    #heartbeat_response{error = Error}.

decode_leave_group_response(<<Error:16/?INT>>) ->
    #leave_group_response{error = Error}.

decode_list_groups_response(<<Error:16/?INT, Tail/binary>>) ->
    {GroupsCount, T1} = decode_array(Tail),
    {Groups, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Group, TT2} = decode_list_groups_res_group(TT1),
            {[Group | L1], TT2}
        end, {[], T1}, lists:seq(1, GroupsCount)),
    #list_groups_response{error = Error, groups = lists:reverse(Groups)}.

decode_list_groups_res_group(Data) ->
    {ID, T1} = decode_string(Data),
    {ProtoType, T2} = decode_string(T1),
    {#list_groups_res_group{id = ID, proto_type = ProtoType}, T2}.

decode_describe_groups_response(Data) ->
    {GroupsCount, T1} = decode_array(Data),
    {Groups, <<>>} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Group, TT2} = decode_describe_groups_res_group(TT1),
            {[Group | L1], TT2}
        end, {[], T1}, lists:seq(1, GroupsCount)),
    #describe_groups_response{groups = lists:reverse(Groups)}.

decode_describe_groups_res_group(<<Error:16/?INT, Tail/binary>>) ->
    {ID, T1} = decode_string(Tail),
    {State, T2} = decode_string(T1),
    {ProtoType, T3} = decode_string(T2),
    {Protocol, T4} = decode_string(T3),
    {MemCount, T5} = decode_array(T4),
    {Members, T6} =
        lists:foldl(fun(_, {L1, TT1}) ->
            {Member, TT2} = decode_describe_groups_res_group_member(TT1),
            {[Member | L1], TT2}
        end, {[], T5}, lists:seq(1, MemCount)),
    {#describe_groups_res_group{error = Error, id = ID, state = State, proto_type = ProtoType, protocol = Protocol, members = lists:reverse(Members)}, T6}.

decode_describe_groups_res_group_member(Data) ->
    {ID, T1} = decode_string(Data),
    {Client, T2} = decode_string(T1),
    {Host, T3} = decode_string(T2),
    {Metadata, T4} = decode_bytes(T3),
    {Assignment, T5} = decode_group_member_assignment(T4),
    {#describe_groups_res_group_member{id = ID, client = Client, host = Host, metadata = Metadata, assignment = Assignment}, T5}.
