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
-compile(export_all).


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

encode_message(#message{offset = Offset,
                        size = Size,
                        crc = CRC,
                        magic = Magic,
                        attributes = Attr,
                        key = Key,
                        value = Value}) ->
    KeyBin = encode_bytes(Key),
    ValueBin = encode_bytes(Value),
    <<Offset:64/?INT, Size:32/?INT, CRC:32/?INT, Magic:8/?INT, Attr:8/?INT, KeyBin/binary, ValueBin/binary>>.

encode_message_set(#message_set{messages = Messages}) ->
    MessagesBinL =
        lists:foldr(fun(Message, L) ->
            [encode_message(Message) | L]
        end, [], Messages),
    encode_array(MessagesBinL).

encode_topic(#topic{name = Name, partitions = Partitions}) ->
    NameBin = encode_string(Name),
    PartitionsBinL =
        lists:foldr(fun(Partition, L) ->
            [<<Partition:32/?INT>> | L]
        end, [], Partitions),
    PartitionsBin = encode_array(PartitionsBinL),
    <<NameBin/binary, PartitionsBin/binary>>.


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

encode_produce_req_partition(#produce_req_partition{id = ID, size = Size, message_set = MessageSet}) ->
    MessageBin = encode_message_set(MessageSet),
    <<ID:32/?INT, Size:32/?INT, MessageBin/binary>>.

encode_fetch_request(#fetch_request{replica = _Replica, max_wait = MaxWait, mini_bytes = MiniBytes, topics = Topics}) ->
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_fetch_req_topic(Topic) | L]
        end, [], Topics),
    TopicsBin = encode_array(TopicsBinL),
    <<-1:32/?INT, MaxWait:32/?INT, MiniBytes:32/?INT, TopicsBin/binary>>.

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

encode_offset_request(#offset_request{replica = Replica, topics = Topics}) ->
    TopicsBinL =
        lists:foldr(fun(Topic, L) ->
            [encode_offset_req_topic(Topic) | L]
        end, [], Topics),
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

%% TODO:
encode_offset_commit_request(#offset_commit_request{}) ->
    ok.

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
            [sync_group_req_assignment(Assignment) | L]
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
decode_metadata_response(Data) ->
    ok.

decode_produce_response(Data) ->
    ok.

decode_fetch_response(Data) ->
    ok.

decode_offset_response(Data) ->
    ok.

decode_group_coordinator_response(Data) ->
    ok.

decode_offset_commit_response(Data) ->
    ok.

decode_offset_fetch_response(Data) ->
    ok.

decode_join_group_response(Data) ->
    ok.

decode_sync_group_response(Data) ->
    ok.

decode_heartbeat_response(Data) ->
    ok.

decode_leave_group_response(Data) ->
    ok.

decode_list_groups_response(Data) ->
    ok.

decode_describe_groups_response(Data) ->
    ok.


encode_request(Req) ->
    Request =
        if
            erlang:is_record(Req, metadata_request) =:= true ->
                #kafka_request{request = Req, api = ?METADATA_REQUEST};
            erlang:is_record(Req, produce_request) =:= true ->
                #kafka_request{request = Req, api = ?PRODUCE_REQUEST};
            erlang:is_record(Req, fetch_request) =:= true ->
                #kafka_request{request = Req, api = ?FETCH_REQUEST};
            erlang:is_record(Req, offset_request) =:= true ->
                #kafka_request{request = Req, api = ?OFFSET_REQUEST};
            erlang:is_record(Req, group_coordinator_request) =:= true ->
                #kafka_request{request = Req, api = ?GROUP_COORDINATOR_REQUEST};
            erlang:is_record(Req, offset_commit_request) =:= true ->
                #kafka_request{request = Req, api = ?OFFSET_COMMIT_REQUEST};
            erlang:is_record(Req, offset_fetch_request) =:= true ->
                #kafka_request{request = Req, api = ?OFFSET_FETCH_REQUEST};
            erlang:is_record(Req, join_group_request) =:= true ->
                #kafka_request{request = Req, api = ?JOIN_GROUP_REQUEST};
            erlang:is_record(Req, sync_group_request) =:= true ->
                #kafka_request{request = Req, api = ?SYNC_GROUP_REQUEST};
            erlang:is_record(Req, heartbeat_request) =:= true ->
                #kafka_request{request = Req, api = ?HEARTBEAT_REQUEST};
            erlang:is_record(Req, leave_group_request) =:= true ->
                #kafka_request{request = Req, api = ?LEAVE_GROUP_REQUEST};
            erlang:is_record(Req, list_groups_request) =:= true ->
                #kafka_request{request = Req, api = ?LIST_GROUPS_REQUEST};
            erlang:is_record(Req, describe_groups_request) =:= true ->
                #kafka_request{request = Req, api = ?DESCRIBE_GROUPS_REQUEST};
            true ->
                #kafka_request{}
        end.
