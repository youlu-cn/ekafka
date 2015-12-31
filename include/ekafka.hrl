%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 18:52
%%%-------------------------------------------------------------------
-author("luyou").

-define(EKAFKA_CONF, ekafka_conf).
-record(ekafka_conf, {key, value}).

-define(MAX_PRODUCER_PROCESSES, 10).


%% kafka protocol
%% request type
-define(PRODUCE_REQUEST,            0).
-define(FETCH_REQUEST,              1).
-define(OFFSET_REQUEST,             2).
-define(METADATA_REQUEST,           3).
-define(OFFSET_COMMIT_REQUEST,      8).
-define(OFFSET_FETCH_REQUEST,       9).
-define(GROUP_COORDINATOR_REQUEST, 10).
-define(JOIN_GROUP_REQUEST,        11).
-define(HEARTBEAT_REQUEST,         12).
-define(LEAVE_GROUP_REQUEST,       13).
-define(SYNC_GROUP_REQUEST,        14).
-define(DESCRIBE_GROUPS_REQUEST,   15).
-define(LIST_GROUPS_REQUEST,       16).


%% error code
-define(NO_ERROR,                                0).
-define(UNKNOWN,                                -1).
-define(OFFSET_OUT_OF_RANGE,                     1).
-define(INVALID_MESSAGE,                         2).
-define(UNKNOWN_TOPIC_OR_PARTITION,              3).
-define(INVALID_MESSAGE_SIZE,                    4).
-define(LEADER_NOT_AVAILABLE,                    5).
-define(NOT_LEADER_FOR_PARTITION,                6).
-define(REQUEST_TIMED_OUT,                       7).
-define(BROKER_NOT_AVAILABLE,                    8).
-define(REPLICA_NOT_AVAILABLE,                   9).
-define(MESSAGE_SIZE_TOO_LARGE,                 10).
-define(STALE_CONTROLLER_EPOCH_CODE,            11).
-define(OFFSET_METADATA_TOO_LARGE_CODE,         12).
-define(GROUP_LOAD_IN_PROGRESS_CODE,            14).
-define(GROUP_COORDINATOR_NOT_AVAILABLE_CODE,   15).
-define(NOT_COORDINATOR_FOR_GROUP_CODE,         16).
-define(INVALID_TOPIC_CODE,                     17).
-define(RECORD_LIST_TOO_LARGE_CODE,             18).
-define(NOT_ENOUGH_REPLICAS_CODE,               19).
-define(NOT_ENOUGH_REPLICAS_AFTER_APPEND_CODE,  20).
-define(INVALID_REQUIRED_ACKS_CODE,             21).
-define(ILLEGAL_GENERATION_CODE,                22).
-define(INCONSISTENT_GROUP_PROTOCOL_CODE,       23).
-define(INVALID_GROUP_ID_CODE,                  24).
-define(UNKNOWN_MEMBER_ID_CODE,                 25).
-define(INVALID_SESSION_TIMEOUT_CODE,           26).
-define(REBALANCE_IN_PROGRESS_CODE,             27).
-define(INVALID_COMMIT_OFFSET_SIZE_CODE,        28).
-define(TOPIC_AUTHORIZATION_FAILED_CODE,        29).
-define(GROUP_AUTHORIZATION_FAILED_CODE,        30).
-define(CLUSTER_AUTHORIZATION_FAILED_CODE,      31).


%% types
-type int32() :: integer().
-type int16() :: integer().
-type int64() :: integer().

%% Metadata API

%%  This API answers the following questions:
%%  What topics exist?
%%  How many partitions does each topic have?
%%  Which broker is currently the leader for each partition?
%%  What is the host and port for each of these brokers?
-record(metadata_request, {topics = []   :: [binary()]}).

%% The response contains metadata for each partition, with partitions grouped together by topic.
%%  This metadata refers to brokers by their broker id. The brokers each have a host and port.
-record(metadata_response, {brokers = [] :: list(#metadata_res_broker{}),
                            topics  = [] :: list(#metadata_res_topic{})}).

-record(metadata_res_broker, {id         :: int32(),
                              host       :: string(),
                              port       :: int32()}).

-record(metadata_res_topic, {code        :: int16(),
                             name        :: string(),
                             part        :: list(#metadata_res_part{})}).

-record(metadata_res_part, {code         :: int16(),
                            id           :: int32(),
                            leader       :: int32(),
                            replica      :: list(int32()),
                            isr          :: list(int32())}).


%% Produce API
%%  The produce API is used to send message sets to the server.
%%  For efficiency it allows sending message sets intended for many topic partitions in a single request.
%%  The produce API uses the generic message set format,
%%  but since no offset has been assigned to the messages at the time of the send the producer is free to fill in that field in any way it likes.
-record(produce_request, {acks           :: int16(),
                          timeout        :: int32(),
                          data           :: binary()}). %%TODO data: [TopicName [Partition MessageSetSize MessageSet]]

%% response
-record(produce_response, {topics        :: list(#produce_res_topic{})}).

-record(produce_res_topic, {topic        :: string(),
                            offsets      :: list(#produce_res_offsets{})}).

-record(produce_res_offsets, {part       :: int32(),
                              code       :: int16(),
                              offset     :: int64()}).


%% Fetch API
%%
-record(fetch_request, {replica          :: int32(), %% should always specify this as -1
                        max_wait         :: int32(),
                        mini_bytes       :: int32(),
                        topic            :: string(),
                        part             :: int32(),
                        offset           :: int64(),
                        max_bytes        :: int32()}).

%% response
-record(fetch_response, {topics          :: list(#fetch_res_topic{})}).

-record(fetch_res_topic, {topic          :: string(),
                          messages       :: list(#fetch_res_messages{})}).

-record(fetch_res_messages, {part        :: int32(),
                             code        :: int16(),
                             hm_offset   :: int64(),
                             size        :: int32(),
                             messages    :: list()}). %%TODO:



%% Offset API
%%  This API describes the valid offset range available for a set of topic-partitions.
%%  As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the partitions in question.
%%  This can be determined using the metadata API.
-record(offset_request, {replica         :: int32(),
                         topics          :: list(#offset_req_topic{})}).

-record(offset_req_topic, {topic         :: string(),
                           offsets       :: list(#offset_req_offsets{})}).

-record(offset_req_offsets, {part        :: int32(),
                             time        :: int64(),
                             max_num     :: int32()}).

%% response
-record(offset_response, {topics         :: list(#offset_res_topic{})}).

-record(offset_res_topic, {topic         :: string(),
                           offsets       :: list(#offset_res_offsets{})}).

-record(offset_res_offsets, {part        :: int32(),
                             code        :: int16(),
                             offset      :: int64()}).


%% Offset Commit/Fetch API



%% Group Membership API



%% Administrative API
%% ListGroups Request
%%  This API can be used to find the current groups managed by a broker.
%%  To get a list of all groups in the cluster, you must send ListGroup to all brokers.
-record(list_groups_request, {}).

-record(list_groups_response, {code       :: int16(),
                               groups     :: list(#list_groups_res_group{})}).

-record(list_groups_res_group,
                            {group        :: string(),
                             proto_type   :: string()}).

%% DescribeGroups
-record(describe_groups_request, {groups  :: list(string())}).

-record(describe_groups_response, {groups :: list()}).

-record(describe_groups_res_group,
                            {code         :: int16(),
                             group        :: string(),
                             state        :: string(),
                             proto_type   :: string(),
                             protocol     :: string(),
                             members      :: list(#describe_groups_res_group_member{})}).

-record(describe_groups_res_group_member,
                            {member       :: string(),
                             client       :: string(),
                             host         :: string(),
                             metadata     :: binary(),
                             assignment   :: binary()}).

