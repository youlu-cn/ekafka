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


-define(INT, signed-integer).

-define(DEFAULT_PRODUCER_PROCESSES, 8).



%% Kafka Protocol
%% API Key
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


%% Error Code
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


%% Types
-type int8()  :: integer().
-type int16() :: integer().
-type int32() :: integer().
-type int64() :: integer().
-type bytes() :: binary().

-type topic_name()        :: string().
-type partition_id()      :: int32().
-type group_id()          :: string().
-type group_member_id()   :: string().


%%
%% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
%%

-record(kafka_request, {api          :: int16(),
                        version      :: int16(),
                        corr_id      :: int32(),
                        client       :: int32(),
                        request      :: #metadata_request{}           |
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
                                        #describe_groups_request{}}).

-record(kafka_response, {corr_id     :: int32(),
                         response    :: #metadata_response{}          |
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
                                        #describe_groups_response{}}).


-record(topic, {name                 :: topic_name(),
                partitions           :: list(partition_id())}).


%% Kafka Message Format
-record(message, {offset             :: int64(),
                  size               :: int32(),
                  crc                :: int32(),
                  magic              :: int8(),
                  attributes         :: int8(),
                  key                :: bytes(),
                  value              :: bytes()}).

-record(message_set, {messages       :: list(#message{})}).


%%
%% Metadata API
%%

%%  This API answers the following questions:
%%  What topics exist?
%%  How many partitions does each topic have?
%%  Which broker is currently the leader for each partition?
%%  What is the host and port for each of these brokers?
%% Metadata Request, if topics is empty, the request will yield metadata for all topics
-record(metadata_request, {topics = []        :: list(topic_name())}).

%% The response contains metadata for each partition, with partitions grouped together by topic.
%%  This metadata refers to brokers by their broker id. The brokers each have a host and port.
%% Metadata Response
-record(metadata_response, {brokers           :: list(#metadata_res_broker{}),
                            topics            :: list(#metadata_res_topic{})}).

-record(metadata_res_broker, {id              :: int32(),
                              host            :: string(),
                              port            :: int32()}).

-record(metadata_res_topic, {error            :: int16(),
                             name             :: topic_name(),
                             partitions       :: list(#metadata_res_partition{})}).

-record(metadata_res_partition, {error        :: int16(),
                                 id           :: partition_id(),
                                 leader       :: int32(),
                                 replicas     :: list(int32()),
                                 isr          :: list(int32())}).


%%
%% Produce API
%%

%%  The produce API is used to send message sets to the server.
%%  For efficiency it allows sending message sets intended for many topic partitions in a single request.
%%  The produce API uses the generic message set format,
%%  but since no offset has been assigned to the messages at the time of the send the producer is free to fill in that field in any way it likes.
%% Produce Request
-record(produce_request, {acks                :: int16(),
                          timeout             :: int32(),
                          topics              :: list(#produce_req_topic{})}).

-record(produce_req_topic, {name              :: topic_name(),
                            partitions        :: list(#produce_req_partition{})}).

-record(produce_req_partition, {id            :: partition_id(),
                                size          :: int32(),
                                message_set   :: #message_set{}}).

%% Produce Response
-record(produce_response, {topics             :: list(#produce_res_topic{})}).

-record(produce_res_topic, {name              :: topic_name(),
                            partitions        :: list(#produce_res_partition{})}).

-record(produce_res_partition, {id            :: partition_id(),
                                error         :: int16(),
                                offset        :: int64()}).


%%
%% Fetch API
%%

%% The fetch API is used to fetch a chunk of one or more logs for some topic-partitions.
%%  Logically one specifies the topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages.
%% Fetch Request
-record(fetch_request, {replica               :: int32(), %% should always specify this as -1
                        max_wait              :: int32(),
                        mini_bytes            :: int32(),
                        topics                :: list(#fetch_req_topic{})}).

-record(fetch_req_topic, {name                :: topic_name(),
                          partitions          :: list(#fetch_req_partition{})}).

-record(fetch_req_partition, {id              :: partition_id(),
                              offset          :: int64(),
                              max_bytes       :: int32()}).

%% Fetch Response
-record(fetch_response, {topics               :: list(#fetch_res_topic{})}).

-record(fetch_res_topic, {name                :: topic_name(),
                          partitions          :: list(#fetch_res_partition{})}).

-record(fetch_res_partition, {id              :: partition_id(),
                              error           :: int16(),
                              hm_offset       :: int64(),
                              size            :: int32(),
                              message_set     :: #message_set{}}).



%%
%% Offset API
%%

%%  This API describes the valid offset range available for a set of topic-partitions.
%%  As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the partitions in question.
%%  This can be determined using the metadata API.
%% Offset Request
-record(offset_request, {replica              :: int32(),
                         topics               :: list(#offset_req_topic{})}).

-record(offset_req_topic, {name               :: topic_name(),
                           partitions         :: list(#offset_req_partition{})}).

-record(offset_req_partition, {id             :: partition_id(),
                               time           :: int64(),
                               max_num        :: int32()}).

%% Offset Response
-record(offset_response, {topics              :: list(#offset_res_topic{})}).

-record(offset_res_topic, {name               :: topic_name(),
                           partitions         :: list(#offset_res_partition{})}).

-record(offset_res_partition, {id             :: partition_id(),
                               error          :: int16(),
                               offset         :: int64()}).



%%
%% Offset Commit/Fetch API
%%

%%  These APIs allow for centralized management of offsets. Read more Offset Management.
%%  As per comments on KAFKA-993 these API calls are not fully functional in releases until Kafka 0.8.1.1. It will be available in the 0.8.2 release.
%% Group Coordinator Request
-record(group_coordinator_request, {id        :: group_id()}).

-record(group_coordinator_response, {error    :: int16(),
                                     id       :: int32(),
                                     host     :: string(),
                                     port     :: int32()}).


%% Offset Commit Request
%%TODO:
-record(offset_commit_request, {}).

%% Offset Commit Response
-record(offset_commit_response, {topics       :: list(#offset_commit_res_topic{})}).

-record(offset_commit_res_topic, {name        :: topic_name(),
                                  partitions  :: list(#offset_commit_res_partition{})}).

-record(offset_commit_res_partition, {id      :: partition_id(),
                                      error   :: int16()}).


%% Offset Fetch Request
-record(offset_fetch_request, {group_id       :: group_id(),
                                   topics         :: list(#offset_fetch_req_topic{})}).

-record(offset_fetch_req_topic, {name         :: topic_name(),
                                 partitions   :: list(partition_id())}).


%% Offset Fetch Response
-record(offset_fetch_response, {topics        :: list(#offset_fetch_res_topic{})}).

-record(offset_fetch_res_topic, {name         :: topic_name(),
                                 partitions   :: list(#offset_fetch_res_partition{})}).

-record(offset_fetch_res_partition, {id       :: partition_id(),
                                     offset   :: int64(),
                                     metadata :: string(),
                                     error    :: int16()}).


%%
%% Group Membership API
%%

%%  These requests are used by clients to participate in a client group managed by Kafka.
%% Join Group Request
%%  For consumer group
%%  protocol type should be "consumer"
-record(join_group_request, {id               :: group_id(),
                             timeout          :: int32(),
                             member           :: string(),
                             proto_type       :: string(),
                             protocols        :: list(#join_group_req_protocol{})}).

-record(join_group_req_protocol, {name        :: string(),
                                  version     :: int16(),
                                  subscription:: list(topic_name()),
                                  userdata    :: bytes()}).

%% Join Group Response
-record(join_group_response, {error           :: int16(),
                              generation      :: int32(),
                              protocol        :: string(),
                              leader          :: string(),
                              id              :: group_member_id(),
                              members         :: list(#join_group_res_member{})}).

-record(join_group_res_member, {id            :: group_member_id(),
                                metadata      :: bytes()}).

%% Sync Group Request
-record(sync_group_request, {id               :: group_id(),
                             generation       :: int32(),
                             member_id        :: group_member_id(),
                             assignment       :: list(#sync_group_req_assignment{})}).

-record(sync_group_req_assignment, {id        :: group_member_id(),
                                    assignment:: #group_member_assignment{}}).

%% Consumer Groups: The format of the MemberAssignment field for consumer groups is included below:
-record(group_member_assignment, {version     :: int16(),
                                  partitions  :: list(#topic{}),
                                  user_data   :: bytes()}).

%% Sync Group Response
-record(sync_group_response, {error           :: int16(),
                              assignment      :: #group_member_assignment{}}).

%% Heartbeat Request
-record(heartbeat_request, {group_id          :: group_id(),
                            generation        :: int32(),
                            member_id         :: group_member_id()}).

%% Heartbeat Response
-record(heartbeat_response, {error            :: int16()}).

%% Leave Group Request
-record(leave_group_request, {id              :: group_id(),
                              member_id       :: group_member_id()}).

%% Leave Group Response
-record(leave_group_response, {error          :: int16()}).


%%
%% Administrative API
%%

%% ListGroups Request
%%  This API can be used to find the current groups managed by a broker.
%%  To get a list of all groups in the cluster, you must send ListGroup to all brokers.
%% List Groups Request
-record(list_groups_request, {}).

%% List Groups Response
-record(list_groups_response, {error          :: int16(),
                               groups         :: list(#list_groups_res_group{})}).

-record(list_groups_res_group, {id            :: group_id(),
                                proto_type    :: string()}).

%% Describe Groups Request
-record(describe_groups_request, {groups      :: list(group_id())}).

%% Describe Groups Response
-record(describe_groups_response, {groups     :: list(#describe_groups_res_group{})}).

-record(describe_groups_res_group, {error     :: int16(),
                                    id        :: group_id(),
                                    state     :: string(),
                                    proto_type:: string(),
                                    protocol  :: string(),
                                    members   :: list(#describe_groups_res_group_member{})}).

-record(describe_groups_res_group_member,
                            {id               :: group_member_id(),
                             client           :: string(),
                             host             :: string(),
                             metadata         :: bytes(),
                             assignment       :: #group_member_assignment{}}).

