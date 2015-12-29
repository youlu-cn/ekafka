%%%-------------------------------------------------------------------
%%% @author luyou
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 十二月 2015 18:52
%%%-------------------------------------------------------------------
-author("luyou").


-define(API_VERSION, 0).
-define(MAGIC_BYTE, 0).

-define(REPLICA_ID, -1).

%% Api keys
-define(API_KEY_PRODUCE,        0).
-define(API_KEY_FETCH,          1).
-define(API_KEY_OFFSET,         2).
-define(API_KEY_METADATA,       3).
-define(API_KEY_LEADER_AND_ISR, 4).
-define(API_KEY_STOP_REPLICA,   5).
-define(API_KEY_OFFSET_COMMIT,  6).
-define(API_KEY_OFFSET_FETCH,   7).

%% Compression
-define(COMPRESS_NONE, 0).
-define(COMPRESS_GZIP, 1).
-define(COMPRESS_SNAPPY, 2).


%% Error code macros, mirrored (including TODOs) from:
%% https://github.com/apache/kafka/blob/0.8.2/clients/src/
%%       main/java/org/apache/kafka/common/protocol/Errors.java
-define(EC_UNKNOWN,                    'UnknownServerException').           % -1
-define(EC_NONE,                       'no_error').                         %  0
-define(EC_OFFSET_OUT_OF_RANGE,        'OffsetOutOfRangeException').        %  1
-define(EC_CORRUPT_MESSAGE,            'CorruptRecordException').           %  2
-define(EC_UNKNOWN_TOPIC_OR_PARTITION, 'UnknownTopicOrPartitionException'). %  3
%% TODO: errorCode 4 for InvalidFetchSize
-define(EC_LEADER_NOT_AVAILABLE,       'LeaderNotAvailableException').
-define(EC_NOT_LEADER_FOR_PARTITION,   'NotLeaderForPartitionException').   %  6
-define(EC_REQUEST_TIMED_OUT,          'TimeoutException').                 %  7
%% TODO: errorCode 8, 9, 11
-define(EC_MESSAGE_TOO_LARGE,          'RecordTooLargeException').          % 10
-define(EC_OFFSET_METADATA_TOO_LARGE,  'OffsetMetadataTooLarge').           % 12
-define(EC_NETWORK_EXCEPTION,          'NetworkException').                 % 13
%% TODO: errorCode 14, 15, 16
-define(EC_INVALID_TOPIC_EXCEPTION,    'InvalidTopicException').            % 17
-define(EC_RECORD_LIST_TOO_LARGE,      'RecordBatchTooLargeException').     % 18
-define(EC_NOT_ENOUGH_REPLICAS,        'NotEnoughReplicasException').       % 19
-define(EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        'NotEnoughReplicasAfterAppendException').                           % 20


-type error_code() :: atom() | integer().
