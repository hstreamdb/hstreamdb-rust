-module(hstreamdb).

-on_load(init/0).

-export([create_stream/5, start_producer/4, append/3]).

-export_type([producer/0, compression_type/0]).

-type producer() :: any().
-type compression_type() :: none | gzip | zstd.

init() ->
    Load = erlang:load_nif("../../target/release/libhstreamdb_erl_nifs", 0),
    io:format("~p~n", [Load]).

-spec create_stream(
    ServerUrl :: binary(),
    StreamName :: binary(),
    ReplicationFactor :: pos_integer(),
    BacklogDuration :: pos_integer(),
    ShardCount :: pos_integer()
) ->
    ok.
create_stream(
    _ServerUrl,
    _StreamName,
    _ReplicationFactor,
    _BacklogDuration,
    _ShardCount
) ->
    none.

-spec start_producer(
    ServerUrl :: binary(),
    StreamName :: binary(),
    CompressionType :: compression_type(),
    FlushSettings :: proplists:proplist()
) ->
    producer().
start_producer(_ServerUrl, _StreamName, _CompressionType, _FlushSettings) ->
    none.

-spec append(Producer :: producer(), PartitionKey :: binary(), RawPayload :: binary()) ->
    ok.
append(_Producer, _PartitionKey, _RawPayload) ->
    none.
