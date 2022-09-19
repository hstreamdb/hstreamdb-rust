-module(hstreamdb).

-compile([nowarn_unused_vars]).

-on_load(init/0).

-export([create_stream/5, start_producer/4, stop_producer/1, append/3]).

-export_type([producer/0, compression_type/0]).

-type producer() :: any().
-type compression_type() :: none | gzip | zstd.

init() ->
    ok = erlang:load_nif("../../target/release/libhstreamdb_erl_nifs", 0),
    ok.

-spec create_stream(
    ServerUrl :: binary(),
    StreamName :: binary(),
    ReplicationFactor :: pos_integer(),
    BacklogDuration :: pos_integer(),
    ShardCount :: pos_integer()
) ->
    ok.
create_stream(ServerUrl, StreamName, ReplicationFactor, BacklogDuration, ShardCount) ->
    none.

-spec start_producer(
    ServerUrl :: binary(),
    StreamName :: binary(),
    CompressionType :: compression_type(),
    FlushSettings :: proplists:proplist()
) ->
    producer().
start_producer(ServerUrl, StreamName, CompressionType, FlushSettings) ->
    none.

-spec stop_producer(
    Producer :: producer()
) -> ok.
stop_producer(Producer) -> none.

-spec append(Producer :: producer(), PartitionKey :: binary(), RawPayload :: binary()) ->
    ok.
append(Producer, PartitionKey, RawPayload) ->
    none.
