-module(hstreamdb).

-compile([nowarn_unused_vars]).

-on_load(init/0).

-export([create_stream/5, start_producer/3, stop_producer/1, append/3, await_append_result/1]).

-export_type([producer/0, compression_type/0]).

-type producer() :: any().
-type append_result() :: any().
-type compression_type() :: none | gzip | zstd.
-type producer_setting() ::
    {compression_type, compression_type()}
    | {concurrency_limit, pos_integer()}
    | {len, non_neg_integer()}
    | {size, non_neg_integer()}.

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
    ok | {error, binary()}.
create_stream(ServerUrl, StreamName, ReplicationFactor, BacklogDuration, ShardCount) ->
    none.

-spec start_producer(
    ServerUrl :: binary(),
    StreamName :: binary(),
    ProducerSettings :: [producer_setting()]
) ->
    {ok, producer()} | {error, binary()}.
start_producer(ServerUrl, StreamName, ProducerSettings) ->
    none.

-spec stop_producer(Producer :: producer()) -> ok.
stop_producer(Producer) ->
    none.

-spec append(Producer :: producer(), PartitionKey :: binary(), RawPayload :: binary()) ->
    append_result().
append(Producer, PartitionKey, RawPayload) ->
    none.

-spec await_append_result(AppendResult :: append_result()) -> {ok, binary()} | {error, binary()}.

await_append_result(AppendResult) -> none.
