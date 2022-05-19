# Compile
`mvn clean compile package`
# Run
`./bin/flink run -m localhost:8081 -p 1 {/path/to/repo}/target/RocksDBStateMemoryControlTestProgram.jar --environment.parallelism 1 --environment.checkpoint_interval 600000     --state_backend rocks     --state_backend.checkpoint_directory file://{path/to/checkpoint}  --state_backend.rocks.incremental true     --sequence_generator_source.sleep_time 1     --sequence_generator_source.keyspace 1000000     --sequence_generator_source.payload_size 50000     --useValueState true     --useListState true     --useMapState true     --useWindow true     --sequence_generator_source.sleep_after_elements 1`
