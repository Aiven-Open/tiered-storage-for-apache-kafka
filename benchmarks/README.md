# Benchmarks

## How to run

> from https://www.baeldung.com/java-async-profiler

Enable Kernel configs:

```shell
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'
```

set `LD_LIBRARY_PATH` environment variable with async-profile build directory:

```shell
export LD_LIBRARY_PATH=/opt/async-profiler-2.9-linux-x64/build/
```

```shell
./gradlew benchmarks:installDist
```

Run benchmark:

```shell
java -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -cp "benchmarks/build/install/benchmarks/*" io.aiven.kafka.tieredstorage.benchs.transform.DetransformBench > results.txt 2>&1
```