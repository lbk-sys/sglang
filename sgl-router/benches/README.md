# SGLang Router Benchmark Tool

A benchmarking tool for SGLang Router written in Rust, capable of handling extreme concurrency levels (10,000+ concurrent requests).

## Building

```bash
# Make sure cargo is available
source ~/.cargo/env

# Build the benchmark tool
cargo build --release --bin router-benchmark
```

## Usage

The tool provides multiple benchmark modes. Run directly with:

```bash
./target/release/router-benchmark [COMMAND] [OPTIONS]
```

Or use cargo:
```bash
cargo run --release --bin router-benchmark -- [COMMAND] [OPTIONS]
```

### Basic Benchmark
Simple benchmark with fixed token count:
```bash
./target/release/router-benchmark basic -r 10000 -c 10000
```

Options:
- `-r, --requests`: Total number of requests (default: 1000)
- `-c, --concurrency`: Concurrency level (default: 100)
- `-t, --tokens`: Max new tokens (default: 50)
- `-T, --timeout`: Request timeout in seconds (default: 60)
- `-u, --url`: Router URL (default: http://0.0.0.0:8000)

### Realistic Benchmark
Benchmark with varied prompts, token counts, and streaming:
```bash
./target/release/router-benchmark realistic -r 5000 -c 5000 --stream
```

Options:
- `-r, --requests`: Total number of requests (default: 1000)
- `-c, --concurrency`: Concurrency level (default: 100)
- `--min-tokens`: Minimum tokens (default: 100)
- `--max-tokens`: Maximum tokens (default: 500)
- `-s, --stream`: Enable streaming (default: true)
- `-T, --timeout`: Request timeout in seconds (default: 120)
- `-u, --url`: Router URL (default: http://0.0.0.0:8000)

### Progressive Load Test
Gradually increase concurrency to find breaking point:
```bash
./target/release/router-benchmark progressive --start-concurrency 100 --max-concurrency 10000 --step 500
```

Options:
- `--start-concurrency`: Starting concurrency (default: 10)
- `--max-concurrency`: Maximum concurrency (default: 1000)
- `--step`: Step size for increasing concurrency (default: 10)
- `--requests-per-level`: Requests per concurrency level (default: 100)
- `-T, --timeout`: Request timeout in seconds (default: 60)
- `-u, --url`: Router URL (default: http://0.0.0.0:8000)

### Sustained Load Test
Run continuous load for a specified duration:
```bash
./target/release/router-benchmark sustained -c 1000 -d 60
```

Options:
- `-c, --concurrency`: Concurrency level (default: 100)
- `-d, --duration`: Duration in seconds (default: 60)
- `-t, --tokens`: Max new tokens (default: 50)
- `-s, --stream`: Enable streaming (default: false)
- `-T, --timeout`: Request timeout in seconds (default: 60)
- `-u, --url`: Router URL (default: http://0.0.0.0:8000)

### Help
View all available options:
```bash
./target/release/router-benchmark --help
./target/release/router-benchmark basic --help
```

## Report Generation

The benchmark tool can generate detailed reports in JSON and text formats:

```bash
# Generate JSON report
./target/release/router-benchmark --json-report basic -r 1000 -c 100

# Specify custom report directory
./target/release/router-benchmark --report-dir ./my_reports --json-report basic -r 1000 -c 100
```

Reports include:
- Configuration details (URL, concurrency, token counts, etc.)
- Performance metrics (success rate, throughput, response times)
- Response time percentiles (P50, P75, P90, P95, P99)
- System information (OS, CPU count)

Reports are saved with timestamps in the format:
- JSON: `<benchmark_type>_<timestamp>.json`
- Text: `<benchmark_type>_<timestamp>.txt`
