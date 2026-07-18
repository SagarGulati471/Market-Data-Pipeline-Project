## Logging Configuration

This project uses Python's hierarchical logging system to control log verbosity independently for application code and third-party libraries.

### Why?

During development, application logs are useful for debugging, while third-party libraries (such as `aiokafka`, `asyncio`, and `urllib3`) can generate a large amount of noisy logs that make it difficult to identify issues in our own code.

The logging configuration allows us to:

* Keep application modules verbose (e.g., `DEBUG`) during development.
* Reduce noise from third-party libraries (e.g., `WARNING` or `ERROR`).
* Configure log levels through environment variables without changing code.

---

## Logger Hierarchy

Python loggers are hierarchical and follow the module/package structure.

For example:

```text
ROOT
│
├── consumers
│   ├── normalizer
│   └── candle_builder
│
├── messaging
│   └── kafka_producer
│
└── aiokafka
    ├── consumer
    └── producer
```

A logger created inside `consumers/candle_builder.py` using:

```python
logger = logging.getLogger(__name__)
```

is automatically named:

```text
consumers.candle_builder
```

Since it is a child of the `consumers` logger, it inherits its log level unless explicitly overridden.

---

## How the Configuration Works

### 1. Configure the Root Logger

```python
logging.basicConfig(level=THIRD_PARTY_LOG_LEVEL, ...)
```

The root logger provides the default log level for all loggers in the application.

We intentionally configure the root logger using the third-party log level so that external libraries remain quiet by default.

Example:

```env
THIRD_PARTY_LOG_LEVEL=WARNING
```

All third-party libraries inherit `WARNING` unless configured otherwise.

---

### 2. Override Project Loggers

The project defines a list of top-level package names:

```python
PROJECT_LOGGERS = (
    "config",
    "consumers",
    "data_collectors",
    "messaging",
    "storage",
    "utils",
)
```


Please Note -
Each parent logger is configured with the application log level:

If we are in a file let's say called consumers/candle_builder.py
And there we print(__main__) , then it will give consumers.candle_builder

Now if we are setting the log level on consumers, then the candle_builder will also inherit the same log level by default, if we want to have a different log levels in different files of module consumers, then in each file we will need to set different log level.


```python
logging.getLogger("consumers").setLevel(DEBUG)
```

This automatically applies to all child modules, for example:

* `consumers.normalizer`
* `consumers.candle_builder`
* `consumers.trade_consumer`

No per-file configuration is required.

---

### 3. Configure Noisy Libraries Separately

Certain libraries (especially Kafka clients) can produce excessive debug logs.

To avoid this, we configure them independently:

```env
LOG_LEVEL=DEBUG
THIRD_PARTY_LOG_LEVEL=WARNING
KAFKA_LOG_LEVEL=ERROR
```

The Log Levels have to be set in the env file.

Result:

| Logger                | Effective Level |
| --------------------- | --------------- |
| Project modules       | DEBUG           |
| Third-party libraries | WARNING         |
| aiokafka              | ERROR           |

This keeps application logs detailed while suppressing unnecessary polling and internal library logs.

---

## Environment Variables

| Variable                | Purpose                                  | Default   |
| ----------------------- | ---------------------------------------- | --------- |
| `LOG_LEVEL`             | Log level for project modules            | `INFO`    |
| `THIRD_PARTY_LOG_LEVEL` | Default log level for external libraries | `WARNING` |
| `KAFKA_LOG_LEVEL`       | Log level specifically for `aiokafka`    | `WARNING` |

---

## Notes

* `logging.getLogger(__name__)` automatically names each logger using its module path (for example, `consumers.candle_builder`).
* Child loggers inherit the log level of their nearest configured parent.
* `force=True` in `logging.basicConfig()` resets any existing logging configuration before applying the project's configuration, ensuring consistent behavior across environments.
