# Build

```
make
```

# Run

```
observation-encoder
```

# Sample config file

```toml
debug = true
ttl_margin = 7

[nats]
url = "nats://nats:4222"
subject_southbound = "test.out"
observation_subject_prefix = "test.observations"

[[nats.observation_buckets]]
name = "globally_new_bucket"
observation = "globally_new"
ttl = 3600
create = true

[[nats.observation_buckets]]
name = "looptest_bucket"
observation = "looptest"
ttl = 3600
create = true
```
