# Build

```
make
```

# Install

```
sudo make install
```

# Run

```
observation-encoder
```

# Sample config file

```toml
debug = true
ttl_margin = 5 # five seconds added to outgoing observations TTL

[nats]
url = "nats://127.0.0.1:4222"
subject_southbound = "leontest.down.tapir-pop"
observation_subject_prefix = "leontest.observations"

[[nats.buckets]]
name = "globally_new"
ttl = 30

[[nats.buckets]]
name = "looptest"
ttl = 10

[cert]
active = false

[api]
active = false

[libtapir]
debug = true
```
