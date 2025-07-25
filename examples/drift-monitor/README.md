# drift-monitor

Real-time monitoring dashboards for Drift protocol liquidations and shred processing system performance, built with `unshred`.

`drift-monitor` is packaged into a docker-compose deployment comprising containers for:
* `drift-monitor`
* ClickHouse
* Prometheus
* Grafana
* (in the local deployment) a mock validator

## Run
### Local
There is a mock validator at `src/bin/mock_validator.rs` whose sole purpose is to send a lot of correctly formatted mock shreds containing various transaction types to `drift-monitor`.
```bash
# Copy env
cp env.example .env
# Set variables (defaults should be fine for local)

# Build and start everything including mock validator sending shreds
make local

# Open dashboards at http://localhost:3000
make grafana

# Stop
make stop  # Stop everything
make clean # Stop everything and remove volumes
```

### Prod
```bash
# Copy env
cp env.example .env
# Set variables e.g. `BIND_HOST=0.0.0.0`

# Build and start everything
make prod
```
