.PHONY: dev prod

# Local development (uses docker-compose.override.yml)
local:
	@echo "Starting development environment..."
	docker compose up -d --build

# Production (explicitly skips override file)
prod:
	@echo "Starting production environment..."
	docker compose -f docker-compose.yml up -d --build

# Deploy from prebuilt image saved locally
prod-prebuilt:
	@echo "Loading prebuilt image and starting production environment..."
	docker load < drift-monitor.tar.gz
	docker compose -f docker-compose.yml up -d

# Common commands
stop:
	docker compose down

clean:
	docker compose down -v

# Services
grafana:
	open http://localhost:3000
