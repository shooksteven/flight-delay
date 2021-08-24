# Builds and runs the Spark Cluster along with the application
# Only tested in MacOS

build:
	@docker compose down
	@echo Building flight_pipeline docker image...
	@docker stop flightPipeline || true
	@docker rmi flight_pipeline:latest || true
	@docker build -t flight_pipeline:latest -t flight_pipeline:1.0.0 .

run:
	docker compose up

br:build run
