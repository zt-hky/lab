# Parameters come from .env; override per invocation, e.g. `make read READ_COLUMNS=10`.
up:    ## Start MinIO, Spark master/workers and the history server
	docker compose up -d minio-init spark-master spark-worker spark-history
gen:   ## Generate test Parquet data into MinIO
	docker compose run --rm --use-aliases --service-ports gen
read:  ## Read the dataset and report task-level parallelism
	docker compose run --rm --use-aliases --service-ports read
down:  ## Stop containers, keep volumes
	docker compose down
clean: ## Stop containers and delete volumes (data, event logs, ivy cache)
	docker compose down -v
