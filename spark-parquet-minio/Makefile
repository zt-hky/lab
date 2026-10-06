# Sửa tham số trong .env hoặc override: make read READ_COLUMNS=10
up:      ## MinIO + master + workers + history
	docker compose up -d minio-init spark-master spark-worker spark-history
gen:     ## dump dữ liệu vào MinIO
	docker compose run --rm --use-aliases --service-ports gen
read:    ## đọc parquet, in mức độ song song
	docker compose run --rm --use-aliases --service-ports read
down:
	docker compose down
clean:   ## xoá cả data/event log
	docker compose down -v
