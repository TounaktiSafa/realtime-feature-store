.PHONY: up down topics reset live backfill dry features-backfill features-live

up:
	docker compose up -d

down:
	docker compose down

topics:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
		--topic login_events --partitions 3 --replication-factor 1 \
		--bootstrap-server localhost:9092

# Live stream: ~20 logins/sec, drift kicks in after 5000 events
live:
	python generator/generate.py --rate 20 --drift-after 5000

# Fast history with simulated past timestamps (training data)
backfill:
	python generator/generate.py --backfill 200000 --start 2026-07-01

dry:
	python generator/generate.py --backfill 20 --dry-run

# Wipe the topic (use before re-running a backfill so there are no duplicates)
reset:
	-docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic login_events --bootstrap-server localhost:9092
	sleep 3
	$(MAKE) topics

# Spark: process everything currently in Kafka, then stop
features-backfill:
	python spark/features_job.py --mode backfill --fresh

# Spark: keep running, process new events continuously
features-live:
	python spark/features_job.py --mode live

