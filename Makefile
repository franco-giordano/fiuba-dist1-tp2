SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

docker-image:
	docker build -f ./filter-q1/Dockerfile -t "filter-q1:latest" .
	docker build -f ./fanout-matches/Dockerfile -t "fanout-matches:latest" .
	docker build -f ./fanout-players/Dockerfile -t "fanout-players:latest" .
	docker build -f ./filter-rating/Dockerfile -t "filter-rating:latest" .
	docker build -f ./shard-exchanger-q2/Dockerfile -t "shard-exchanger-q2:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 1
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
