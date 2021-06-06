#!/bin/bash
docker build -f client/Dockerfile -t "client:latest" .
docker run -i --network=fiuba-dist1-tp2_testing_net client
