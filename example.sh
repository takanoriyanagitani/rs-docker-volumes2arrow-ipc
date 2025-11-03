#!/bin/sh

echo example 1 using arrow-cat
./rs-docker-volumes2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./rs-docker-volumes2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'docker_volumes' \
	--sql "
		SELECT
			name,
			driver,
			created_at
		FROM docker_volumes
		ORDER BY created_at DESC
		LIMIT 3
	" |
	rs-arrow-ipc-stream-cat
