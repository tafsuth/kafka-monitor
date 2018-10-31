deps:
	go get -u github.com/Shopify/sarama
	go get -u golang.org/x/sync/errgroup
docker-up:
	docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`ipconfig getifaddr en0` --env ADVERTISED_PORT=9092 spotify/kafka
