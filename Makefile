build: build-consumer build-producer
build-consumer:
	@go build -o consumer consumer.go apm.go
build-producer:
	@go build -o producer producer.go apm.go
