consume:
	go run consumer.go -brokers=localhost:9092 -group=test1 -topics=test1
produce:
	go run producer.go -brokers=localhost:9092 -topic=test1 -producers=5 -records-number=2