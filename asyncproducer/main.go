package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"os"

	_ "net/http/pprof"

	metrics "github.com/rcrowley/go-metrics"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers   = os.Getenv("BROKERS")
	version   = sarama.DefaultVersion.String()
	topic     = os.Getenv("TOPIC")
	producers = 1
	verbose   = false

	recordsNumber int64 = 1
	recordsRate         = metrics.GetOrRegisterMeter("records.rate", nil)

	MessageStart = 301
	MessageEnd   = 311
)

func init() {
	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func main() {

}
