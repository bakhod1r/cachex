//go:build integration

package cachexrabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegrationNewDeclareConflict(t *testing.T) {
	c := conn(t)
	ex := exchangeName()
	ch, err := c.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(ex, amqp.ExchangeDirect, false, true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := New(Config{Conn: c, Exchange: ex}); err == nil {
		t.Fatal("redeclaring a direct exchange as fanout succeeded")
	}
}
