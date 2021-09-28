package net

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTcpListener_Close(t *testing.T) {
	listener, _ := ListenTCP4("localhost:0")
	assert.Nil(t, listener.Close())
}

func TestTcpListener_Accept(t *testing.T) {
	listener, _ := ListenTCP4("localhost:0")
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		assert.NotNil(t, conn)
		assert.Nil(t, err)
		conn.Close()
	}()

	conn, err := listener.Connect(time.Second)
	assert.NotNil(t, conn)
	assert.Nil(t, err)
	conn.Close()
}

func TestTcpListener_Read_Write(t *testing.T) {
	listener, _ := ListenTCP4("localhost:0")
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		defer conn.Close()
		for i := 0; i < 1024; i++ {
			if _, err := conn.Write([]byte{byte(i)}); err != nil {
				t.Fail()
			}
		}
	}()

	buf := make([]byte, 1024)

	conn, _ := listener.Connect(time.Second)
	defer conn.Close()
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.FailNow()
	}

	for i := 0; i < 1024; i++ {
		assert.Equal(t, byte(i), buf[i])
	}
}
