package sms

import "github.com/pkopriv2/golang-sdk/lang/concurrent"

// ** SMS INTERFACES AND UTILITIES ** //

type Client interface {
	Send(to string, msg string) error
}

type Message struct {
	To  string
	Msg string
}

type MemClient struct {
	Msgs concurrent.List
}

func NewMemClient() *MemClient {
	return &MemClient{concurrent.NewList(10)}
}

func (e *MemClient) Send(to, msg string) error {
	e.Msgs.Append(Message{to, msg})
	return nil
}

func (e *MemClient) All() []Message {
	ret := make([]Message, 0, len(e.Msgs.All()))
	for _, c := range e.Msgs.All() {
		ret = append(ret, c.(Message))
	}
	return ret
}

func (e *MemClient) Search(fn func(Message) bool) []Message {
	ret := make([]Message, 0, 10)
	for _, msg := range e.All() {
		if fn(msg) {
			ret = append(ret, msg)
		}
	}
	return ret
}

func (e *MemClient) Clear() {
	e.Msgs = concurrent.NewList(128)
}
