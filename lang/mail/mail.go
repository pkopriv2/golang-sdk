package mail

import (
	"fmt"
)

// ** EMAIL INTERFACES AND UTILITIES ** //

type Client interface {
	Send(to, subject, msg string) error
	SendHtml(to, subject, plain, html string) error
}

type Mail struct {
	Re      string
	Subject string
	Msg     string
}

type MemClient struct {
}

func NewMemClient() Client {
	return &MemClient{}
}

func (e *MemClient) Send(re, sub, msg string) error {
	fmt.Printf("Mailto(re=%v, sub=%v):\nBody:\n%v", re, sub, msg)
	return nil
}

func (e *MemClient) SendHtml(re, sub, msg, html string) error {
	fmt.Printf("Mailto(re=%v, sub=%v):\nBody:\n%v", re, sub, msg)
	return nil
}
