package mail

import (
	"fmt"

	mailgun "gopkg.in/mailgun/mailgun-go.v1"
)

// ** EMAIL INTERFACES AND UTILITIES ** //

type MailGunClient struct {
	dom  string
	cl   mailgun.Mailgun
	tags []string
}

func NewMailGunClient(dom string, apiKey string, publicKey string, tags []string) Client {
	if tags == nil {
		tags = []string{}
	}
	return &MailGunClient{dom, mailgun.NewMailgun(dom, apiKey, publicKey), tags}
}

func (m *MailGunClient) Send(to, sub, msg string) (err error) {
	message := mailgun.NewMessage(
		fmt.Sprintf("admin@%v", m.dom),
		sub,
		msg,
		to)
	for _, t := range m.tags {
		message.AddTag(t)
	}
	_, _, err = m.cl.Send(message)
	return
}

func (m *MailGunClient) SendHtml(to, sub, plain, html string) (err error) {
	message := mailgun.NewMessage(
		fmt.Sprintf("admin@%v", m.dom),
		sub,
		plain,
		to)
	message.SetHtml(html)
	for _, t := range m.tags {
		message.AddTag(t)
	}
	_, _, err = m.cl.Send(message)
	return
}
