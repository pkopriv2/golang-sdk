package msgs

import (
	"github.com/pkopriv2/golang-sdk/lang/mail"
	"github.com/pkopriv2/golang-sdk/lang/sms"
	"github.com/pkg/errors"
)

var ErrMissingField = errors.New("Msgs:MissingField")

func SendText(sms sms.Client, m Message) (err error) {
	var msg []byte
	if m.Short != nil {
		msg, err = m.RenderShort(m.Bindings)
		if err != nil {
			return
		}
	} else if m.Plain != nil {
		msg, err = m.RenderText(m.Bindings)
		if err != nil {
			return
		}
	} else {
		err = errors.Wrapf(ErrMissingField, "Cannot send SMS without one of [Short,Plain]: %v", m.Subject)
		return
	}

	err = sms.Send(m.Recipient, string(msg))
	return
}

func SendMail(cl mail.Client, m Message) (err error) {
	var msg []byte
	if m.Plain != nil {
		msg, err = m.RenderText(m.Bindings)
		if err != nil {
			return
		}
	} else if m.Short != nil {
		msg, err = m.RenderShort(m.Bindings)
		if err != nil {
			return
		}
	}

	var html []byte
	if m.Html != nil {
		html, err = m.RenderHtml(m.Bindings)
		if err != nil {
			return
		}
	}

	if msg == nil && html == nil {
		err = errors.Wrap(ErrMissingField, "Sending email requires one of [Short,Plain,Html]")
		return
	}

	if html == nil {
		err = errors.Wrapf(cl.Send(m.Recipient, m.Subject, string(msg)), "Unable to send message to [%v]", m.Recipient)
		return
	}

	if msg == nil {
		msg = []byte{}
	}

	err = errors.Wrapf(cl.SendHtml(m.Recipient, m.Subject, string(msg), string(html)), "Unable to send message to [%v]", m.Recipient)
	return
}
