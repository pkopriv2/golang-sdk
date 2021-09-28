package sms

import (
	"github.com/pkg/errors"
	twilio "github.com/sfreiberg/gotwilio"
)

// ** EMAIL INTERFACES AND UTILITIES ** //
var ErrSMS = errors.New("Warden:SMS")

type TwilioClient struct {
	from string
	cl   *twilio.Twilio
}

func NewTwilioClient(from, sid, token string) *TwilioClient {
	return &TwilioClient{from, twilio.NewTwilioClient(sid, token)}
}

func (m *TwilioClient) Send(to, msg string) (err error) {
	_, exp, err := m.cl.SendSMS(m.from, to, msg, "", "")
	if err != nil {
		return
	}
	if exp != nil {
		err = errors.Wrapf(ErrSMS, "Unable to send [%v]: %v", exp.Code, exp.Message)
		return
	}
	return
}
