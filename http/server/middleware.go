package server

import (
	"time"
)

func buildMiddleware(fns ...Middleware) Middleware {
	return func(h Handler) Handler {
		cur := recoveryHandler(h)
		for _, fn := range fns {
			cur = fn(cur)
		}
		return cur
	}
}

func recoveryHandler(h Handler) Handler {
	return func(e Environment, req Request) (resp Response) {
		defer func() {
			if msg := recover(); msg != nil {
				e.Logger().Error("Handler panic [%v %v]: [%+v]", req.Method(), req.URL(), msg)
				resp = Reply(StatusPanic)
			}
		}()
		resp = h(e, req)
		return
	}
}

func TimerMiddleware(h Handler) Handler {
	return func(e Environment, req Request) Response {
		now := time.Now()
		defer func() {
			e.Logger().Info("Request duration [%v]", time.Now().Sub(now))
		}()
		return h(e, req)
	}
}

func RouteMiddleware(h Handler) Handler {
	return func(e Environment, req Request) Response {
		e.Logger().Info("%v %v", req.Method(), req.URL())
		return h(e, req)
	}
}
