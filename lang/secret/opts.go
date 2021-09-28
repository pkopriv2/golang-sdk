package secret

// Options for generating a shared secret.
type SecretOptions struct {
	Algorithm Type
	Entropy   int
}

func defaultSecretOptions() SecretOptions {
	return SecretOptions{Lines, 32}
}

func BuildSecretOptions(fns ...func(*SecretOptions)) SecretOptions {
	ret := defaultSecretOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}
