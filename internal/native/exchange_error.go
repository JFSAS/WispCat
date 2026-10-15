package native

// ExchangeError reports transport metadata only. It never retains a request,
// response, address, account identifier, or credential.
type ExchangeError struct {
	Stage        string
	Code         int
	SSLError     int
	VerifyResult int64
	Retryable    bool
}

func (e *ExchangeError) Error() string { return "verified TLS exchange failed" }
