package chaos

import (
	"fmt"
	"net/url"
)

func proxyURL(dsn string, port int) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.Host = fmt.Sprintf("127.0.0.1:%d", port)
	params := u.Query()
	params.Del("host")
	params.Del("port")
	params.Set("sslmode", "disable")
	u.RawQuery = params.Encode()
	return u.String(), nil
}
