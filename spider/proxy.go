package spider

import (
	"log"
	"net/http"
	"net/url"
)

func CreateProxyClient(ip, port string) *http.Client {
	proxyUrl, err := url.Parse("http://" + ip + ":" + port)
	if err != nil {
		log.Fatal(err)
	}
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyUrl),
	}
	return &http.Client{
		Transport: transport,
	}
}
