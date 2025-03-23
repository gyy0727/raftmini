package http

import (
	"errors"
	"fmt"
	"net/url"
)

type Peers struct {
	Urls map[uint64]url.URL
}

var peers Peers

//*AddPeer 方法将给定的 URL 字符串解析为 url.URL 并添加到 Peers 结构体的映射表中
func (t *Transport) AddPeer(id uint64, us []string) error {
	fmt.Println("addpeer ------------ ",id,"++++++",us)
	if peers.Urls == nil {
		peers.Urls = make(map[uint64]url.URL)
	}

	for _, urlStr := range us {
		parsedURL, err := url.Parse(urlStr)
		if err != nil {
			return fmt.Errorf("invalid URL: %v", err)
		}

		// 验证 URL 是否包含有效的 Scheme 和 Host
		if parsedURL.Scheme == "" || parsedURL.Host == "" {
			return errors.New("URL must include a scheme and a host")
		}

		peers.Urls[id] = *parsedURL
	}

	return nil
}

//*RemovePeer 方法根据给定的 id 从 Peers 结构体的映射表中移除对应的 URL
func (p *Transport) RemovePeer(id uint64) error {
	if _, exists := peers.Urls[id]; !exists {
		return errors.New("id does not exist")
	}

	delete(peers.Urls, id)
	return nil
}
