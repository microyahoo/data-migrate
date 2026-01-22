package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type FeishuCardSender struct {
	webhookURL string
}

func NewFeishuCardSender(webhookURL string) *FeishuCardSender {
	return &FeishuCardSender{
		webhookURL: webhookURL,
	}
}

// Send 发送卡片消息
func (s *FeishuCardSender) Send(content string) error {
	type M map[string]any
	card := M{
		"msg_type": "interactive",
		"card": M{
			"schema": "2.0",
			"config": M{
				"update_multi": true,
				"style": M{
					"text_size": M{
						"normal_v2": M{
							"default": "normal",
							"pc":      "normal",
							"mobile":  "heading",
						},
					},
				},
			},
			"header": M{
				"title": M{
					"tag":     "plain_text",
					"content": "容量统计任务状态",
				},
				"subtitle": M{
					"tag":     "plain_text",
					"content": "",
				},
				"template": "blue",
				"padding":  "12px 12px 12px 12px",
			},
			"body": M{
				"direction": "vertical",
				"padding":   "12px 12px 12px 12px",
				"elements": []M{
					{
						"tag":        "markdown",
						"text_align": "left",
						"text_size":  "normal_v2",
						"margin":     "0px 0px 0px 0px",
						"content":    content,
					},
				},
			},
		},
	}

	data, err := json.Marshal(card)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", s.webhookURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errorResp map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&errorResp); err == nil {
			log.Printf("Feishu API error response: %v", errorResp)
		}
		return fmt.Errorf("webhook returned status: %d", resp.StatusCode)
	}

	return nil
}

func SendFeishuCard(webhookURL, content string) error {
	if content == "" {
		return nil
	}
	sender := NewFeishuCardSender(webhookURL)
	return sender.Send(content)
}
