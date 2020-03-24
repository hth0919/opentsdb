package opentsdb

import (
	"encoding/json"
	"fmt"
	"log"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/url"
	"strings"
)

type RawRequest struct {
	Method   string
	HttpPath string
	Values   string
}

func NewRequest(method, absPath, values string) *RawRequest {
	return &RawRequest{
		Method:   method,
		HttpPath: absPath,
		Values:   values,
	}
}

//type Options map[string]interface{}
func (c *Client) putMetrics(metrics []*UniMetric) (putResp *PutResponse, err error) {
	if metrics == nil || len(metrics) == 0 {
		return nil, fmt.Errorf("query is nil or empty")
	}

	metricsStr, _ := json.Marshal(metrics)
	log.Println("put.metrics", string(metricsStr))
	req := NewRequest("POST", c.putUrl, string(metricsStr))

	rr, err := c.SendRequest(req)
	log.Printf("put.post.resp statusCode:%d body:%s", string(rr.StatusCode), string(rr.Body))
	putResp = &PutResponse{
		StatusCode: rr.StatusCode,
		RespInfo:   string(rr.Body),
	}
	return
}

func (c *Client) queryGet(query *QueryRequestGet) (queryResp *QueryResponse, errResp *ErrorResponse, err error) {
	if query == nil {
		return nil, nil, fmt.Errorf("query is nil")
	}

	httpPath := c.queryUrl
	startTimeOptions := fmt.Sprintf("?start=%s", query.Start)
	httpPath += startTimeOptions
	options := PackQueryString(query)
	httpPath += options
	log.Printf("query.get:%s", options)

	req := NewRequest("GET", httpPath, "")
	rr, err := c.SendRequest(req)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("query.get.resp statusCode:%d body:%s", string(rr.StatusCode), string(rr.Body))
	queryResp, errResp, err = rr.DecodeQueryResp()
	if err != nil {
		return nil, nil, err
	}
	return
}

func (c *Client) queryPost(query *QueryRequestPost) (queryResp *QueryResponse, errResp *ErrorResponse, err error) {
	if query == nil {
		return nil, nil, fmt.Errorf("query is nil")
	}

	httpPath := c.queryUrl
	metricsStr, _ := json.Marshal(query)
	log.Println("query.post", string(metricsStr))

	req := NewRequest("POST", httpPath, string(metricsStr))
	rr, err := c.SendRequest(req)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("query.post.resp statusCode:%d body:%s", string(rr.StatusCode), string(rr.Body))
	queryResp, errResp, err = rr.DecodeQueryResp()
	if err != nil {
		return nil, nil, err
	}
	return
}

var RETRY_TIMES = 3

func (c *Client) SendRequest(rr *RawRequest) (*RawResponse, error) {
	var req *http.Request
	var resp *http.Response
	var respBody []byte
	var err error

	for attempt := 0; attempt < RETRY_TIMES; attempt++ {
		log.Println("Connecting to opentsdb ", attempt+1, " for ", rr.HttpPath, " | method ", rr.Method)

		req, err := func() (*http.Request, error) {
			if rr.Values == "" {
				if req, err = http.NewRequest(rr.Method, rr.HttpPath, nil); err != nil {
					log.Println("http.NewRequest err", err, "request", *rr)
					return nil, err
				}
			} else {
				body := strings.NewReader(rr.Values)
				if req, err = http.NewRequest(rr.Method, rr.HttpPath, body); err != nil {
					log.Println("http.NewRequest err", err, "request", *rr)
					return nil, err

				}
			}
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Content-Type", "application/json")

			return req, nil
		}()
		resp, err = c.httpClient.Do(req)
		if err != nil {
			log.Println("network error: ", err.Error())
			continue
		}

		log.Println("recv.response.from ", rr.HttpPath)
		// valid http status code
		if validHttpStatusCode[resp.StatusCode] {
			respBody, err = ioutil.ReadAll(resp.Body)
			if err == nil {
				log.Println("recv.success", rr.HttpPath)
				break
			}
			if err == io.ErrUnexpectedEOF {
				respBody = []byte{}
				break
			}
		}
		resp.Body.Close()
	}
	log.Println("resp", *resp)
	if resp == nil {
		return nil, err
	}
	r := &RawResponse{
		StatusCode: resp.StatusCode,
		Body:       respBody,
		Header:     resp.Header,
	}

	return r, err
}
