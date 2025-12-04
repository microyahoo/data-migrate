package common

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

func NewS3Client(ctx context.Context, endpoint, accessKey, secretKey string, region string, skipSSLVerify bool) (*s3.Client, error) {
	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: skipSSLVerify},
		// dial tcp 10.3.9.232:80: connect: cannot assign requested address
		// https://github.com/golang/go/issues/16012
		// https://github.com/valyala/fasthttp/blob/d795f13985f16622a949ea9fc3459cf54dc78b3e/client_timing_test.go#L292
		// https://github.com/etcd-io/etcd/blob/55de68d18c63fde8747f2cd9f7c2ff242346f756/client/pkg/transport/timeout_transport.go#L39
		// overrides the DefaultMaxIdleConnsPerHost = 2
		MaxIdleConnsPerHost: 1024, // allow more idle connections between peers to avoid unnecessary port allocation

		// https://gitlab.com/gitlab-org/gitlab-pages/-/merge_requests/274#note_335755221
		//
		// Not setting this results in connections in the pool staying open forever. When the pool size is 2,
		// this is unlikely to matter much, as we're highly unlikely to leave those connections idle.
		// But with a higher number of connections in the pool, we'll probably want to set this so that after a burst of activity,
		// we can close idle connections. Perhaps a value like 15 minutes for benchmark test?
		//
		// https://github.com/yarpc/yarpc-go/issues/1906
		// https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
		IdleConnTimeout: 15 * time.Minute,
	}
	// Use this Session to do things that are hidden from the performance monitoring
	// Setting up the housekeeping S3 client
	hc := &http.Client{Transport: tr}

	cfg, err := s3config.LoadDefaultConfig(ctx,
		s3config.WithRetryer(func() aws.Retryer {
			// In high concurrency scenarios, the AWS SDK experiences a large number of errors of
			// "failed to get rate limit token, retry quota exceeded, 0 available, 5 requested"
			// in default client-side rate-limiting mechanism.
			// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
			//
			// In the V2 AWS apis, in that AWS has a retry strategy if the request fails with something like a 503.
			// So by default the request will retry 3 times with a backoff wait period between each retry.
			// I think it could skew the results of a performance test. AWS provides a noop retry implementation
			// if you don't want retries at all.
			return aws.NopRetryer{}
		}),
		s3config.WithHTTPClient(hc),
		s3config.WithRegion(region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.WithError(err).Warningf("Unable to build S3 config")
		return nil, err
	}

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, nil
}
