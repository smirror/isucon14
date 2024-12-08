package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/exp/rand"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

type paymentGatewayGetPaymentsResponseOne struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func requestPaymentGatewayPostPayment(
	ctx context.Context,
	client HTTPClient,
	paymentGatewayURL string,
	token string,
	param *paymentGatewayPostPaymentRequest,
	retrieveRidesOrderByCreatedAtAsc func() ([]Ride, error),
) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}

	retry := 0
	maxRetry := 5
	backoff := time.Millisecond * 100

	for {
		err := func() error {
			// POSTリクエストを送信
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)

			res, err := client.Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusNoContent {
				// GET /payments で確認
				return verifyPayments(ctx, client, paymentGatewayURL, token, retrieveRidesOrderByCreatedAtAsc)
			}
			return nil
		}()

		if err != nil {
			if retry < maxRetry {
				retry++
				time.Sleep(backoff + time.Millisecond*rand.Int(200)) // ランダムバックオフ
				continue
			} else {
				return fmt.Errorf("maximum retry limit reached: %w", err)
			}
		}
		break
	}

	return nil
}

// 決済確認用の関数
func verifyPayments(
	ctx context.Context,
	client HTTPClient,
	paymentGatewayURL string,
	token string,
	retrieveRidesOrderByCreatedAtAsc func() ([]Ride, error),
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, paymentGatewayURL+"/payments", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("[GET /payments] unexpected status code (%d)", res.StatusCode)
	}

	var payments []paymentGatewayGetPaymentsResponseOne
	if err := json.NewDecoder(res.Body).Decode(&payments); err != nil {
		return err
	}

	rides, err := retrieveRidesOrderByCreatedAtAsc()
	if err != nil {
		return err
	}

	if len(rides) != len(payments) {
		return fmt.Errorf("unexpected number of payments: %d != %d", len(rides), len(payments))
	}

	return nil
}
