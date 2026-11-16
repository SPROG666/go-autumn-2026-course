package receipts

import (
	"context"
	"sync"
	"time"
)

// Задание 1: общая map и Mutex. Каркас ожидания уже дан.
func PrintAllReceipts(items []ReceiptRequest, work ReceiptPrinter) []Receipt {
	results := make(map[int]Receipt)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for index, item := range items {
		wg.Go(func() {
			result := work(item)
			mu.Lock()
			results[index] = result
			mu.Unlock()
		})
	}
	wg.Wait()
	output := make([]Receipt, len(items))
	for index := range items {
		output[index] = results[index]
	}
	return output
}

// Задание 2: Fan-out / Fan-in. Каркас объединения потоков уже дан.
func FanOut(in <-chan ReceiptRequest, workers int, work ReceiptPrinter) []<-chan Receipt {
	if workers <= 0 {
		return nil
	}
	streams := make([]<-chan Receipt, workers)
	for index := range streams {
		out := make(chan Receipt)
		streams[index] = out
		go func(out chan<- Receipt) {
			defer close(out)
			for item := range in {
				out <- work(item)
			}
		}(out)

	}
	return streams
}
func FanIn(streams ...<-chan Receipt) <-chan Receipt {
	out := make(chan Receipt)
	var wg sync.WaitGroup
	for _, stream := range streams {
		wg.Go(func() {
			for receipt := range stream {
				out <- receipt
			}
		})
	}
	go func() { wg.Wait(); close(out) }()
	return out
}

// Задание 3: таймер или тикер и контекст.
func RunCashReport(ctx context.Context, interval time.Duration, report func()) error {
	if interval <= 0 {
		return ErrInvalidInterval
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			report()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
