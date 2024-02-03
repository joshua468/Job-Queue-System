package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var ctx = context.Background()

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()
	go startWorker(client)

	for i := 1; i <= 5; i++ {
		jobID := uuid.New().String()
		enqueueJob(client, jobID, fmt.Sprintf("Job %d", i))
	}
	time.Sleep(10 * time.Second)
}

func startWorker(client *redis.Client) {
	for {
		jobID, jobData, err := dequeueJob(client)
		if err != nil {
			fmt.Println("Error", err)
			continue
		}
		fmt.Printf("Processing job %s: %s\n", jobID, jobData)
	}
}

func enqueueJob(client *redis.Client, jobID, jobData string) {
	client.ZAdd(ctx, "jobs", &redis.Z{Member: jobID, Score: float64(time.Now().Unix() + 5)})
	client.Set(ctx, "job:"+jobID, jobData, 0)
	fmt.Printf("Enqueued Job %s: %s\n", jobID, jobData)
}

func dequeueJob(client *redis.Client) (string, string, error) {
	result, err := client.ZPopMin(ctx, "jobs").Result()
	if err != nil {
		return "", "", err
	}
	jobID := result[0].Member.(string)
	jobData, _ := client.Get(ctx, "job:"+jobID).Result()

	client.Del(ctx, "job:"+jobID)
	return jobID, jobData, nil
}
