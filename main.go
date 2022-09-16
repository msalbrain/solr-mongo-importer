package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	// "go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/yaml.v2"
)

// use the ping method to test for connection
// ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
// defer cancel()
// err = client.Ping(ctx, readpref.Primary())

// this is application works similarly to mongo-connector
// the first function mongolizer checkes for if solr is upto date with
// the data present in mongo replica set i.e (len(my_core)) == (len(collection))
// if false:
// check for difference and start updating after skipping len(my_core)
// afterwards mongolizer() ends and NightWatch() starts
// NightWatch looks out for any insert changes in oplog and makes respective update to solr.

type Config struct {
	Solr struct {
		Core       string `yaml:"core"`
		Url        string `yaml:"url"`
		DataLenght int    `yaml:"data_lenght"`
	} `yaml:"solr"`

	Mongo struct {
		Db         string `yaml:"db"`
		Collection string `yaml:"collection"`
		Url        string `yaml:"url"`
		Timeout    int    `yaml:"timeout"`
	} `yaml:"mongodb"`
}

// NewConfig returns a new decoded Config struct
func NewConfig(configPath string) (*Config, error) {
	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

func push_to_solr(j_data []map[string]interface{}, config *Config) {
	var copy_j_data []map[string]interface{}

	copy(copy_j_data, j_data)
	for _, i := range j_data {
		delete(i, "_id")
		i["is_real"] = true
		copy_j_data = append(copy_j_data, i)
	}
	// fmt.Print(copy_j_data)
	
	b := fmt.Sprintf(config.Solr.Url + "/solr/" + config.Solr.Core + "/update?_=%d&commitWithin=1000&overwrite=true&wt=json", time.Now().Unix())
	// b = "http://localhost:8000/hello"

	j, _ := json.Marshal(copy_j_data)
	resp, err := http.Post(b, "application/json", bytes.NewBuffer(j))

	if err != nil {
		log.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

}

func Mongolizer(config *Config) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Mongo.Timeout)*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.Mongo.Url))

	defer func() {

		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}

		}()

	collection := client.Database("scrape").Collection("twitter")

	cur, err := collection.Find(ctx, bson.D{{}})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)

	var wg sync.WaitGroup

	i := 0
	rep := []map[string]interface{}{}
	req := 0
	for cur.Next(ctx) {
		i++
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}

		wg.Add(1)

		rep = append(rep, result)
		if req%19 == 0 {
			time.Sleep(200 * time.Millisecond) // after 19 requests sleep for 0.2 sec
		} else if i%config.Solr.DataLenght == 0 {
			req++
			go func(j_data []map[string]interface{}) {

				push_to_solr(j_data, config)
				defer wg.Done()
			}(rep)
			rep = rep[:0]
			time.Sleep(10 * time.Millisecond)
		}

	}

	wg.Wait()
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

}

func NightWatch(config *Config) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Mongo.Timeout)*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.Mongo.Url))

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	coll := client.Database(config.Mongo.Db).Collection(config.Mongo.Collection)
	pipeline := mongo.Pipeline{bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}}}

	cs, err := coll.Watch(context.TODO(), pipeline)
	if err != nil {
		panic(err)
	}
	defer cs.Close(context.TODO())
	fmt.Println("Waiting For Change Events. Insert something in MongoDB!")
	for cs.Next(context.TODO()) {
		var event bson.M
		if err := cs.Decode(&event); err != nil {
			panic(err)
		}
		output, err := json.MarshalIndent(event["fullDocument"], "", "    ")
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n", output)
	}
	if err := cs.Err(); err != nil {
		panic(err)
	}
}

func main() {
	configure, err := NewConfig("config.yml")
	if err == nil {
		fmt.Println(*configure)
		Mongolizer(configure)
		NightWatch(configure)
	}else if err != nil {
		log.Fatal(err)
	}
}

