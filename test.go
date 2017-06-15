package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"golang.org/x/sync/errgroup"

	"os"

	"strconv"

	"io"

	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/olivere/elastic.v5"
)

type profile struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	errorlog := log.New(os.Stdout, "APP ", log.LstdFlags)

	// Obtain a client. You can also provide your own HTTP client here.
	client, err := elastic.NewClient(elastic.SetErrorLog(errorlog))
	if err != nil {
		// Handle error
		panic(err)
	}
	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping("http://127.0.0.1:9200").Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	exists, err := client.IndexExists("projects").Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `
{
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "_default_": {
            "_all": {
                "enabled": true
            }
        },
        "profile": {
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "age": {
                    "type": "long"
                }
            }
        }
    }
}
`

		createIndex, err := client.CreateIndex("projects").Body(mapping).Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}
	profile1 := profile{Name: "peezzzz", Age: 25}
	put1, err := client.Index().Index("projects").Type("profile").Id("1").BodyJson(profile1).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Indexed ProjectS %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	profile2 := `{"name" : "pee", "age" : 22}`
	put2, err := client.Index().Index("projects").
		Type("profile").
		Id("2").
		BodyString(profile2).
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed ProjectS %s to index %s, type %s\n", put2.Id, put2.Index, put2.Type)

	get1, err := client.Get().
		Index("projects").
		Type("profile").
		Id("2").
		Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}

	if get1.Found {
		fmt.Printf("Got document %s in version %d from index %s, type %s\n", get1.Id, *get1.Version, get1.Index, get1.Type)
		//byteField, _ := json.Marshal(get1.Fields)

	}
	updateprofile := profile{Name: "peeupdate", Age: 555}
	update, err := client.Update().Index("projects").Type("profile").Id("1").Doc(updateprofile).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Update by ID : %s\n", update.Id)
	deleteprofile, err := client.Delete().Index("projects").Type("profile").Id("2").Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Delete by ID : %s\n", deleteprofile.Id)
	getbyquery := elastic.NewTermQuery("name", "peeupdate")
	serarchResult, err := client.Search().Index("projects").
		Query(getbyquery).Sort("name", true).
		From(0).Size(10).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Query took %d milliseconds\n", serarchResult.TookInMillis)
	insert3 := `{"name":"pee the new insert","age":99}`
	put3, err := client.Index().Index("projects").Type("profile").Id("3").BodyString(insert3).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Compelte to insert %s\n", put3.Index)
	scr := elastic.NewScript("ctx._source.age += params.num").Param("num", 1000)
	upsert, err := client.Update().Index("projects").Type("profile").Id("555").
		Script(scr).Upsert(map[string]interface{}{"name": "peeupsert", "age": 999}).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("This is my upsert %s\n", upsert.Id)
	// deleteprofileall, err := client.Delete().Index("projects").Type("profile").Do(context.Background())
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("Delete Index %s complete!", deleteprofileall.Index)
	n := 0
	for i := 0; i < 2; i++ {
		bulkRequest := client.Bulk()
		for j := 0; j < 10000; j++ {
			n++
			str := fmt.Sprintf("%s-%d", "pee", n)
			profilebulk := profile{Name: str, Age: n + 20}
			req := elastic.NewBulkIndexRequest().Index("projects").Type("profile").Id(strconv.Itoa(n)).Doc(profilebulk)
			bulkRequest = bulkRequest.Add(req)
		}
		bulkResponse, err := bulkRequest.Do(context.Background())
		if err != nil {
			panic(err)
		}
		if bulkResponse != nil {

		}
		fmt.Printf("round %d is complete! \n", i+1)

	}

	ee := 0
	for i := 0; i < 1; i++ {
		blukRequest := client.Bulk()
		for j := 0; j < 10000; j++ {
			ee++
			str := fmt.Sprintf("Pee_Update Na'Ja-%d", ee)
			profilebulkupdate := profile{Name: str, Age: n + 1}
			req := elastic.NewBulkUpdateRequest().Index("projects").Type("profile").Id(strconv.Itoa(ee)).Doc(profilebulkupdate)
			blukRequest = blukRequest.Add(req)
		}
		bulkResponse, err := blukRequest.Do(context.Background())
		if err != nil {
			panic(err)
		}
		if bulkResponse != nil {

		}
		fmt.Printf("Update round %d is complete!\n", i+1)
	}
	// for i := 0; i < 1; i++ {
	// 	bulkRequest := client.Bulk()
	// 	for j := 0; j < 5000; j++ {
	// 		ee++
	// 		req := elastic.NewBulkDeleteRequest().Index("projects").Type("profile").Id(strconv.Itoa(ee))
	// 		bulkRequest = bulkRequest.Add(req)
	// 	}
	// 	bulkResponse, err := bulkRequest.Do(context.Background())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if bulkResponse != nil {

	// 	}
	// 	fmt.Printf("Delete round %d is complete!\n", i+1)
	// }
	for i := 0; i < 1; i++ {
		blukRequest := client.Bulk()
		for j := 0; j < 9000; j++ {
			ee--
			str := fmt.Sprintf("Pee_UpSert Na'Ja-%d", ee)
			profilebulkupdate := profile{Name: str, Age: n + 1}
			req := elastic.NewBulkUpdateRequest().Index("projects").Type("profile").Id(strconv.Itoa(ee)).Doc(profilebulkupdate).DocAsUpsert(true)
			blukRequest = blukRequest.Add(req)
		}
		bulkResponse, err := blukRequest.Do(context.Background())
		if err != nil {
			panic(err)
		}
		if bulkResponse != nil {

		}
		fmt.Printf("UpSert round %d is complete!\n", i+1)
	}
	total, err := client.Count("projects").Type("profile").Do(context.Background())
	if err != nil {

	}
	bar := pb.StartNew(int(total))
	fmt.Println(bar)
	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		scroll := client.Scroll("projects").Type("profile").Size(100)
		for {
			result, err := scroll.Do(context.Background())
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, hit := range result.Hits.Hits {
				hits <- *hit.Source
			}
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for hit := range hits {
				// Deserialize
				var p profile
				err := json.Unmarshal(hit, &p)
				if err != nil {
					return err
				}
				fmt.Printf("Projects : Profile by %s Age: %d\n", p.Name, p.Age)
				// Do something with the product here, e.g. send it to another channel
				// for further processing.
				_ = p

				bar.Increment()

				// Terminate early?
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		panic(err)
	}

	// Done.
	bar.FinishPrint("Done")
}
