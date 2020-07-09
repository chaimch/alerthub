// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"

	"github.com/chaimcode/alerthub/manager"
)

func main() {
	log.Print("Starting event suppressor...")
	suppressor := manager.NewSuppressor()
	defer suppressor.Close()
	go suppressor.Dispatch()
	log.Println("Done.")

	log.Println("Starting event aggregator...")
	aggregator := manager.NewAggregator()
	defer aggregator.Close()

	/*
		summaryReqs & closed
	*/
	summarizer := new(manager.SummaryDispatcher)
	go aggregator.Dispatch(summarizer)
	log.Println("Done.")

	// done := make(chan bool)
	// go func() {
	// 	rules := manager.AggregationRules{
	// 		&manager.AggregationRule{
	// 			Filters: manager.Filters{manager.NewFilter("service", "discovery")},
	// 		},
	// 	}

	// 	aggregator.SetRules(rules)

	// 	events := manager.Events{
	// 		&manager.Event{
	// 			Payload: map[string]string{
	// 				"service": "discovery",
	// 			},
	// 		},
	// 	}

	// 	aggregator.Receive(events)

	// 	done <- true
	// }()
	// <-done

	log.Println("Running summary dispatcher...")
	summarizer.Dispatch(suppressor)
}
