package main

import (
  "container/heap"
	"fmt"
	"hash/fnv"
	"log"
	"regexp"
	"sort"
	"time"
)

// Event models an action triggered by Prometheus.
type Event struct {
	// CreatedAt indicates when the event was created.
	CreatedAt time.Time

	// ExpiresAt is the allowed lifetime for this event before it is reaped.
	ExpiresAt time.Time

	Payload map[string]string
}

func (e Event) Fingerprint() uint64 {
	keys := []string{}

	for k, _ := range e.Payload {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	summer := fnv.New64a()

	for _, k := range keys {
		fmt.Fprintf(summer, k, e.Payload[k])
	}

	return summer.Sum64()
}

type Events []*Event

type ruleState uint

type aggregationState int

const (
	aggIdle aggregationState = iota
	aggEmitting
)

type AggregationRule struct {
	Filters *Filters

	// BUG(matt): Unsupported.
	RepeatRate time.Duration

	fingerprint uint64
}

func NewAggregationRule(filters ...*Filter) *AggregationRule {
	f := new(Filters)
	heap.Init(f)
	for _, filter := range filters {
		heap.Push(f, filter)
	}

	return &AggregationRule{
		Filters:     f,
		fingerprint: f.fingerprint(),
	}
}

type AggregationInstance struct {
	Rule   *AggregationRule
	Events Events

	// BUG(matt): Unsupported.
	EndsAt time.Time

	state aggregationState
}

func (r *AggregationRule) Handles(e *Event) bool {
	return r.Filters.Handle(e)
}

func (r *AggregationInstance) Ingest(e *Event) {
	r.Events = append(r.Events, e)
}

func (r *AggregationInstance) Tidy() {
	// BUG(matt): Drop this in favor of having the entire AggregationInstance
	// being dropped when too old.
	log.Println("Tidying...")
	if len(r.Events) == 0 {
		return
	}

	events := Events{}

	t := time.Now()
	for _, e := range r.Events {
		if t.Before(e.CreatedAt) {
			events = append(events, e)
		}
	}

	if len(events) == 0 {
		r.state = aggIdle
	}

	r.Events = events
}

type DestinationDispatcher interface {
	Send(*EventSummary) error
}

func DispatcherFor(destination string) DestinationDispatcher {
	switch {
	case strings.HasPrefix(destination, "IRC"):
	case strings.HasPrefix(destination, "TRELLO"):
	case strings.HasPrefix(destination, "MAIL"):
	case strings.HasPrefix(destination, "PAGERDUTY"):
	}
}

type EventSummary struct {
	Rule *AggregationRule

	Events Events

	Destination string
}

type EventSummaries []EventSummary

func (r *AggregationInstance) Summarize(s chan<- EventSummary) {
	if r.state != aggIdle {
		return
	}
	if len(r.Events) == 0 {
		return
	}

	r.state = aggEmitting

	s <- EventSummary{
		Rule:   r.Rule,
		Events: r.Events,
	}
}

type AggregationRules []*AggregationRule

func (r AggregationRules) Len() int {
	return len(r)
}

func (r AggregationRules) Less(i, j int) bool {
	return r[i].fingerprint < r[j].fingerprint
}

func (r AggregationRules) Swap(i, j int) {
	r[i], r[j] = r[i], r[j]
}

type Aggregator struct {
	Rules      AggregationRules
	Aggregates map[uint64]*AggregationInstance
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		Aggregates: make(map[uint64]*AggregationInstance),
	}
}

type AggregateEventsResponse struct {
	Err error
}

type AggregateEventsRequest struct {
	Events Events

	Response chan *AggregateEventsResponse
}

func (a *Aggregator) aggregate(r *AggregateEventsRequest, s chan<- EventSummary) {
	log.Println("aggregating", *r)
	for _, element := range r.Events {
		fp := element.Fingerprint()
		for _, r := range a.Rules {
			log.Println("Checking rule", r, r.Handles(element))
			if r.Handles(element) {
				aggregation, ok := a.Aggregates[fp]
				if !ok {
					aggregation = &AggregationInstance{
						Rule: r,
					}

					a.Aggregates[fp] = aggregation
				}

				aggregation.Ingest(element)
				aggregation.Summarize(s)
				break
			}
		}
	}

	r.Response <- new(AggregateEventsResponse)
}

type AggregatorResetRulesResponse struct {
	Err error
}
type AggregatorResetRulesRequest struct {
	Rules AggregationRules

	Response chan *AggregatorResetRulesResponse
}

func (a *Aggregator) replaceRules(r *AggregatorResetRulesRequest) {
	newRules := AggregationRules{}
	for _, rule := range r.Rules {
		newRules = append(newRules, rule)
	}

	sort.Sort(newRules)

	a.Rules = newRules

	r.Response <- new(AggregatorResetRulesResponse)
}

func (a *Aggregator) Dispatch(reqs <-chan *AggregateEventsRequest, rules <-chan *AggregatorResetRulesRequest, s chan<- EventSummary) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	closed := 0

	for closed < 1 {
		select {
		case req, open := <-reqs:
			a.aggregate(req, s)

			if !open {
				closed++
			}

		case rules, open := <-rules:
			a.replaceRules(rules)

			if !open {
				closed++
			}

		case <-t.C:
			for _, a := range a.Aggregates {
				a.Tidy()
			}
		}
	}
}

type SummaryDispatcher struct{}

func (d *SummaryDispatcher) dispatchSummary(s EventSummary, i chan<- *IsInhibitedRequest) {
	log.Println("dispatching summary", s)
	r := &IsInhibitedRequest{
		Response: make(chan IsInhibitedResponse),
	}
	i <- r
	resp := <-r.Response
	log.Println(resp)
}

func (d *SummaryDispatcher) Dispatch(s <-chan EventSummary, i chan<- *IsInhibitedRequest) {
	for summary := range s {
		d.dispatchSummary(summary, i)
		//		fmt.Println("Summary for", summary.Rule, "with", summary.Events, "@", len(summary.Events))
	}
}

type Filter struct {
	Name  *regexp.Regexp
	Value *regexp.Regexp

	fingerprint uint64
}

func NewFilter(namePattern string, valuePattern string) *Filter {
	summer := fnv.New64a()
	fmt.Fprintf(summer, namePattern, valuePattern)

	return &Filter{
		Name:        regexp.MustCompile(namePattern),
		Value:       regexp.MustCompile(valuePattern),
		fingerprint: summer.Sum64(),
	}
}

func (f *Filter) Handles(e *Event) bool {
	for k, v := range e.Payload {
		if f.Name.MatchString(k) && f.Value.MatchString(v) {
			return true
		}
	}

	return false
}

type Filters []*Filter

func (f Filters) Len() int {
	return len(f)
}

func (f Filters) Less(i, j int) bool {
	return f[i].fingerprint < f[j].fingerprint
}

func (f Filters) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f *Filters) Push(v interface{}) {
	(*f) = append(*f, v.(*Filter))
}

func (f *Filters) Pop() interface{} {
	i := (*f)[len(*f)-1]
	(*f) = (*f)[0:len(*f)]

	return i
}

func (f Filters) Handle(e *Event) bool {
	fCount := len(f)
	fMatch := 0

	for _, filter := range f {
		if filter.Handles(e) {
			fMatch++
		}
	}

	return fCount == fMatch
}

func (f Filters) fingerprint() uint64 {
	summer := fnv.New64a()

	for i, f := range f {
		fmt.Fprintln(summer, i, f.fingerprint)
	}

	return summer.Sum64()
}

type Suppression struct {
	Id uint

	Description string

	Filters *Filters

	EndsAt time.Time

	CreatedBy string
	CreatedAt time.Time
}

type SuppressionRequest struct {
	Suppression Suppression

	Response chan SuppressionResponse
}

type SuppressionResponse struct {
	Err error
}

type IsInhibitedRequest struct {
	Event Event

	Response chan IsInhibitedResponse
}

type IsInhibitedResponse struct {
	Err error

	Inhibited             bool
	InhibitingSuppression *Suppression
}

type SuppressionSummaryResponse struct {
	Err error

	Suppressions Suppressions
}

type SuppressionSummaryRequest struct {
	MatchCandidates map[string]string

	Response chan<- SuppressionSummaryResponse
}

type Suppressor struct {
	Suppressions *Suppressions
}

func NewSuppressor() *Suppressor {
	suppressions := new(Suppressions)
	heap.Init(suppressions)

	return &Suppressor{
		Suppressions: suppressions,
	}
}

type Suppressions []Suppression

func (s Suppressions) Len() int {
	return len(s)
}

func (s Suppressions) Less(i, j int) bool {
	return s[i].EndsAt.Before(s[j].EndsAt)
}

func (s Suppressions) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *Suppressions) Push(v interface{}) {
	*s = append(*s, v.(Suppression))
}

func (s *Suppressions) Pop() interface{} {
	old := *s
	n := len(old)
	item := old[n-1]
	*s = old[0 : n-1]
	return item
}

func (s *Suppressor) dispatchSuppression(r SuppressionRequest) {
	log.Println("dispatching suppression", r)

	heap.Push(s.Suppressions, r.Suppression)
	r.Response <- SuppressionResponse{}
}

func (s *Suppressor) reapSuppressions(t time.Time) {
	log.Println("readping suppression...")

	i := sort.Search(len(*s.Suppressions), func(i int) bool {
		return (*s.Suppressions)[i].EndsAt.After(t)
	})

	*s.Suppressions = (*s.Suppressions)[i:]

	// BUG(matt): Validate if strictly necessary.
	heap.Init(s.Suppressions)
}

func (s *Suppressor) generateSummary(r SuppressionSummaryRequest) {
	log.Println("Generating summary", r)
	response := SuppressionSummaryResponse{}

	for _, suppression := range *s.Suppressions {
		response.Suppressions = append(response.Suppressions, suppression)
	}

	r.Response <- response
}

func (s *Suppressor) queryInhibit(q *IsInhibitedRequest) {
	response := IsInhibitedResponse{}

	for _, s := range *s.Suppressions {
		if s.Filters.Handle(&q.Event) {
			response.Inhibited = true
			response.InhibitingSuppression = &s

			break
		}
	}

	q.Response <- response
}

func (s *Suppressor) Dispatch(suppressions <-chan SuppressionRequest, inhibitQuery <-chan *IsInhibitedRequest, summaries <-chan SuppressionSummaryRequest) {
	reaper := time.NewTicker(30 * time.Second)
	defer reaper.Stop()

	closed := 0

	for closed < 2 {
		select {
		case suppression, open := <-suppressions:
			s.dispatchSuppression(suppression)

			if !open {
				closed++
			}

		case query, open := <-inhibitQuery:
			s.queryInhibit(query)

			if !open {
				closed++
			}

		case summary, open := <-summaries:
			s.generateSummary(summary)

			if !open {
				closed++
			}

		case time := <-reaper.C:
			s.reapSuppressions(time)
		}
	}
}

type Main struct {
	SuppressionRequests chan SuppressionRequest
	InhibitQueries      chan *IsInhibitedRequest
	Summaries           chan SuppressionSummaryRequest
	AggregateEvents     chan *AggregateEventsRequest
	EventSummary        chan EventSummary
	Rules               chan *AggregatorResetRulesRequest
}

func (m *Main) close() {
	close(m.SuppressionRequests)
	close(m.InhibitQueries)
	close(m.Summaries)
	close(m.AggregateEvents)
	close(m.EventSummary)
	close(m.Rules)
}

func main() {
	main := &Main{
		SuppressionRequests: make(chan SuppressionRequest),
		InhibitQueries:      make(chan *IsInhibitedRequest),
		Summaries:           make(chan SuppressionSummaryRequest),
		AggregateEvents:     make(chan *AggregateEventsRequest),
		EventSummary:        make(chan EventSummary),
		Rules:               make(chan *AggregatorResetRulesRequest),
	}
	defer main.close()

	log.Print("Starting event suppressor...")
	suppressor := &Suppressor{
		Suppressions: new(Suppressions),
	}
	go suppressor.Dispatch(main.SuppressionRequests, main.InhibitQueries, main.Summaries)
	log.Println("Done.")

	log.Println("Starting event aggregator...")
	aggregator := NewAggregator()
	go aggregator.Dispatch(main.AggregateEvents, main.Rules, main.EventSummary)
	log.Println("Done.")

	done := make(chan bool)
	go func() {
		ar := make(chan *AggregatorResetRulesResponse)
		agg := &AggregatorResetRulesRequest{
			Rules: AggregationRules{
				NewAggregationRule(NewFilter("service", "discovery")),
			},
			Response: ar,
		}

		main.Rules <- agg
		log.Println("aggResult", <-ar)

		r := make(chan *AggregateEventsResponse)
		aer := &AggregateEventsRequest{
			Events: Events{
				&Event{
					Payload: map[string]string{
						"service": "discovery",
					},
				},
			},
			Response: r,
		}

		main.AggregateEvents <- aer

		log.Println("Response", r)

		done <- true
	}()
	<-done

	log.Println("Running summary dispatcher...")
	summarizer := new(SummaryDispatcher)
	summarizer.Dispatch(main.EventSummary, main.InhibitQueries)
}
