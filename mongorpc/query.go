package mongorpc

import "context"

// QueryBuilder provides a fluent interface for building queries.
type QueryBuilder struct {
	collection *Collection
	filter     Filter
	projection map[string]int
	sort       map[string]int
	limit      int64
	skip       int64
}

func newQueryBuilder(c *Collection) *QueryBuilder {
	return &QueryBuilder{
		collection: c,
		filter:     make(Filter),
		projection: make(map[string]int),
		sort:       make(map[string]int),
	}
}

// Where adds an equality filter.
func (q *QueryBuilder) Where(field string, value any) *QueryBuilder {
	q.filter[field] = value
	return q
}

// Eq adds an equality filter.
func (q *QueryBuilder) Eq(field string, value any) *QueryBuilder {
	q.filter[field] = value
	return q
}

// Ne adds a not-equal filter.
func (q *QueryBuilder) Ne(field string, value any) *QueryBuilder {
	q.filter[field] = Filter{"$ne": value}
	return q
}

// Gt adds a greater-than filter.
func (q *QueryBuilder) Gt(field string, value any) *QueryBuilder {
	q.filter[field] = Filter{"$gt": value}
	return q
}

// Gte adds a greater-than-or-equal filter.
func (q *QueryBuilder) Gte(field string, value any) *QueryBuilder {
	q.filter[field] = Filter{"$gte": value}
	return q
}

// Lt adds a less-than filter.
func (q *QueryBuilder) Lt(field string, value any) *QueryBuilder {
	q.filter[field] = Filter{"$lt": value}
	return q
}

// Lte adds a less-than-or-equal filter.
func (q *QueryBuilder) Lte(field string, value any) *QueryBuilder {
	q.filter[field] = Filter{"$lte": value}
	return q
}

// In adds an in-array filter.
func (q *QueryBuilder) In(field string, values ...any) *QueryBuilder {
	q.filter[field] = Filter{"$in": values}
	return q
}

// NotIn adds a not-in-array filter.
func (q *QueryBuilder) NotIn(field string, values ...any) *QueryBuilder {
	q.filter[field] = Filter{"$nin": values}
	return q
}

// Regex adds a regex filter.
func (q *QueryBuilder) Regex(field, pattern string) *QueryBuilder {
	q.filter[field] = Filter{"$regex": pattern}
	return q
}

// Exists adds an exists filter.
func (q *QueryBuilder) Exists(field string, exists bool) *QueryBuilder {
	q.filter[field] = Filter{"$exists": exists}
	return q
}

// Select specifies fields to include.
func (q *QueryBuilder) Select(fields ...string) *QueryBuilder {
	for _, f := range fields {
		q.projection[f] = 1
	}
	return q
}

// Exclude specifies fields to exclude.
func (q *QueryBuilder) Exclude(fields ...string) *QueryBuilder {
	for _, f := range fields {
		q.projection[f] = 0
	}
	return q
}

// SortAsc adds ascending sort.
func (q *QueryBuilder) SortAsc(field string) *QueryBuilder {
	q.sort[field] = 1
	return q
}

// SortDesc adds descending sort.
func (q *QueryBuilder) SortDesc(field string) *QueryBuilder {
	q.sort[field] = -1
	return q
}

// Limit sets the result limit.
func (q *QueryBuilder) Limit(n int64) *QueryBuilder {
	q.limit = n
	return q
}

// Skip sets the number of results to skip.
func (q *QueryBuilder) Skip(n int64) *QueryBuilder {
	q.skip = n
	return q
}

// All executes the query and returns all results.
func (q *QueryBuilder) All(ctx context.Context) ([]Document, error) {
	return q.collection.Find(ctx, q.filter, FindOptions{
		Projection: q.projection,
		Sort:       q.sort,
		Limit:      q.limit,
		Skip:       q.skip,
	})
}

// First executes the query and returns the first result.
func (q *QueryBuilder) First(ctx context.Context) (Document, error) {
	docs, err := q.Limit(1).All(ctx)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, ErrNotFound
	}
	return docs[0], nil
}

// Count executes the query and returns the count.
func (q *QueryBuilder) Count(ctx context.Context) (int64, error) {
	return q.collection.CountDocuments(ctx, q.filter)
}
