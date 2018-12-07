package mutex

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/net/context"

	"stash.ea.com/scm/exos/server/lib/data/dynamodb"
)

var (
	TableNameMutex = "mutex"

	MutexPartitionKeyName = "aggregate_id"

	MutexSortKeyName = ""
)

var (
	errInvalidExpiry = errors.New("invalid expiry")
	errInvalidOwnerID = errors.New("invalid owner id")
)

type Locker interface {
	Get(ctx context.Context, key string, valuePtr interface{}) error
	Set(ctx context.Context, key string, value interface{}) error
	Del(ctx context.Context, key string) error
}

// TODO(jacy): update expiry for long running processes
const (
	defaultMutexExpiryInSeconds = 1
	LockMaxAttempts = 10
)

type LockDoc struct {
	AggregateID string    `json:"aggregate_id"`
	OwnerID     string    `json:"owner_id"`
	Expiry      time.Time `json:"timestamp"`
}

func (d LockDoc) HashKey() string {
	return d.AggregateID
}

func (d LockDoc) RangeKey() string {
	return ""
}

func newLockDoc(aggregateID, OwnerID string) LockDoc {
	return LockDoc{
		AggregateID: aggregateID,
		OwnerID:     OwnerID,
		Expiry:      time.Now().UTC().Add(time.Second * time.Duration(defaultMutexExpiryInSeconds)),
	}
}

type Service struct {
	store dynamodb.DynamoDB
	locker Locker
}

func NewService(db dynamodb.DynamoDB) *Service {
	return &Service{
		store: db,
	}
}

// Lock attempts to create a lock to a group that only allows access from a single user.
// TODO(jacy): Dealing TTL. Perhaps having dynamodb managing it in the background
// TODO(jacy): We could make the expiry a sort key and if time.Now > expiry then we consider the lock is no long valid
func (s *Service) Lock(ctx context.Context, aggregateID, ownerID string) error {
	doc := newLockDoc(aggregateID, ownerID)
	// Ensure expiry is greater than current time
	if !doc.Expiry.UTC().After(time.Now().UTC()) {
		return errInvalidExpiry
	}

	in := dynamodb.UpsertInput{
		TableName: TableNameMutex,
		Value:     doc,
		// Ensure lock uniqueness
		ConditionExpression: fmt.Sprintf("attribute_not_exists(%s)", MutexPartitionKeyName),
	}

	if err := s.store.Upsert(ctx, in); err != nil {
		return err
	}

	return nil
}

// Unlock unlocks a group.
func (s *Service) Unlock(ctx context.Context, aggregateID, ownerID string) error {
	doc, err := s.Get(ctx, aggregateID)
	if err != nil {
		return err
	}

	if doc.OwnerID != ownerID {
		return errInvalidOwnerID
	}

	in := dynamodb.DeleteInput{
		TableName:  TableNameMutex,
		PrimaryKey: dynamodb.PrimaryKey{MutexPartitionKeyName: aggregateID},
	}
	// TODO(jacy): what happens if the lock does not exist?
	if err := s.store.Delete(ctx, in); err != nil {
		return err
	}
	return nil
}

func (s *Service) Get(ctx context.Context, aggregateID string, docPtr interface{}) error {
	in := dynamodb.GetInput{
		TableName: TableNameMutex,
		PrimaryKey: dynamodb.PrimaryKey{MutexPartitionKeyName: aggregateID},
	}

	doc := LockDoc{}
	if err := s.store.Get(ctx, in, &doc); err != nil {
		return doc, err
	}
	return doc, nil
}