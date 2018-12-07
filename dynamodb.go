package mutex

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DynamoLockerImpl struct {
	DynamoDB  *dynamodb.DynamoDB
	TableName string
}

func NewDynamoLockerImpl(cli *dynamodb.DynamoDB, tableName string) *DynamoLockerImpl {
	return &DynamoLockerImpl{}
}

func (dl *DynamoLockerImpl) Get(ctx context.Context, key string, valuePtr interface{}) error {
	pk := map[string]string{
		"id": key,
	}
	keyInput, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return err
	}

	in := &dynamodb.GetItemInput{
		TableName: aws.String(dl.TableName),
		Key:       keyInput,
	}

	out, err := dl.DynamoDB.GetItem(in)
	if err != nil {
		return err
	}

	valuePtr = out.Item
	return nil
}

func (dl *DynamoLockerImpl) Set(ctx context.Context, key string, value interface{}) error {

}

func (dl *DynamoLockerImpl) Del(ctx context.Context, key string) error {

}
