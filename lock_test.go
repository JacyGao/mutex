package mutex

import (
	"testing"
	"golang.org/x/net/context"

	"stash.ea.com/scm/exos/server/lib/test/testutils"
)

func TestMutexOperations(t *testing.T) {
	localDB := testutils.NewLocalDynamoDB()
	ms := NewService(localDB)
	ctx := context.Background()

	if err := ms.Lock(ctx, "alliance_1", "persona_1"); err != nil {
		testutils.Fatalf(t, err.Error())
	}

	err := ms.Lock(ctx, "alliance_1", "persona_2")
	testutils.CheckWithErrorf(t, err, "expected error")

	if err := ms.Unlock(ctx, "alliance_1", "persona_1"); err != nil {
		testutils.Fatalf(t, err.Error())
	}
}

