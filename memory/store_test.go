package memory_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/memory"
	"github.com/patjlm/storectrl/storetest"
)

func TestStore(t *testing.T) {
	storetest.TestStore(t, storetest.Config{
		NewStore: func(scheme *runtime.Scheme) storectrl.Store {
			return memory.NewStore(scheme)
		},
		NewSmallEventLogStore: func(scheme *runtime.Scheme) storectrl.Store {
			return memory.NewStore(scheme, memory.WithEventLogSize(5))
		},
	})
}
