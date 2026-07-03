package storectrl

import (
	"context"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type storeClient struct {
	store  Store
	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

func NewClient(store Store, scheme *runtime.Scheme) client.Client {
	return &storeClient{
		store:  store,
		scheme: scheme,
		mapper: meta.NewDefaultRESTMapper([]schema.GroupVersion{}),
	}
}

func (c *storeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.store.Get(ctx, key, obj)
}

func (c *storeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.store.List(ctx, list, opts...)
}

func (c *storeClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.store.Create(ctx, obj)
}

func (c *storeClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.store.Delete(ctx, obj)
}

func (c *storeClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.store.Update(ctx, obj)
}

func (c *storeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	patchBytes, err := patch.Data(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)
	current, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("failed to deep copy object")
	}
	if err := c.store.Get(ctx, key, current); err != nil {
		return err
	}

	currentBytes, err := json.Marshal(current)
	if err != nil {
		return err
	}

	var patchedBytes []byte
	switch patch.Type() {
	case types.JSONPatchType:
		jp, err := jsonpatch.DecodePatch(patchBytes)
		if err != nil {
			return err
		}
		patchedBytes, err = jp.Apply(currentBytes)
		if err != nil {
			return err
		}
	case types.MergePatchType, types.StrategicMergePatchType:
		patchedBytes, err = jsonpatch.MergePatch(currentBytes, patchBytes)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported patch type: %s", patch.Type())
	}

	if err := json.Unmarshal(patchedBytes, obj); err != nil {
		return err
	}

	return c.store.Update(ctx, obj)
}

func (c *storeClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return err
	}

	listGVK := gvk
	listGVK.Kind = gvk.Kind + "List"

	list, err := c.scheme.New(listGVK)
	if err != nil {
		return err
	}

	listObj, ok := list.(client.ObjectList)
	if !ok {
		return fmt.Errorf("expected ObjectList, got %T", list)
	}

	deleteOpts := &client.DeleteAllOfOptions{}
	deleteOpts.ApplyOptions(opts)

	var listOpts []client.ListOption
	if deleteOpts.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(deleteOpts.Namespace))
	}
	if deleteOpts.LabelSelector != nil {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: deleteOpts.LabelSelector})
	}
	if deleteOpts.FieldSelector != nil {
		listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: deleteOpts.FieldSelector})
	}

	if err := c.store.List(ctx, listObj, listOpts...); err != nil {
		return err
	}

	items, err := meta.ExtractList(listObj)
	if err != nil {
		return err
	}

	for _, item := range items {
		itemObj, ok := item.(client.Object)
		if !ok {
			return fmt.Errorf("expected Object, got %T", item)
		}
		if err := c.store.Delete(ctx, itemObj); err != nil {
			return err
		}
	}

	return nil
}

func (c *storeClient) Status() client.SubResourceWriter {
	return &storeStatusWriter{client: c}
}

func (c *storeClient) SubResource(subResource string) client.SubResourceClient {
	if subResource == "status" {
		return &storeSubResourceClient{client: c}
	}
	return &storeSubResourceClient{client: c, unsupported: true}
}

func (c *storeClient) Scheme() *runtime.Scheme {
	return c.scheme
}

func (c *storeClient) RESTMapper() meta.RESTMapper {
	return c.mapper
}

func (c *storeClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return apiutil.GVKForObject(obj, c.scheme)
}

func (c *storeClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return true, nil
}

func (c *storeClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return c.store.Apply(ctx, obj, opts...)
}

type storeStatusWriter struct {
	client *storeClient
}

func (w *storeStatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return fmt.Errorf("subresource creation not supported")
}

func (w *storeStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return w.client.store.Update(ctx, obj)
}

func (w *storeStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return w.client.Patch(ctx, obj, patch)
}

func (w *storeStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return w.client.store.Apply(ctx, obj)
}

type storeSubResourceClient struct {
	client      *storeClient
	unsupported bool
}

func (s *storeSubResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	if s.unsupported {
		return fmt.Errorf("subresource not supported")
	}
	return fmt.Errorf("subresource Get not supported")
}

func (s *storeSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return fmt.Errorf("subresource creation not supported")
}

func (s *storeSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.unsupported {
		return fmt.Errorf("subresource not supported")
	}
	return s.client.store.Update(ctx, obj)
}

func (s *storeSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.unsupported {
		return fmt.Errorf("subresource not supported")
	}
	return s.client.Patch(ctx, obj, patch)
}

func (s *storeSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return s.client.store.Apply(ctx, obj)
}
