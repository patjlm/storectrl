package storectrl

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BaseObject can be embedded in domain types to satisfy client.Object.
// It provides TypeMeta (API version, kind) and ObjectMeta (name,
// namespace, UID, resource version, labels, annotations, etc.).
//
// Types embedding BaseObject must still implement DeepCopyObject()
// from runtime.Object. Use DeepCopyBaseObject as a helper.
//
// Example:
//
//	type MyResource struct {
//	    storectrl.BaseObject `json:",inline"`
//	    Spec   MyResourceSpec   `json:"spec"`
//	    Status MyResourceStatus `json:"status"`
//	}
//
//	func (m *MyResource) DeepCopyObject() runtime.Object {
//	    cp := *m
//	    m.BaseObject.DeepCopyInto(&cp.BaseObject)
//	    return &cp
//	}
type BaseObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// DeepCopyInto copies all fields into another BaseObject.
func (b *BaseObject) DeepCopyInto(out *BaseObject) {
	out.TypeMeta = b.TypeMeta
	b.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
}

// BaseList can be embedded in list types to satisfy client.ObjectList.
//
// Types embedding BaseList must implement DeepCopyObject() and provide
// an Items field of their element type.
//
// Example:
//
//	type MyResourceList struct {
//	    storectrl.BaseList `json:",inline"`
//	    Items []MyResource `json:"items"`
//	}
type BaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
}

// DeepCopyInto copies all fields into another BaseList.
func (b *BaseList) DeepCopyInto(out *BaseList) {
	out.TypeMeta = b.TypeMeta
	out.ListMeta = *b.ListMeta.DeepCopy()
}
