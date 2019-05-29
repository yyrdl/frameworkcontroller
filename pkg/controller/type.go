package controller

import (
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/rest"
	kubeClient "k8s.io/client-go/kubernetes"
	coreLister "k8s.io/client-go/listers/core/v1"
	frameworkClient "github.com/microsoft/frameworkcontroller/pkg/client/clientset/versioned"
	frameworkLister "github.com/microsoft/frameworkcontroller/pkg/client/listers/frameworkcontroller/v1"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1" 
)


// FrameworkController maintains the lifecycle for all Frameworks in the cluster.
// It is the engine to transition the Framework.Status and other Framework related
// objects to satisfy the Framework.Spec eventually.
type FrameworkController struct {
	kConfig *rest.Config
	cConfig *ci.Config

	// Client is used to write remote objects in ApiServer.
	// Remote objects are up-to-date and is writable.
	//
	// To read objects, it is better to use Lister instead of Client, since the
	// Lister is cached and the cache is the ground truth of other managed objects.
	//
	// Write remote objects cannot immediately change the local cached ground truth,
	// so, it is just a hint to drive the ground truth changes, and a complete write
	// should wait until the local cached objects reflect the write.
	//
	// Client already has retry policy to retry for most transient failures.
	// Client write failure does not mean the write does not succeed on remote, the
	// failure may be due to the success response is just failed to deliver to the
	// Client.
	kClient kubeClient.Interface
	fClient frameworkClient.Interface

	// Informer is used to sync remote objects to local cached objects, and deliver
	// events of object changes.
	//
	// The event delivery is level driven, not edge driven.
	// For example, the Informer may not deliver any event if a create is immediately
	// followed by a delete.
	cmInformer  cache.SharedIndexInformer
	podInformer cache.SharedIndexInformer
	fInformer   cache.SharedIndexInformer

	// Lister is used to read local cached objects in Informer.
	// Local cached objects may be outdated and is not writable.
	//
	// Outdated means current local cached objects may not reflect previous Client
	// remote writes.
	// For example, in previous round of syncFramework, Client created a Pod on
	// remote, however, in current round of syncFramework, the Pod may not appear
	// in the local cache, i.e. the local cached Pod is outdated.
	//
	// The local cached Framework.Status may be also outdated, so we take the
	// expected Framework.Status instead of the local cached one as the ground
	// truth of Framework.Status.
	//
	// The events of object changes are aligned with local cache, so we take the
	// local cached object instead of the remote one as the ground truth of
	// other managed objects except for the Framework.Status.
	// The outdated other managed object can be avoided by sync it only after the
	// remote write is also reflected in the local cache.
	cmLister  coreLister.ConfigMapLister
	podLister coreLister.PodLister
	fLister   frameworkLister.FrameworkLister

	// Queue is used to decouple items delivery and processing, i.e. control
	// how items are scheduled and distributed to process.
	// The items may come from Informer's events, or Controller's events, etc.
	//
	// It is not strictly FIFO because its Add method will only enqueue an item
	// if it is not already in the queue, i.e. the queue is deduplicated.
	// In fact, it is a FIFO pending set combined with a processing set instead of
	// a standard queue, i.e. a strict FIFO data structure.
	// So, even if we only allow to start a single worker, we cannot ensure all items
	// in the queue will be processed in FIFO order.
	// Finally, in any case, processing later enqueued item should not depend on the
	// result of processing previous enqueued item.
	//
	// However, it can be used to provide a processing lock for every different items
	// in the queue, i.e. the same item will not be processed concurrently, even in
	// the face of multiple concurrent workers.
	// Note, different items may still be processed concurrently in the face of
	// multiple concurrent workers. So, processing different items should modify
	// different objects to avoid additional concurrency control.
	//
	// Given above queue behaviors, we can choose to enqueue what kind of items:
	// 1. Framework Key
	//    Support multiple concurrent workers, but processing is coarse grained.
	//    Good at managing many small scale Frameworks.
	//    More idiomatic and easy to implement.
	// 2. All Managed Object Keys, such as Framework Key, Pod Key, etc
	//    Only support single worker, but processing is fine grained.
	//    Good at managing few large scale Frameworks.
	// 3. Events, such as [Pod p is added to Framework f]
	//    Only support single worker, and processing is fine grained.
	//    Good at managing few large scale Frameworks.
	// 4. Objects, such as Framework Object
	//    Only support single worker.
	//    Compared with local cached objects, the dequeued objects may be outdated.
	//    Internally, item will be used as map key, so objects means low performance.
	// Finally, we choose choice 1, so it is a Framework Key Queue.
	//
	// Processing is coarse grained:
	// Framework Key as item cannot differentiate Framework events, even for Add,
	// Update and Delete Framework event.
	// Besides, the dequeued item may be outdated compared the local cached one.
	// So, we can coarsen Add, Update and Delete event as a single Update event,
	// enqueue the Framework Key, and until the Framework Key is dequeued and started
	// to process, we refine the Update event to Add, Update or Delete event.
	//
	// Framework Key in queue should be valid, i.e. it can be SplitKey successfully.
	//
	// Enqueue a Framework Key means schedule a syncFramework for the Framework,
	// no matter the Framework's objects changed or not.
	//
	// Methods:
	// Add:
	//   Only keep the earliest item to dequeue:
	//   The item will only be enqueued if it is not already in the queue.
	// AddAfter:
	//   Only keep the earliest item to Add:
	//   The item may be Added before the duration elapsed, such as the same item
	//   is AddedAfter later with an earlier duration.
	fQueue workqueue.RateLimitingInterface

	// fExpectedStatusInfos is used to store the expected Framework.Status info for
	// all Frameworks.
	// See ExpectedFrameworkStatusInfo.
	//
	// Framework Key -> The expected Framework.Status info
	fExpectedStatusInfos map[string]*ExpectedFrameworkStatusInfo
}


type ExpectedFrameworkStatusInfo struct {
	// The expected Framework.Status.
	// It is the ground truth Framework.Status that the remote and the local cached
	// Framework.Status are expected to be.
	//
	// It is used to sync against the local cached Framework.Spec and the local
	// cached other related objects, and it helps to ensure the Framework.Status is
	// Monotonically Exposed.
	// Note, the local cached Framework.Status may be outdated compared with the
	// remote one, so without the it, the local cached Framework.Status is not
	// enough to ensure the Framework.Status is Monotonically Exposed.
	// See FrameworkStatus.
	status *ci.FrameworkStatus

	// Whether the expected Framework.Status is the same as the remote one.
	// It helps to ensure the expected Framework.Status is persisted before sync.
	remoteSynced bool
}