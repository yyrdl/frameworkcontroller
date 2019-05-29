// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package controller

import (
	"fmt"
	"time"
	"reflect"
	"strings"
	log "github.com/sirupsen/logrus"
	errorWrap "github.com/pkg/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/types"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/rest"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	errorAgg "k8s.io/apimachinery/pkg/util/errors"
	kubeClient "k8s.io/client-go/kubernetes"
	kubeInformer "k8s.io/client-go/informers"
	coreLister "k8s.io/client-go/listers/core/v1"
	frameworkClient "github.com/microsoft/frameworkcontroller/pkg/client/clientset/versioned"
	frameworkInformer "github.com/microsoft/frameworkcontroller/pkg/client/informers/externalversions"
	frameworkLister "github.com/microsoft/frameworkcontroller/pkg/client/listers/frameworkcontroller/v1"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/util"
	"github.com/microsoft/frameworkcontroller/pkg/common"
)


func NewFrameworkController() *FrameworkController {
	log.Infof("Initializing " + ci.ComponentName)

	cConfig := ci.NewConfig()
	common.LogLines("With Config: \n%v", common.ToYaml(cConfig))
	kConfig := ci.BuildKubeConfig(cConfig)

	kClient, fClient := util.CreateClients(kConfig)

	// Informer resync will periodically replay the event of all objects stored in its cache.
	// However, by design, Informer and Controller should not miss any event.
	// So, we should disable resync to avoid hiding missing event bugs inside Controller.
	//
	// TODO: Add AttemptCreating state after SharedInformer supports IncludeUninitialized.
	// So that we can move the object initialization time out of the
	// ObjectLocalCacheCreationTimeoutSec, to reduce the expectation timeout false alarm
	// rate when Pod is specified with Initializers.
	// See https://github.com/kubernetes/kubernetes/pull/51247
	cmListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().ConfigMaps()
	podListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().Pods()
	fListerInformer := frameworkInformer.NewSharedInformerFactory(fClient, 0).Frameworkcontroller().V1().Frameworks()
	cmInformer := cmListerInformer.Informer()
	podInformer := podListerInformer.Informer()
	fInformer := fListerInformer.Informer()
	cmLister := cmListerInformer.Lister()
	podLister := podListerInformer.Lister()
	fLister := fListerInformer.Lister()

	// Using DefaultControllerRateLimiter to rate limit on both particular items and overall items.
	fQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	c := &FrameworkController{
		kConfig:              kConfig,
		cConfig:              cConfig,
		kClient:              kClient,
		fClient:              fClient,
		cmInformer:           cmInformer,
		podInformer:          podInformer,
		fInformer:            fInformer,
		cmLister:             cmLister,
		podLister:            podLister,
		fLister:              fLister,
		fQueue:               fQueue,
		fExpectedStatusInfos: map[string]*ExpectedFrameworkStatusInfo{},
	}

	fInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkObj(obj, "Framework Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// FrameworkController only cares about Framework.Spec update
			oldF := oldObj.(*ci.Framework)
			newF := newObj.(*ci.Framework)
			if !reflect.DeepEqual(oldF.Spec, newF.Spec) {
				c.enqueueFrameworkObj(newObj, "Framework.Spec Updated")
			}
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkObj(obj, "Framework Deleted")
		},
	})

	cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkConfigMapObj(obj, "Framework ConfigMap Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueFrameworkConfigMapObj(newObj, "Framework ConfigMap Updated")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkConfigMapObj(obj, "Framework ConfigMap Deleted")
		},
	})

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueFrameworkPodObj(obj, "Framework Pod Added")
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueFrameworkPodObj(newObj, "Framework Pod Updated")
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueueFrameworkPodObj(obj, "Framework Pod Deleted")
		},
	})

	return c
}


func (c *FrameworkController) Run(stopCh <-chan struct{}) {
	defer c.fQueue.ShutDown()
	defer log.Errorf("Stopping " + ci.ComponentName)
	defer runtime.HandleCrash()

	log.Infof("Recovering " + ci.ComponentName)
	util.PutCRD(
		c.kConfig,
		ci.BuildFrameworkCRD(),
		c.cConfig.CRDEstablishedCheckIntervalSec,
		c.cConfig.CRDEstablishedCheckTimeoutSec)

	go c.fInformer.Run(stopCh)
	go c.cmInformer.Run(stopCh)
	go c.podInformer.Run(stopCh)
	if !cache.WaitForCacheSync(
		stopCh,
		c.fInformer.HasSynced,
		c.cmInformer.HasSynced,
		c.podInformer.HasSynced) {
		panic("Failed to WaitForCacheSync")
	}

	log.Infof("Running %v with %v workers",
		ci.ComponentName, *c.cConfig.WorkerNumber)

	for i := int32(0); i < *c.cConfig.WorkerNumber; i++ {
		// id is dedicated for each iteration, while i is not.
		id := i
		go wait.Until(func() { c.worker(id) }, time.Second, stopCh)
	}

	<-stopCh
}

func (c *FrameworkController) worker(id int32) {
	defer log.Errorf("Stopping worker-%v", id)
	log.Infof("Running worker-%v", id)

	for c.processNextWorkItem(id) {
	}
}

func (c *FrameworkController) processNextWorkItem(id int32) bool {
	// Blocked to get an item which is different from the current processing items.
	key, quit := c.fQueue.Get()
	if quit {
		return false
	}
	log.Infof("[%v]: Assigned to worker-%v", key, id)

	// Remove the item from the current processing items to unblock getting the
	// same item again.
	defer c.fQueue.Done(key)

	err := c.syncFramework(key.(string))
	if err == nil {
		// Reset the rate limit counters of the item in the queue, such as NumRequeues,
		// because we have synced it successfully.
		c.fQueue.Forget(key)
	} else {
		c.fQueue.AddRateLimited(key)
	}

	return true
}


// No need to recover the non-AddAfter items, because the Informer has already
// delivered the Add events for all recovered Frameworks which caused all
// Frameworks will be enqueued to sync.
func (c *FrameworkController) recoverFrameworkWorkItems(f *ci.Framework) {
	
	logPfx := fmt.Sprintf("[%v]: recoverFrameworkWorkItems: ", f.Key())
	
	log.Infof(logPfx + "Started")

	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status == nil {
		return
	}

	c.recoverTimeoutChecks(f)
}

func (c *FrameworkController) recoverTimeoutChecks(f *ci.Framework) {
	// If a check is already timeout, the timeout will be handled by the following
	// sync after the recover, so no need to enqueue it again.
	c.enqueueFrameworkAttemptCreationTimeoutCheck(f, true)
	c.enqueueFrameworkRetryDelayTimeoutCheck(f, true)
	for _, taskRoleStatus := range f.TaskRoleStatuses() {
		for _, taskStatus := range taskRoleStatus.TaskStatuses {
			taskRoleName := taskRoleStatus.Name
			taskIndex := taskStatus.Index
			c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, true)
			c.enqueueTaskRetryDelayTimeoutCheck(f, taskRoleName, taskIndex, true)
		}
	}
}



func (c *FrameworkController) completeTaskAttempt(
		f *ci.Framework, taskRoleName string, taskIndex int32,
		completionStatus *ci.CompletionStatus) {
	logPfx := fmt.Sprintf("[%v][%v][%v]: completeTaskAttempt: ",
		f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	taskStatus.AttemptStatus.CompletionStatus = completionStatus
	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeletionPending,nil)

	// To ensure the CompletionStatus is persisted before deleting the pod,
	// we need to wait until next sync to delete the pod, so manually enqueue
	// a sync.
	c.enqueueFramework(f, "TaskAttemptDeletionPending")
	log.Infof(logPfx + "Waiting TaskAttempt CompletionStatus to be persisted")
}

func (c *FrameworkController) completeFrameworkAttempt(
		f *ci.Framework, completionStatus *ci.CompletionStatus) {
	logPfx := fmt.Sprintf("[%v]: completeFrameworkAttempt: ", f.Key())

	f.Status.AttemptStatus.CompletionStatus = completionStatus
	f.TransitionFrameworkState(ci.FrameworkAttemptDeletionPending)

	// To ensure the CompletionStatus is persisted before deleting the cm,
	// we need to wait until next sync to delete the cm, so manually enqueue
	// a sync.
	c.enqueueFramework(f, "FrameworkAttemptDeletionPending")
	log.Infof(logPfx + "Waiting FrameworkAttempt CompletionStatus to be persisted")
}

func (c *FrameworkController) updateRemoteFrameworkStatus(f *ci.Framework) error {
	logPfx := fmt.Sprintf("[%v]: updateRemoteFrameworkStatus: ", f.Key())
	log.Infof(logPfx + "Started")
	defer func() { log.Infof(logPfx + "Completed") }()

	updateF := f
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		updateF.Status = f.Status
		_, updateErr := c.fClient.FrameworkcontrollerV1().Frameworks(updateF.Namespace).Update(updateF)
		if updateErr == nil {
			return nil
		}

		// Try to resolve conflict by patching more recent object.
		localF, getErr := c.fLister.Frameworks(updateF.Namespace).Get(updateF.Name)
		if getErr != nil {
			if apiErrors.IsNotFound(getErr) {
				return fmt.Errorf("Framework cannot be found in local cache: %v", getErr)
			} else {
				log.Warnf(logPfx+"Framework cannot be got from local cache: %v", getErr)
			}
		} else {
			// Only resolve conflict for the same object to avoid updating another
			// object of the same name.
			if f.UID != localF.UID {
				return fmt.Errorf(
					"Framework UID mismatch: Current UID %v, Local Cached UID %v",
					f.UID, localF.UID)
			} else {
				updateF = localF.DeepCopy()
			}
		}

		return updateErr
	})

	if updateErr != nil {
		// Will still be requeued and retried after rate limited delay.
		return fmt.Errorf(logPfx+"Failed: %v", updateErr)
	} else {
		return nil
	}
}

func (c *FrameworkController) getExpectedFrameworkStatusInfo(key string) (
		*ExpectedFrameworkStatusInfo, bool) {
	value, exists := c.fExpectedStatusInfos[key]
	return value, exists
}

func (c *FrameworkController) deleteExpectedFrameworkStatusInfo(key string) {
	log.Infof("[%v]: deleteExpectedFrameworkStatusInfo: ", key)
	delete(c.fExpectedStatusInfos, key)
}

func (c *FrameworkController) updateExpectedFrameworkStatusInfo(key string,
		status *ci.FrameworkStatus, remoteSynced bool) {
	log.Infof("[%v]: updateExpectedFrameworkStatusInfo", key)
	c.fExpectedStatusInfos[key] = &ExpectedFrameworkStatusInfo{
		status:       status,
		remoteSynced: remoteSynced,
	}
}
