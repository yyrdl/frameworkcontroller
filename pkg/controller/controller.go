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
	"reflect"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	kubeInformer "k8s.io/client-go/informers"
	frameworkInformer "github.com/microsoft/frameworkcontroller/pkg/client/informers/externalversions"
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



func (c *FrameworkController) completeTaskAttempt(f *ci.Framework, taskRoleName string, taskIndex int32,completionStatus *ci.CompletionStatus) {

	logPfx := fmt.Sprintf("[%v][%v][%v]: completeTaskAttempt: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	taskStatus.AttemptStatus.CompletionStatus = completionStatus
	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeletionPending)

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
