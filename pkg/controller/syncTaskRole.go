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


func (c *FrameworkController) syncTaskRoleStatuses(f *ci.Framework, cm *core.ConfigMap) (syncFrameworkCancelled bool, err error) {

	logPfx := fmt.Sprintf("[%v]: syncTaskRoleStatuses: ", f.Key())

	log.Infof(logPfx + "Started")

	defer func() { log.Infof(logPfx + "Completed") }()

	errs := []error{}

	for _, taskRoleStatus := range f.TaskRoleStatuses() {

		log.Infof("[%v][%v]: syncTaskRoleStatus", f.Key(), taskRoleStatus.Name)
		
		for _, taskStatus := range taskRoleStatus.TaskStatuses {
		
			cancelled, err := c.syncTaskState(f, cm, taskRoleStatus.Name, taskStatus.Index)
		
			if err != nil {
				errs = append(errs, err)
			}

			if cancelled {
				log.Infof("[%v][%v][%v]: syncFramework is cancelled",f.Key(), taskRoleStatus.Name, taskStatus.Index)
				return true, errorAgg.NewAggregate(errs)
			}

			if err != nil {
				// The Tasks in the TaskRole have the same Spec except for the PodName,
				// so in most cases, same Platform Transient Error will return.
				log.Warnf("[%v][%v][%v]: Failed to sync Task, skip to sync the Tasks behind it in the TaskRole: %v",
					f.Key(), taskRoleStatus.Name, taskStatus.Index, err)

				break
			}
		}
	}

	return false, errorAgg.NewAggregate(errs)
}


// task role state machine

func (c *FrameworkController) syncTaskState(f *ci.Framework, cm *core.ConfigMap,
	taskRoleName string, taskIndex int32) (syncFrameworkCancelled bool, err error) {
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)
	
	log.Infof(logPfx + "Started")
	
	defer func() { log.Infof(logPfx + "Completed") }()

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	if taskStatus.State == ci.TaskCompleted {

		log.Infof(logPfx + "Skipped: Task is already completed")
		return false, nil
	}

	// Get the ground truth readonly pod
	pod, err := c.getOrCleanupPod(f, cm, taskRoleName, taskIndex)

	if err != nil {
		return false, err
	}

	var syncExit,cancelled bool


	if nil != pod && nil != pod.DeletionTimestamp{
		_,_,err = ci._syncWhenTaskIsBeingDeleted(f,taskRoleName,taskIndex)
		return false,err
	}

	if nil != pod && taskStatus.State == ci.TaskAttemptDeletionPending{
		syncExit,cancelled,err = ci._syncWhenTaskAttemptDeletionPending(f,taskRoleName,taskIndex)
	}

	if true == syncExit || nil != err {
		return cancelled,err
	}

	if nil != pod && taskStatus.State == ci.TaskAttemptDeletionRequested{
		_,_,err = ci._synWhenTaskAttemptDeletionRequested(f,taskRoleName,taskIndex)
		return false,err
	}
 
	// Possibly due to the NodeController has not heard from the kubelet who
	// manages the Pod for more than node-monitor-grace-period but less than
	// pod-eviction-timeout.
	// And after pod-eviction-timeout, the Pod will be marked as deleting, but
	// it will only be automatically deleted after the kubelet comes back and
	// kills the Pod.
	if nil != pod && pod.Status.Phase == core.PodUnknown {
		_,_,err = ci._syncWhenUnknownPodPhase(f.taskRoleName,taskIndex,pod)
		return false , err
	} 

	// Below Pod fields may be available even when PodPending, such as the Pod
	// has been bound to a Node, but one or more Containers has not been started.
	if nil != pod && nil != &pod.Status{
		taskStatus.AttemptStatus.PodIP  = &pod.Status.PodIP;
		taskStatus.AttemptStatus.PodHostIP = &pod.Status.HostIP;
	}

	// state  => TaskAttemptPreparing
	if nil != pod && pod.Status.Phase == core.PodPending {
		_,_,err = ci._syncBeforeTaskAttemptPreparing(f,taskRoleName,taskIndex)
		return false,err
	}

	// state => TaskAttempRunning
	if nil != pod && pod.Status.Phase == core.PodRunning {
		_,_,err = ci._syncBeforeTaskAttemptRunning(f,taskRoleName,taskIndex)
		return false,err
	}

	// succeeded  , state => TaskAttemptDeletionPending
	if nil != pod && pod.Status.Phase == core.PodSucceeded {
		_,_, err = ci._syncWhenTaskAttemptSucceeded(f,taskRoleName,taskIndex)
		return false,err
	}

	//failed , state  => TaskAttemptDeletionPending
	if nil != pod && pod.Status.Phase == core.PodFailed{
		_,_,err = ci._syncWhenTaskAttemptFailed(f,taskRoleName,taskIndex)
		return false,err
	}

	// otherwise ,unrecognized pod phase
	if nil != pod {
		_,_,err = ci._syncWhenUnrecognizedPodPhase(f,taskRoleName,taskIndex,pod)
		return false,err
	}
	
	// three cases when pod is nil

	// before TaskAttemptCreationPending 

	if nil == pod && nil == taskStatus.PodUID() {
		syncExit,cancelled,err = ci._syncBeforeTaskAttemptCreationPending(f,taskRoleName,taskIndex)
	}

	if true == syncExit || nil != err {
		return cancelled,err
	}

	//the request has already been made, but the pod isn't found at local ,just wait
	if nil == pod && nil != taskStatus.PodUID() && taskStatus.State == ci.TaskAttemptCreationRequested{
		syncExit,cancelled,err = ci._syncWaitPodAppearInTheLocal(f,taskRoleName,taskIndex)
	}

	if true == syncExit || nil != err {
		return cancelled,err
	}

	// unexpected pod deletion

	// the legal state are TaskAttemptDeletionRequested ，TaskAttemptDeleting，TaskAttemptCompleted，and TaskCompleted
	if nil == pod && nil != taskStatus.PodUID() && false == isLegalPodNilTaskState(taskStatus.State){
		syncExit,cancelled,err = ci._syncWhenUnexpectedPodDeletion(f,taskRoleName,taskIndex)
	}
	
	if true == syncExit || nil != err {
		return cancelled,err
	}

	// At this point, taskStatus.State must be in:
	// {TaskCompleted, TaskAttemptCreationPending,TaskCompleted}

	if taskStatus.State == ci.TaskAttemptCreationPending {
		// createTaskAttempt
		syncExit,cancelled,err = ci._syncWhenTaskAttemptCreationPending(f,cm,taskRoleName,taskIndex)
	}
 
	if true == syncExit || nil != err {
		return cancelled,err
	}

	if taskStatus.State == ci.TaskAttemptCompleted {
		syncExit,cancelled,err = ci._syncWhenTaskAttemptCompleted(f,taskRoleName,taskIndex)
	}
	
	if true == syncExit || nil != err{
		return cancelled,err
	}
	
	if taskStatus.State == ci.TaskCompleted {
		syncExit,cancelled,err = ci._syncWhenTaskCompleted(f,taskRoleName,taskIndex)
	}
	
	if true == syncExit || nil != err{
		return cancelled,err
	}

	// Unreachable
	panic(fmt.Errorf(logPfx+"Failed: At this point, TaskState should be in {} instead of %v",taskStatus.State))
}



func (c *FrameworkController)_syncWhenUnexpectedPodDeletion(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

		
	if taskStatus.AttemptStatus.CompletionStatus == nil {
		diag := fmt.Sprintf("Pod was deleted by others")
		log.Warnf(logPfx + diag)
		taskStatus.AttemptStatus.CompletionStatus =  ci.CompletionCodePodExternalDeleted.NewCompletionStatus(diag)
	}

	taskStatus.AttemptStatus.CompletionTime = common.PtrNow()
	
	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCompleted,nil)

	log.Infof(logPfx+"TaskAttemptInstance %v is completed with CompletionStatus: %v",*taskStatus.TaskAttemptInstanceUID(),
	taskStatus.AttemptStatus.CompletionStatus)

	return false,false,nil
}


func (c *FrameworkController)_syncBeforeTaskAttemptCreationPending(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationPending,nil)

	return false,false,nil
}


func (c *FrameworkController) _syncWaitPodAppearInTheLocal(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	// Avoid sync with outdated object:
	// pod is remote creation requested but not found in the local cache.

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
		 
	if c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, true) {

		log.Infof(logPfx +"Waiting Pod to appear in the local cache or timeout")

		return  true,false,nil
	}

	diag := fmt.Sprintf("Pod does not appear in the local cache within timeout %v, "+"so consider it was deleted and force delete it",
					common.SecToDuration(c.cConfig.ObjectLocalCacheCreationTimeoutSec))

	log.Warnf(logPfx + diag)

	// Ensure pod is deleted in remote to avoid managed pod leak after
	// TaskCompleted.

	err := c.deletePod(f, taskRoleName, taskIndex, *taskStatus.PodUID())
		
	if err != nil {
		return true,false, err
	}

	taskStatus.AttemptStatus.CompletionStatus = ci.CompletionCodePodCreationTimeout.NewCompletionStatus(diag)

	return false,false,nil

}


func (c *FrameworkController) _syncWhenTaskIsBeingDeleted(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeleting,nil)

	log.Infof(logPfx + "Waiting Pod to be deleted")

	return true, false, nil

}

func (c *FrameworkController)_syncWhenTaskAttemptDeletionPending(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	// The CompletionStatus has been persisted, so it is safe to delete the
	// pod now.

	err := c.deletePod(f, taskRoleName, taskIndex, *taskStatus.PodUID())
	if err != nil {
		return true,false, err
	}

	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptDeletionRequested,nil)

	return false,false,nil

}


func (c *FrameworkController) _synWhenTaskAttemptDeletionRequested(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)


	// The deletion requested object will never appear again with the same UID,
	// so always just wait.
	log.Infof(logPfx +"Waiting Pod to disappearing or disappear in the local cache")

	return  true,false, nil

}


func(c* FrameworkController)_syncWhenUnknownPodPhase(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	log.Infof(logPfx+"Waiting Pod to be deleted or deleting or transitioned from %v",pod.Status.Phase)

	return  true,false, nil
}

// nil != pod &&  pod.Status.Phase == core.PodPending 

func (c *FrameworkController) _syncBeforeTaskAttemptPreparing(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptPreparing,nil)

	return true,false,nil

}

// nil != pod && pod.Status.Phase == core.PodRunning
func (c *FrameworkController) _syncBeforeTaskAttemptRunning(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptRunning,nil)

	return true,false,nil
}

// nil != pod && pod.Status.Phase == core.PodSucceeded 
func (c *FrameworkController)_syncWhenTaskAttemptSucceeded(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)
	
	diag := fmt.Sprintf("Pod succeeded")

	log.Infof(logPfx + diag)

	c.completeTaskAttempt(f, taskRoleName, taskIndex,ci.CompletionCodeSucceeded.NewCompletionStatus(diag))

	return true,false,nil

}

func (c *FrameworkController) _syncWhenUnknownPodPhase(f *ci.Framework,taskRoleName string, 
	taskIndex int32,pod *core.Pod)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	log.Infof(logPfx+"Waiting Pod to be deleted or deleting or transitioned from %v",pod.Status.Phase)
	
	return  true,false,nil

}

func (c *FrameworkController) _syncWhenUnrecognizedPodPhase(f *ci.Framework,taskRoleName string, 
	taskIndex int32,pod *core.Pod)(syncExit bool,cancelled bool,err error){

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	return true,false, fmt.Errorf(logPfx+"Failed: Got unrecognized Pod Phase: %v", pod.Status.Phase)

}


func (c *FrameworkController) _syncWhenTaskAttemptFailed(f *ci.Framework,taskRoleName string, 
	taskIndex int32,pod *core.Pod)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)
	
	allContainerStatuses := append(pod.Status.InitContainerStatuses,pod.Status.ContainerStatuses...)

	lastContainerExitCode := common.NilInt32()
	
	lastContainerCompletionTime := time.Time{}
	
	allContainerDiags := []string{}

	for _, containerStatus := range allContainerStatuses {

		terminated := containerStatus.State.Terminated
		
		if terminated != nil && terminated.ExitCode != 0 {

			allContainerDiags = append(allContainerDiags, fmt.Sprintf("[Container %v, ExitCode: %v, Reason: %v, Message: %v]",
				containerStatus.Name, terminated.ExitCode, terminated.Reason,
				terminated.Message))

			if lastContainerExitCode == nil || lastContainerCompletionTime.Before(terminated.FinishedAt.Time) {
				lastContainerExitCode = &terminated.ExitCode
				lastContainerCompletionTime = terminated.FinishedAt.Time
			}
		}
	}

	if lastContainerExitCode == nil {
		
		diag := fmt.Sprintf("Pod failed without any non-zero container exit code, maybe " +"stopped by the system")

		log.Warnf(logPfx + diag)
		
		c.completeTaskAttempt(f, taskRoleName, taskIndex,ci.CompletionCodePodFailedWithoutFailedContainer.NewCompletionStatus(diag))
	
	} else {
		
		diag := fmt.Sprintf("Pod failed with non-zero container exit code: %v",strings.Join(allContainerDiags, ", "))
		
		log.Infof(logPfx + diag)
		
		if strings.Contains(diag, string(ci.ReasonOOMKilled)) {
			
			c.completeTaskAttempt(f, taskRoleName, taskIndex,ci.CompletionCodeContainerOOMKilled.NewCompletionStatus(diag))
		
		} else {
			c.completeTaskAttempt(f, taskRoleName, taskIndex,ci.CompletionCode(*lastContainerExitCode).NewCompletionStatus(diag))
		}
	}

	return true,false,nil

}


func (c *FrameworkController) _syncWhenTaskAttemptCompleted(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskRoleSpec := f.TaskRoleSpec(taskRoleName)
	taskSpec := taskRoleSpec.Task
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	retryDecision := taskSpec.RetryPolicy.ShouldRetry(taskStatus.RetryPolicyStatus,taskStatus.AttemptStatus.CompletionStatus.Type,0, 0)

	if taskStatus.RetryPolicyStatus.RetryDelaySec == nil {

		// RetryTask is not yet scheduled, so need to be decided.
		if retryDecision.ShouldRetry {
			
			log.Infof(logPfx+"Will retry Task with new TaskAttempt: RetryDecision: %v",retryDecision)

			taskStatus.RetryPolicyStatus.RetryDelaySec = &retryDecision.DelaySec

		} else {
			// completeTask
			log.Infof(logPfx+"Will complete Task: RetryDecision: %v",retryDecision)

			taskStatus.CompletionTime = common.PtrNow()

			f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskCompleted,nil)
		}
	}

	if taskStatus.RetryPolicyStatus.RetryDelaySec != nil {
		// RetryTask is already scheduled, so just need to check timeout.
		if c.enqueueTaskRetryDelayTimeoutCheck(f, taskRoleName, taskIndex, true) {
			log.Infof(logPfx + "Waiting Task to retry after delay")
			return true,false, nil
		}

		// retryTask
		log.Infof(logPfx + "Retry Task")
		taskStatus.RetryPolicyStatus.TotalRetriedCount++
		if retryDecision.IsAccountable {
			taskStatus.RetryPolicyStatus.AccountableRetriedCount++
		}
		taskStatus.RetryPolicyStatus.RetryDelaySec = nil
		taskStatus.AttemptStatus = f.NewTaskAttemptStatus(taskRoleName, taskIndex, taskStatus.RetryPolicyStatus.TotalRetriedCount)

		f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationPending,nil)
	}

	return false,false,nil
}



func (c *FrameworkController) _syncWhenTaskCompleted(f *ci.Framework,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){
	
	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)
	
	taskRoleSpec := f.TaskRoleSpec(taskRoleName)
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	taskRoleStatus := f.TaskRoleStatus(taskRoleName)

	// attemptToCompleteFrameworkAttempt
	completionPolicy := taskRoleSpec.FrameworkAttemptCompletionPolicy

	minFailedTaskCount := completionPolicy.MinFailedTaskCount

	minSucceededTaskCount := completionPolicy.MinSucceededTaskCount
	
	
	if taskStatus.IsFailed() && minFailedTaskCount != ci.UnlimitedValue {
		
		failedTaskCount := taskRoleStatus.GetTaskCount((*ci.TaskStatus).IsFailed)
		
		if failedTaskCount >= minFailedTaskCount {
			diag_template := "FailedTaskCount %v has reached MinFailedTaskCount %v in TaskRole [%v]:Triggered by Task [%v][%v]: Diagnostics: %v"

			diag := fmt.Sprintf(diag_template,failedTaskCount, minFailedTaskCount, taskRoleName,taskRoleName, taskIndex, 
				taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
			
			log.Infof(logPfx + diag)
			
			c.completeFrameworkAttempt(f,taskStatus.AttemptStatus.CompletionStatus.Code.NewCompletionStatus(diag))

			return true,true, nil
		}
	
	}
	
	
	if taskStatus.IsSucceeded() && minSucceededTaskCount != ci.UnlimitedValue {
		
		succeededTaskCount := taskRoleStatus.GetTaskCount((*ci.TaskStatus).IsSucceeded)
		
		if succeededTaskCount >= minSucceededTaskCount {

			diag_template := "SucceededTaskCount %v has reached MinSucceededTaskCount %v in TaskRole [%v]:Triggered by Task [%v][%v]: Diagnostics: %v"
		
			diag := fmt.Sprintf(diag_template,succeededTaskCount, minSucceededTaskCount, taskRoleName,taskRoleName, taskIndex, 
				taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
				
			log.Infof(logPfx + diag)
			
			c.completeFrameworkAttempt(f,ci.CompletionCodeSucceeded.NewCompletionStatus(diag))
			
			return true,true, nil
		}
	
	}
	
	if f.AreAllTasksCompleted() {
		
		totalTaskCount := f.GetTaskCount(nil)
		
		failedTaskCount := f.GetTaskCount((*ci.TaskStatus).IsFailed)
		
		diag_template := "All Tasks are completed and no user specified conditions in FrameworkAttemptCompletionPolicy have ever been triggered: "+
		"TotalTaskCount: %v, FailedTaskCount: %v: Triggered by Task [%v][%v]: Diagnostics: %v",
		
		diag := fmt.Sprintf(diag_template, totalTaskCount, failedTaskCount,taskRoleName, taskIndex, 
			taskStatus.AttemptStatus.CompletionStatus.Diagnostics)
	
		log.Infof(logPfx + diag)
		
		c.completeFrameworkAttempt(f,ci.CompletionCodeSucceeded.NewCompletionStatus(diag))
		
		return true,true, nil
	}
	
	
	return true,false, nil
}


func (c *FrameworkController) _syncWhenTaskAttemptCreationPending(f *ci.Framework, cm *core.ConfigMap,taskRoleName string, 
	taskIndex int32)(syncExit bool,cancelled bool,err error){

	logPfx := fmt.Sprintf("[%v][%v][%v]: syncTaskState: ",f.Key(), taskRoleName, taskIndex)

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	pod, err := c.createPod(f, cm, taskRoleName, taskIndex)

	if err != nil {

		apiErr := errorWrap.Cause(err)

		if apiErrors.IsInvalid(apiErr) {
			// Should be Framework Error instead of Platform Transient Error.
			// Directly complete the FrameworkAttempt, since we should not complete
			// a TaskAttempt without an associated Pod in any case.

			diag := fmt.Sprintf("Pod Spec is invalid in TaskRole [%v]: Triggered by Task [%v][%v]: Diagnostics: %v",
				taskRoleName, taskRoleName, taskIndex, apiErr)

			log.Infof(logPfx + diag)

			c.completeFrameworkAttempt(f,ci.CompletionCodePodSpecInvalid.NewCompletionStatus(diag))

			return true ,true, nil
		}  
		
		
		return true, false, err
		
	}

	taskStatus.AttemptStatus.PodUID = &pod.UID

	taskStatus.AttemptStatus.InstanceUID = ci.GetTaskAttemptInstanceUID(taskStatus.TaskAttemptID(), taskStatus.PodUID())

	f.TransitionTaskState(taskRoleName, taskIndex, ci.TaskAttemptCreationRequested,nil)

	// Informer may not deliver any event if a create is immediately followed by
	// a delete, so manually enqueue a sync to check the pod existence after the
	// timeout.
	c.enqueueTaskAttemptCreationTimeoutCheck(f, taskRoleName, taskIndex, false)

	// The ground truth pod is the local cached one instead of the remote one,
	// so need to wait before continue the sync.
	log.Infof(logPfx +"Waiting Pod to appear in the local cache or timeout")

	return true,false, nil

}
