package controller

import (
	log "github.com/sirupsen/logrus"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/common"
	"github.com/microsoft/frameworkcontroller/pkg/util"
)

// obj could be *ci.Framework or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkObj(obj interface{}, msg string) {
	key, err := util.GetKey(obj)
	if err != nil {
		log.Errorf("Failed to get key for obj %#v, skip to enqueue: %v", obj, err)
		return
	}

	_, _, err = util.SplitKey(key)
	if err != nil {
		log.Errorf("Got invalid key %v for obj %#v, skip to enqueue: %v", key, obj, err)
		return
	}

	c.fQueue.Add(key)
	log.Infof("[%v]: enqueueFrameworkObj: %v", key, msg)
}

// obj could be *core.ConfigMap or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkConfigMapObj(obj interface{}, msg string) {
	if cm := util.ToConfigMap(obj); cm != nil {
		if f := c.getConfigMapOwner(cm); f != nil {
			c.enqueueFrameworkObj(f, msg+": "+cm.Name)
		}
	}
}

// obj could be *core.Pod or cache.DeletedFinalStateUnknown.
func (c *FrameworkController) enqueueFrameworkPodObj(obj interface{}, msg string) {
	if pod := util.ToPod(obj); pod != nil {
		if cm := c.getPodOwner(pod); cm != nil {
			c.enqueueFrameworkConfigMapObj(cm, msg+": "+pod.Name)
		}
	}
}


func (c *FrameworkController) enqueueFrameworkAttemptCreationTimeoutCheck(f *ci.Framework, failIfTimeout bool) bool {
	
	if f.Status.State != ci.FrameworkAttemptCreationRequested {
		return false
	}

	// https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/controller_utils.go#L68
	// ObjectLocalCacheCreationTimeoutSec  => ExpectationsTimeout

	leftDuration := common.CurrentLeftDuration(f.Status.TransitionTime,c.cConfig.ObjectLocalCacheCreationTimeoutSec)

	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)
	log.Infof("[%v]: enqueueFrameworkAttemptCreationTimeoutCheck after %v",f.Key(), leftDuration)

	return true
}

func (c *FrameworkController) enqueueTaskAttemptCreationTimeoutCheck(f *ci.Framework, taskRoleName string, taskIndex int32,failIfTimeout bool) bool {
	
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	
	if taskStatus.State != ci.TaskAttemptCreationRequested {
		return false
	}

	leftDuration := common.CurrentLeftDuration(taskStatus.TransitionTime,c.cConfig.ObjectLocalCacheCreationTimeoutSec)

	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)

	log.Infof("[%v][%v][%v]: enqueueTaskAttemptCreationTimeoutCheck after %v",f.Key(), taskRoleName, taskIndex, leftDuration)

	return true
}

func (c *FrameworkController) enqueueFrameworkRetryDelayTimeoutCheck(f *ci.Framework, failIfTimeout bool) bool {

	if f.Status.State != ci.FrameworkAttemptCompleted {
		return false
	}

	leftDuration := common.CurrentLeftDuration(f.Status.TransitionTime,f.Status.RetryPolicyStatus.RetryDelaySec)

	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)

	log.Infof("[%v]: enqueueFrameworkRetryDelayTimeoutCheck after %v",f.Key(), leftDuration)

	return true
}

func (c *FrameworkController) enqueueTaskRetryDelayTimeoutCheck(f *ci.Framework, taskRoleName string, taskIndex int32,failIfTimeout bool) bool {
	
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	
	if taskStatus.State != ci.TaskAttemptCompleted {
		return false
	}

	leftDuration := common.CurrentLeftDuration(taskStatus.TransitionTime,taskStatus.RetryPolicyStatus.RetryDelaySec)
	 
	if common.IsTimeout(leftDuration) && failIfTimeout {
		return false
	}

	c.fQueue.AddAfter(f.Key(), leftDuration)

	log.Infof("[%v][%v][%v]: enqueueTaskRetryDelayTimeoutCheck after %v",f.Key(), taskRoleName, taskIndex, leftDuration)

	return true
}

func (c *FrameworkController) enqueueFramework(f *ci.Framework, msg string) {
	c.fQueue.Add(f.Key())
	log.Infof("[%v]: enqueueFramework: %v", f.Key(), msg)
}