package controller


import (
	"fmt"
	"time"
	"reflect"
	log "github.com/sirupsen/logrus"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	errorAgg "k8s.io/apimachinery/pkg/util/errors"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/util"
	"github.com/microsoft/frameworkcontroller/pkg/common"
)


// It should not be invoked concurrently with the same key.
// Return error only for Platform Transient Error, so that the key
// can be enqueued again after rate limited delay.
// For Platform Permanent Error, it should be delivered by panic.
// For Framework Error, it should be delivered into Framework.Status.

func (c *FrameworkController) syncFramework(key string) (returnedErr error) {

	startTime := time.Now()

	logPfx := fmt.Sprintf("[%v]: syncFramework: ", key)

	log.Infof(logPfx + "Started")

	defer func() {

		if returnedErr != nil {
			// returnedErr is already prefixed with logPfx

			log.Warnf(returnedErr.Error())

			log.Warnf(logPfx +"Failed to due to Platform Transient Error. " + "Will enqueue it again after rate limited delay")
		}

		log.Infof(logPfx+"Completed: Duration %v", time.Since(startTime))

	}()

	namespace, name, err := util.SplitKey(key)

	if err != nil {
		// Unreachable
		panic(fmt.Errorf(logPfx+"Failed: Got invalid key from queue, but the queue should only contain "+"valid keys: %v", err))
	}

	localF, err := c.fLister.Frameworks(namespace).Get(name)

	if nil != err{

		if apiErrors.IsNotFound(err){
			// GarbageCollectionController will handle the dependent object
			// deletion according to the ownerReferences.
			log.Infof(logPfx + "Skipped: Framework cannot be found in local cache: %v", err)
			c.deleteExpectedFrameworkStatusInfo(key)
			return nil
		}

		return fmt.Errorf(logPfx+"Failed: Framework cannot be got from local cache: %v", err)
		 
	}

	if nil != localF.DeletionTimestamp{
		// Skip syncFramework to avoid fighting with GarbageCollectionController,
		// because GarbageCollectionController may be deleting the dependent object.
		log.Infof(logPfx+"Skipped: Framework is deleting: Will be deleted at %v",localF.DeletionTimestamp)
		return nil
	}


	f := localF.DeepCopy()

	// From now on, f is a writable copy of the original local cached one, and
	// it may be different from the original one.

	expected, exists := c.getExpectedFrameworkStatusInfo(f.Key())

	if !exists {
		if f.Status != nil {
			// Recover f related things, since it is the first time we see it and
			// its Status is not nil.
			c.recoverFrameworkWorkItems(f)
		}

		// f.Status must be the same as the remote one, since it is the first
		// time we see it.
		c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, true)
	} else {
		// f.Status may be outdated, so override it with the expected one, to
		// ensure the Framework.Status is Monotonically Exposed.
		f.Status = expected.status

		// Ensure the expected Framework.Status is the same as the remote one
		// before sync.
		if !expected.remoteSynced {
			updateErr := c.updateRemoteFrameworkStatus(f)
			if updateErr != nil {
				return updateErr
			}
			c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, true)
		}
	}

	// At this point, f.Status is the same as the expected and remote
	// Framework.Status, so it is ready to sync against f.Spec and other
	// related objects.
	errs := []error{}
	remoteF := f.DeepCopy()

	syncErr := c.syncFrameworkStatus(f)
	errs = append(errs, syncErr)

	if !reflect.DeepEqual(remoteF.Status, f.Status) {
		// Always update the expected and remote Framework.Status even if sync
		// error, since f.Status should never be corrupted due to any Platform
		// Transient Error, so no need to rollback to the one before sync, and
		// no need to DeepCopy between f.Status and the expected one.
		updateErr := c.updateRemoteFrameworkStatus(f)
		errs = append(errs, updateErr)

		c.updateExpectedFrameworkStatusInfo(f.Key(), f.Status, updateErr == nil)
	} else {
		log.Infof(logPfx +"Skip to update the expected and remote Framework.Status since " + "they are unchanged")
	}

	return errorAgg.NewAggregate(errs)
	 
}



func (c *FrameworkController) syncFrameworkStatus(f *ci.Framework) error {

	logPfx := fmt.Sprintf("[%v]: syncFrameworkStatus: ", f.Key())

	log.Infof(logPfx + "Started")

	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status == nil {
		f.Status = f.NewFrameworkStatus()
	} else {
		// TODO: Support Framework.Spec Update
	}

	return c.syncFrameworkState(f)
}

 
func (c *FrameworkController) syncFrameworkState(f *ci.Framework) error{

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())

	log.Infof(logPfx + "Started")

	defer func() { log.Infof(logPfx + "Completed") }()

	if f.Status.State == ci.FrameworkCompleted {
		log.Infof(logPfx + "Skipped: Framework is already completed")
		return nil
	}

	// Get the ground truth readonly cm
	cm, err := c.getOrCleanupConfigMap(f)

	if err != nil {
		return err
	}

	var syncExit bool

	// need to delete the configMap, just Delete it

	if nil != cm  && nil == cm.DeletionTimestamp &&  f.Status.State == ci.FrameworkAttemptDeletionPending {
		syncExit,err = c._syncWhenFrameworkAttemptDeletionPending(f)
		if syncExit == true || nil != err{
			return err
		}
	}
	
	// The configMap has already been deleted or is being deleted, just wait until it is totally completed

	// 等待 删除ConfigMap 彻底完成

	if nil != cm && (nil != cm.DeletionTimestamp || f.Status.State == ci.FrameworkAttemptDeletionRequested) {

		syncExit,err = c._syncWaitConfigMapDeletionCompleted(f,nil != cm.DeletionTimestamp)

		return err
	}

	
	if nil != cm && f.Status.State != ci.FrameworkAttemptPreparing && f.Status.State != ci.FrameworkAttemptRunning{
		syncExit,err = c._syncWhenFrameworkAttemptCreationRequested(f)
		if syncExit == true || nil != err{
			return err
		}
	}

	//the framework has not been created , switch to state "FrameworkAttemptCreationPending"
	//表明　controller 还未创建这个 framework ,　向下一个状态转变

	if nil == cm && nil == f.ConfigMapUID() {
		syncExit,err = c._syncBeforeFrameworkAttemptCreationPending(f)
	}

	if syncExit == true || nil != err{
		return err
	}

	// Avoid sync with outdated object:
	// cm is remote creation requested but not found in the local cache.
	// 在controller 已经发起创建 cm的请求，但是 cm　还未更新到本地缓存
	if nil == cm && f.Status.State == ci.FrameworkAttemptCreationRequested{
		syncExit,err = c._syncWaitConfigMapAppearInTheLocal(f)
	}

	if syncExit == true || nil != err{
		return err
	}

	// unexpected configMap deletion
	//　ConfigMap　被非预期的删除
	if nil == cm && false == isLegalConfigMapNilFrameworkState(f.Status.State){
		syncExit, err = c._syncWhenUnexpectedConfigMapDeletion(f)
	}

	if syncExit == true || nil != err{
		return err
	}

	// State 是删除的过渡状态， cm 已经消失了,删除彻底完成,转到下一个状态
	if nil == cm && ( f.Status.State == ci.FrameworkAttemptDeletionRequested  || f.Status.State == ci.FrameworkAttemptDeleting){
		syncExit,err = c._syncWhenConfigMapDeletionDone(f)
	}

	if syncExit == true || nil != err{
		return err
	}

	if f.Status.State == ci.FrameworkAttemptCompleted{
		syncExit,err = c._syncWhenFrameworkAttemptCompleted(f)
	}

	if syncExit == true || nil != err{
		return err
	}

	if f.Status.State == ci.FrameworkAttemptCreationPending{
		syncExit,err = c._syncWhenFrameworkAttemptCreationPending(f)
	}

	if syncExit == true || nil != err{
		return err
	}
 
	// FrameworkAttemptPreparing => FrameworkAttemptPreparing  or  FrameworkAttemptPreparing => FrameworkAttemptRunning
	// FrameworkAttemptRunning => FrameworkAttemptRunning or  FrameworkAttemptRunning => FrameworkAttemptPreparing

	if f.Status.State == ci.FrameworkAttemptPreparing || f.Status.State == ci.FrameworkAttemptRunning{
		syncExit,err = c._syncFrameworkAttemptRunningState(f,cm)
	}

	if syncExit == true || nil != err{
		return err
	}

	// Unreachable
	panic(fmt.Errorf(logPfx+"Failed: At this point, FrameworkState should be in {%v, %v} instead of %v",
			ci.FrameworkAttemptPreparing, ci.FrameworkAttemptRunning, f.Status.State))

}

// FrameworkAttemptPreparing => FrameworkAttemptPreparing  or  FrameworkAttemptPreparing => FrameworkAttemptRunning
// FrameworkAttemptRunning => FrameworkAttemptRunning or  FrameworkAttemptRunning => FrameworkAttemptPreparing

func (c *FrameworkController)_syncFrameworkAttemptRunningState(f *ci.Framework,cm *core.ConfigMap)(bool,error){
	
	cancelled, err := c.syncTaskRoleStatuses(f, cm)

	if !cancelled {
		if !f.IsAnyTaskRunning() {
			f.TransitionFrameworkState(ci.FrameworkAttemptPreparing)
		} else {
			f.TransitionFrameworkState(ci.FrameworkAttemptRunning)
		}
	}

	return true, err
}


func (c *FrameworkController)_syncWhenFrameworkAttemptCreationPending(f *ci.Framework)(bool,error){

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())
	// createFrameworkAttempt
	cm, err := c.createConfigMap(f)

	if err != nil {
		return true,err
	}

	f.Status.AttemptStatus.ConfigMapUID = &cm.UID

	f.Status.AttemptStatus.InstanceUID = ci.GetFrameworkAttemptInstanceUID(f.FrameworkAttemptID(), f.ConfigMapUID())

	f.TransitionFrameworkState(ci.FrameworkAttemptCreationRequested)

	// Informer may not deliver any event if a create is immediately followed by
	// a delete, so manually enqueue a sync to check the cm existence after the
	// timeout.
	c.enqueueFrameworkAttemptCreationTimeoutCheck(f, false)

	// The ground truth cm is the local cached one instead of the remote one,
	// so need to wait before continue the sync.

	log.Infof(logPfx +"Waiting ConfigMap to appear in the local cache or timeout")

	return true,nil
}

func (c *FrameworkController) _syncWhenFrameworkAttemptCompleted(f *ci.Framework)(bool,error){

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())
	
	retryDecision := f.Spec.RetryPolicy.ShouldRetry(
		f.Status.RetryPolicyStatus,
		f.Status.AttemptStatus.CompletionStatus.Type,
		*c.cConfig.FrameworkMinRetryDelaySecForTransientConflictFailed,
		*c.cConfig.FrameworkMaxRetryDelaySecForTransientConflictFailed)
		
	
	if f.Status.RetryPolicyStatus.RetryDelaySec == nil {
		// RetryFramework is not yet scheduled, so need to be decided.
		
		if retryDecision.ShouldRetry {
			
			// scheduleToRetryFramework
			log.Infof(logPfx+"Will retry Framework with new FrameworkAttempt: RetryDecision: %v",retryDecision)
			f.Status.RetryPolicyStatus.RetryDelaySec = &retryDecision.DelaySec

		} else {
			// completeFramework
			
			log.Infof(logPfx+"Will complete Framework: RetryDecision: %v",retryDecision)
			
			f.Status.CompletionTime = common.PtrNow()
			f.TransitionFrameworkState(ci.FrameworkCompleted)
			return  true,nil
		}
	}

	if f.Status.RetryPolicyStatus.RetryDelaySec != nil {
		// RetryFramework is already scheduled, so just need to check timeout.
		if c.enqueueFrameworkRetryDelayTimeoutCheck(f, true) {
			log.Infof(logPfx + "Waiting Framework to retry after delay")
			return true,nil
		}

		// retryFramework
		log.Infof(logPfx + "Retry Framework")
		f.Status.RetryPolicyStatus.TotalRetriedCount++

		if retryDecision.IsAccountable {
			f.Status.RetryPolicyStatus.AccountableRetriedCount++
		}

		f.Status.RetryPolicyStatus.RetryDelaySec = nil

		f.Status.AttemptStatus = f.NewFrameworkAttemptStatus(f.Status.RetryPolicyStatus.TotalRetriedCount)

		f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
	}

	return false,nil
}


func (c *FrameworkController) _syncWhenFrameworkAttemptCreationRequested(f *ci.Framework)(bool,error){
	f.TransitionFrameworkState(ci.FrameworkAttemptPreparing)
	return false,nil
}

func (c *FrameworkController) _syncWaitConfigMapDeletionCompleted(f *ci.Framework,isDeleting bool)(bool,error){

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())

	if true == isDeleting {
		// the DeletionTimeStamp is not nil, and the resource is going to be deleted by k8s
		f.TransitionFrameworkState(ci.FrameworkAttemptDeleting)
	}
	// else , we just made the deletion request 
	
	log.Infof(logPfx + "Waiting ConfigMap to be deleted, and waiting ConfigMap to disappearing or disappear in the local cache")

	return true, nil
}

func (c *FrameworkController) _syncWhenConfigMapDeletionDone(f *ci.Framework)(bool,error){
	
	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())

	if f.Status.AttemptStatus.CompletionStatus == nil {
		diag := fmt.Sprintf("ConfigMap was deleted by others")
		log.Warnf(logPfx + diag)
		f.Status.AttemptStatus.CompletionStatus =
				ci.CompletionCodeConfigMapExternalDeleted.NewCompletionStatus(diag)
	}

	f.Status.AttemptStatus.CompletionTime = common.PtrNow()
	f.TransitionFrameworkState(ci.FrameworkAttemptCompleted)
	log.Infof(logPfx+
			"FrameworkAttemptInstance %v is completed with CompletionStatus: %v",
		*f.FrameworkAttemptInstanceUID(),
		f.Status.AttemptStatus.CompletionStatus)

	return false,nil
}


func (c *FrameworkController)_syncWhenFrameworkAttemptDeletionPending(f *ci.Framework)(bool,error){
	// The CompletionStatus has been persisted, so it is safe to delete the
	// cm now.
	err := c.deleteConfigMap(f, *f.ConfigMapUID())

	if err != nil {
		return true, err
	}

	f.TransitionFrameworkState(ci.FrameworkAttemptDeletionRequested)

	return false,nil
}
 
func (c *FrameworkController) _syncWhenUnexpectedConfigMapDeletion(f *ci.Framework)(bool,error){

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())

	if f.Status.AttemptStatus.CompletionStatus == nil {

		diag := fmt.Sprintf("ConfigMap was deleted by others")

		log.Warnf(logPfx + diag)

		f.Status.AttemptStatus.CompletionStatus =
				ci.CompletionCodeConfigMapExternalDeleted.NewCompletionStatus(diag)
	}

	f.Status.AttemptStatus.CompletionTime = common.PtrNow()

	f.TransitionFrameworkState(ci.FrameworkAttemptCompleted)

	log.Infof(logPfx+"FrameworkAttemptInstance %v is completed with CompletionStatus: %v",
		*f.FrameworkAttemptInstanceUID(),
		f.Status.AttemptStatus.CompletionStatus)
	
	return false,nil

}


func (c *FrameworkController)_syncBeforeFrameworkAttemptCreationPending(f *ci.Framework)(bool,error){
	f.TransitionFrameworkState(ci.FrameworkAttemptCreationPending)
	return false,nil
}


func (c *FrameworkController) _syncWaitConfigMapAppearInTheLocal(f *ci.Framework)(bool,error){

	logPfx := fmt.Sprintf("[%v]: syncFrameworkState: ", f.Key())

	if c.enqueueFrameworkAttemptCreationTimeoutCheck(f, true) {
		log.Infof(logPfx +
				"Waiting ConfigMap to appear in the local cache or timeout")
		return true,nil
	}

	diag := fmt.Sprintf(
		"ConfigMap does not appear in the local cache within timeout %v, "+
				"so consider it was deleted and force delete it",
		common.SecToDuration(c.cConfig.ObjectLocalCacheCreationTimeoutSec))


	log.Warnf(logPfx + diag)

	// Ensure cm is deleted in remote to avoid managed cm leak after
	// FrameworkCompleted.
	err := c.deleteConfigMap(f, *f.ConfigMapUID())

	if err != nil {
		return true, err
	}

	f.Status.AttemptStatus.CompletionStatus =
			ci.CompletionCodeConfigMapCreationTimeout.NewCompletionStatus(diag)
	return false,nil
}