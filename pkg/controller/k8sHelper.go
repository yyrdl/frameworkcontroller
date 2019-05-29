package controller


import (
	"fmt"
	log "github.com/sirupsen/logrus"
	errorWrap "github.com/pkg/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
)

func (c *FrameworkController) getConfigMapOwner(cm *core.ConfigMap) *ci.Framework {

	cmOwner := meta.GetControllerOf(cm)

	if cmOwner == nil {
		return nil
	}

	if cmOwner.Kind != ci.FrameworkKind {
		return nil
	}

	f, err := c.fLister.Frameworks(cm.Namespace).Get(cmOwner.Name)

	if err != nil {

		if !apiErrors.IsNotFound(err) {

			log.Errorf("[%v]: ConfigMapOwner %#v cannot be got from local cache: %v",cm.Namespace+"/"+cm.Name, *cmOwner, err)
		
		}

		return nil
	}

	if f.UID != cmOwner.UID {
		// GarbageCollectionController will handle the dependent object
		// deletion according to the ownerReferences.
		return nil
	}

	return f
}

func (c *FrameworkController) getPodOwner(pod *core.Pod) *core.ConfigMap {

	podOwner := meta.GetControllerOf(pod)

	if podOwner == nil {
		return nil
	}

	if podOwner.Kind != ci.ConfigMapKind {
		return nil
	}

	cm, err := c.cmLister.ConfigMaps(pod.Namespace).Get(podOwner.Name)

	if err != nil {

		if !apiErrors.IsNotFound(err) {

			log.Errorf("[%v]: PodOwner %#v cannot be got from local cache: %v",pod.Namespace+"/"+pod.Name, *podOwner, err)
		
		}

		return nil
	}

	if cm.UID != podOwner.UID {
		// GarbageCollectionController will handle the dependent object
		// deletion according to the ownerReferences.
		return nil
	}

	return cm
}



// Get Framework's current ConfigMap object, if not found, then clean up existing
// controlled ConfigMap if any.
// Returned cm is either managed or nil, if it is the managed cm, it is not
// writable and may be outdated even if no error.
// Clean up instead of recovery is because the ConfigMapUID is always the ground
// truth.
func (c *FrameworkController) getOrCleanupConfigMap(f *ci.Framework) (*core.ConfigMap, error) {

	cm, err := c.cmLister.ConfigMaps(f.Namespace).Get(f.ConfigMapName())

	if err != nil {

		if apiErrors.IsNotFound(err) {
			return nil, nil
		} 
		
		return nil, fmt.Errorf("[%v]: ConfigMap %v cannot be got from local cache: %v",f.Key(), f.ConfigMapName(), err)
	}

	if f.ConfigMapUID() != nil && *f.ConfigMapUID() == cm.UID{
		// cm is the managed
		return cm,nil
	}

	// cm is the unmanaged


	if meta.IsControlledBy(cm,f){
		// The managed ConfigMap becomes unmanaged if and only if Framework.Status
		// is failed to persist due to FrameworkController restart or create fails
		// but succeeds on remote, so clean up the ConfigMap to avoid unmanaged cm
		// leak.
		return nil, c.deleteConfigMap(f, cm.UID)
	}

	return nil, fmt.Errorf(
		"[%v]: ConfigMap %v naming conflicts with others: "+
				"Existing ConfigMap %v with DeletionTimestamp %v is not "+
				"controlled by current Framework %v, %v",
		f.Key(), f.ConfigMapName(),
		cm.UID, cm.DeletionTimestamp, f.Name, f.UID)
}

// Using UID to ensure we delete the right object.

func (c *FrameworkController) deleteConfigMap(f *ci.Framework, cmUID types.UID) error {

	cmName := f.ConfigMapName()

	err := c.kClient.CoreV1().ConfigMaps(f.Namespace).Delete(cmName,&meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &cmUID}})

	if err != nil && !apiErrors.IsNotFound(err) {
		return fmt.Errorf("[%v]: Failed to delete ConfigMap %v, %v: %v",f.Key(), cmName, cmUID, err)
	} 
	 
	log.Infof("[%v]: Succeeded to delete ConfigMap %v, %v",f.Key(), cmName, cmUID)
	
    return nil
	
}

func (c *FrameworkController) createConfigMap(f *ci.Framework) (*core.ConfigMap, error) {

	cm := f.NewConfigMap()

	remoteCM, err := c.kClient.CoreV1().ConfigMaps(f.Namespace).Create(cm)

	if err != nil {
		return nil, fmt.Errorf("[%v]: Failed to create ConfigMap %v: %v",f.Key(), cm.Name, err)
	} 
	
	
	log.Infof("[%v]: Succeeded to create ConfigMap %v",f.Key(), cm.Name)
	
	return remoteCM, nil
	
}



// Get Task's current Pod object, if not found, then clean up existing
// controlled Pod if any.
// Returned pod is either managed or nil, if it is the managed pod, it is not
// writable and may be outdated even if no error.
// Clean up instead of recovery is because the PodUID is always the ground truth.
func (c *FrameworkController) getOrCleanupPod(f *ci.Framework, cm *core.ConfigMap,taskRoleName string, taskIndex int32) (*core.Pod, error) {

	var err_template string

	taskStatus := f.TaskStatus(taskRoleName, taskIndex)

	pod, err := c.podLister.Pods(f.Namespace).Get(taskStatus.PodName())
	
	if nil != err {

		if apiErrors.IsNotFound(err){
			return nil ,nil
		}

		err_template = "[%v][%v][%v]: Pod %v cannot be got from local cache: %v"
		
		return nil , fmt.Errorf(err_template,f.Key(), taskRoleName, taskIndex, taskStatus.PodName(), err)
	}

	if taskStatus.PodUID() != nil && * taskStatus.PodUID() == pod.UID{
		return pod,nil
	}

	// pod is the unmanaged

	if meta.IsControlledBy(pod,cm){
		// The managed Pod becomes unmanaged if and only if Framework.Status
		// is failed to persist due to FrameworkController restart or create fails
		// but succeeds on remote, so clean up the Pod to avoid unmanaged pod leak.
		return nil, c.deletePod(f, taskRoleName, taskIndex, pod.UID)
	}

	err_template = 	"[%v][%v][%v]: Pod %v naming conflicts with others: Existing Pod %v with DeletionTimestamp %v is not controlled by current ConfigMap %v, %v"

	return nil, fmt.Errorf( err_template,f.Key(), taskRoleName, taskIndex, taskStatus.PodName(),pod.UID, pod.DeletionTimestamp, cm.Name, cm.UID)
	 
}

// Using UID to ensure we delete the right object.
func (c *FrameworkController) deletePod(f *ci.Framework, taskRoleName string, taskIndex int32,podUID types.UID) error {
	
	taskStatus := f.TaskStatus(taskRoleName, taskIndex)
	
	podName := taskStatus.PodName()
	
	err := c.kClient.CoreV1().Pods(f.Namespace).Delete(podName,&meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &podUID}})
	
	if err != nil && !apiErrors.IsNotFound(err) {

		return fmt.Errorf("[%v][%v][%v]: Failed to delete Pod %v, %v: %v",f.Key(), taskRoleName, taskIndex, podName, podUID, err)
	
	}  
		
	log.Infof("[%v][%v][%v]: Succeeded to delete Pod %v, %v",f.Key(), taskRoleName, taskIndex, podName, podUID)
	
	return nil
	 
}

func (c *FrameworkController) createPod(f *ci.Framework, cm *core.ConfigMap,taskRoleName string, taskIndex int32) (*core.Pod, error) {

	pod := f.NewPod(cm, taskRoleName, taskIndex)

	remotePod, err := c.kClient.CoreV1().Pods(f.Namespace).Create(pod)

	if err != nil {
		return nil, errorWrap.Wrapf(err,"[%v]: Failed to create Pod %v", f.Key(), pod.Name)
	} 


	log.Infof("[%v]: Succeeded to create Pod: %v", f.Key(), pod.Name)

	return remotePod, nil

}