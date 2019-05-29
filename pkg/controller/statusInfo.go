
package controller 

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/util/retry"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
)

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
	}  
	
	
	return nil
	
}


func (c *FrameworkController) getExpectedFrameworkStatusInfo(key string) (*ExpectedFrameworkStatusInfo, bool) {

	value, exists := c.fExpectedStatusInfos[key]

	return value, exists
}

func (c *FrameworkController) deleteExpectedFrameworkStatusInfo(key string) {

	log.Infof("[%v]: deleteExpectedFrameworkStatusInfo: ", key)

	delete(c.fExpectedStatusInfos, key)
}

func (c *FrameworkController) updateExpectedFrameworkStatusInfo(key string,status *ci.FrameworkStatus, remoteSynced bool) {

	log.Infof("[%v]: updateExpectedFrameworkStatusInfo", key)

	c.fExpectedStatusInfos[key] = &ExpectedFrameworkStatusInfo{
		status:       status,
		remoteSynced: remoteSynced,
	}
}
