package controller


import (
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
	"github.com/microsoft/frameworkcontroller/pkg/util"
)



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