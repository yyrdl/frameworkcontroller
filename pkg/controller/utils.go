package controller


import (
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
)

func isLegalPodNilTaskState(state ci.TaskState) bool {
	
	return state == ci.TaskAttemptCreationPending || state == ci.TaskAttemptDeletionRequested || state == ci.TaskAttemptDeleting || state == ci.TaskAttemptCompleted || state == ci.TaskCompleted

}


func isLegalConfigMapNilFrameworkState (state ci.FrameworkState) bool{

	return state == ci.FrameworkAttemptCreationPending || state == ci.FrameworkAttemptDeletionRequested || state == ci.FrameworkAttemptDeleting || state == ci.FrameworkAttemptCompleted || state == ci.FrameworkCompleted
}