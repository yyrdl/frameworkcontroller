package controller



import (
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
)

func isLegalPodNilTaskState(state TaskState) bool {
	
	return state == ci.TaskAttemptCreationPending || state == ci.TaskAttemptDeletionRequested || state == ci.TaskAttemptDeleting || state == ci.TaskAttemptCompleted || state == ci.TaskCompleted

}