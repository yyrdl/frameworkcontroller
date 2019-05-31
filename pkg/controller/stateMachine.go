package controller

import (
	core "k8s.io/api/core/v1"
	ci "github.com/microsoft/frameworkcontroller/pkg/apis/frameworkcontroller/v1"
)

type StateHandler struct {
	
}

type FrameworkStateMachine struct {
	
}

type TaskStateMachine struct {
	pod * core.Pod
}


func (m * TaskStateMachine) Context(f *ci.Framework) *TaskStateMachine{
	
	return nil
}

func (m * TaskStateMachine)Next(){

}