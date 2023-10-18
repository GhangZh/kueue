/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volcanojob

import (
	"context"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"strings"
	volcanoapi "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
)

var (
	gvk = volcanoapi.SchemeGroupVersion.WithKind("Job")

	FrameworkName = "batch.volcano.sh/v1alpha1"
)

func init() {
	utilruntime.Must(jobframework.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:           SetupIndexes,
		NewReconciler:          NewReconciler,
		SetupWebhook:           SetupVoclnaoJobWebhook,
		JobType:                &volcanoapi.Job{},
		AddToScheme:            volcanoapi.AddToScheme,
		IsManagingObjectsOwner: isVolcanoJob,
	}))
}

// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=list;get;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;watch;update;patch
// +kubebuilder:rbac:groups=batch.volcano.sh,resources=mpijobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch.volcano.sh,resources=mpijobs/status,verbs=get;update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/finalizers,verbs=update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=resourceflavors,verbs=get;list;watch

var NewReconciler = jobframework.NewGenericReconciler(func() jobframework.GenericJob { return &VolcanoJob{} }, nil)

func isVolcanoJob(owner *metav1.OwnerReference) bool {
	return owner.Kind == "Job" && strings.HasPrefix(owner.APIVersion, "batch.volcano.sh/v1alpha1")
}

type VolcanoJob volcanoapi.Job

var _ jobframework.GenericJob = (*VolcanoJob)(nil)
var _ jobframework.JobWithPriorityClass = (*VolcanoJob)(nil)

func (j *VolcanoJob) Object() client.Object {
	return (*volcanoapi.Job)(j)
}

func fromObject(o runtime.Object) *VolcanoJob {
	return (*VolcanoJob)(o.(*volcanoapi.Job))
}

func (j *VolcanoJob) IsSuspended() bool {
	log := ctrl.LoggerFrom(context.Background())
	log.V(2).WithValues("Name", j.GetName(), "Status", j.Status.State.Phase).Info("IsSuspended")
	annotations := j.GetAnnotations()
	if annotations != nil && annotations["suspend"] == "true" {
		return true
	}

	return false
}

func (j *VolcanoJob) IsActive() bool {
	return j.Status.Running != 0
}

func (j *VolcanoJob) Suspend() {
	if j.Annotations == nil {
		j.Annotations = make(map[string]string)
	}
	j.Annotations["suspend"] = "true"
}

func (j *VolcanoJob) GetGVK() schema.GroupVersionKind {
	return gvk
}

func (j *VolcanoJob) PodSets() []kueue.PodSet {
	if len(j.Spec.Tasks) == 0 {
		return nil
	}

	minCount := ptr.To[int32](j.Spec.MinAvailable)
	var podSets []kueue.PodSet
	for _, task := range j.Spec.Tasks {
		podSets = append(podSets, kueue.PodSet{
			Name:     kueue.DefaultPodSetName,
			Template: *task.Template.DeepCopy(),
			Count:    task.Replicas,
			MinCount: minCount,
		})
	}

	return podSets
}

func (j *VolcanoJob) RunWithPodSetsInfo(podSetsInfo []jobframework.PodSetInfo) error {
	j.Annotations["suspend"] = "false"
	if len(podSetsInfo) != 1 {
		return jobframework.BadPodSetsInfoLenError(1, len(podSetsInfo))
	}
	return nil

}

func (j *VolcanoJob) ReclaimablePods() []kueue.ReclaimablePod {
	if len(j.Spec.Tasks) == 0 {
		return nil
	}
	parallelism := ptr.Deref(&j.Spec.Tasks[0].Replicas, 1)
	log := ctrl.Log.WithName(j.Name)
	if parallelism == 1 || j.Status.Succeeded == 0 {
		return nil
	}

	remaining := ptr.Deref(j.Spec.MinSuccess, parallelism) - j.Status.Succeeded - j.Status.Failed
	if remaining >= parallelism {
		log.V(2).Info("ReclaimablePods", "parallelism", parallelism, "remaining", remaining)
		return nil
	}
	return []kueue.ReclaimablePod{{
		Name:  kueue.DefaultPodSetName,
		Count: parallelism - remaining,
	}}
}

func (j *VolcanoJob) RestorePodSetsInfo(podSetInfos []jobframework.PodSetInfo) bool {
	return false
}

func (j *VolcanoJob) Finished() (metav1.Condition, bool) {
	var conditionType volcanoapi.JobPhase
	var finished bool
	status := j.Status.State.Phase
	if status == volcanoapi.Completed || status == volcanoapi.Failed || status == volcanoapi.Terminated || status == volcanoapi.Aborted {
		conditionType = status
		finished = true
	}

	condition := metav1.Condition{
		Type:    kueue.WorkloadFinished,
		Status:  metav1.ConditionTrue,
		Reason:  "JobFinished",
		Message: "Job finished successfully",
	}
	if conditionType == volcanoapi.Failed {
		condition.Message = "VolcanoJob failed"
	}

	return condition, finished
}

func (j *VolcanoJob) PriorityClass() string {
	return j.Spec.PriorityClassName
}

func (j *VolcanoJob) PodsReady() bool {
	if j.Status.State.Phase == volcanoapi.Running {
		return true
	}
	return false
}

func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

func GetWorkloadNameForVolcanoJob(jobName string) string {
	return jobframework.GetWorkloadNameForOwnerWithGVK(jobName, gvk)
}
