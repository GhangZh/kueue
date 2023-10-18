/*
Copyright 2022 The Kubernetes Authors.

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
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	volcanoapi "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
)

type VolcanoJobWebhook struct {
	manageJobsWithoutQueueName bool
}

func WebhookType() runtime.Object {
	return &volcanoapi.Job{}
}

// SetupVoclnaoJobWebhook configures the webhook for batchJob.
func SetupVoclnaoJobWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.DefaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	wh := &VolcanoJobWebhook{
		manageJobsWithoutQueueName: options.ManageJobsWithoutQueueName,
	}
	return ctrl.NewWebhookManagedBy(mgr).
		For(WebhookType()).
		WithDefaulter(wh).
		WithValidator(wh).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-batch-v1-job,mutating=true,failurePolicy=fail,sideEffects=None,groups=batch,resources=jobs,verbs=create,versions=v1,name=mjob.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &VolcanoJobWebhook{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the type
func (w *VolcanoJobWebhook) Default(ctx context.Context, obj runtime.Object) error {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("volcanojob-webhook")
	log.V(5).Info("Applying defaults", "job", klog.KObj(job))

	jobframework.ApplyDefaultForSuspend(job, w.manageJobsWithoutQueueName)

	return nil
}

var _ webhook.CustomValidator = &VolcanoJobWebhook{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *VolcanoJobWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("volcanojob-webhook")
	log.Info("Validating create", "job", klog.KObj(job))
	return nil, validateCreate(job).ToAggregate()
}

func validateCreate(job jobframework.GenericJob) field.ErrorList {
	return jobframework.ValidateCreateForQueueName(job)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *VolcanoJobWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldJob := fromObject(oldObj)
	newJob := fromObject(newObj)
	log := ctrl.LoggerFrom(ctx).WithName("volcanojob-webhook")
	log.Info("Validating update", "job", klog.KObj(newJob))
	allErrs := jobframework.ValidateUpdateForQueueName(oldJob, newJob)
	return nil, allErrs.ToAggregate()
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type
func (w *VolcanoJobWebhook) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}
