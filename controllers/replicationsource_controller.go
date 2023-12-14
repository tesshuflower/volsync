/*
Copyright 2020 The VolSync authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controllers

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	vgsnapv1alpha1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumegroupsnapshot/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	sm "github.com/backube/volsync/controllers/statemachine"
	"github.com/backube/volsync/controllers/utils"
)

// ReplicationSourceReconciler reconciles a ReplicationSource object
type ReplicationSourceReconciler struct {
	client.Client
	Log           logr.Logger
	Scheme        *runtime.Scheme
	EventRecorder record.EventRecorder
}

type rsMachine struct {
	rs      *volsyncv1alpha1.ReplicationSource
	client  client.Client
	logger  logr.Logger
	metrics volsyncMetrics
	mover   mover.Mover
}

var _ sm.ReplicationMachine = &rsMachine{}

//nolint:lll
//nolint:funlen
//+kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=volsync.backube,resources=replicationsources/finalizers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=volsync.backube,resources=replicationsources/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
//+kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;update;patch
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,resourceNames=volsync-privileged-mover,verbs=use
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=groupsnapshot.storage.k8s.io,resources=volumegroupsnapshots,verbs=get;list;watch;create;update;patch;delete;deletecollection

//nolint:funlen
func (r *ReplicationSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("replicationsource", req.NamespacedName)
	inst := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Client.Get(ctx, req.NamespacedName, inst); err != nil {
		if kerrors.IsNotFound(err) {
			logger.Error(err, "Failed to get Source")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if inst.Status == nil {
		inst.Status = &volsyncv1alpha1.ReplicationSourceStatus{}
	}

	var result ctrl.Result
	var err error

	// Check if privileged movers are allowed via namespace annotation
	privilegedMoverOk, err := utils.PrivilegedMoversOk(ctx, r.Client, logger, inst.GetNamespace())
	if err != nil {
		return result, err
	}

	rsm, err := newRSMachine(inst, r.Client, logger, record.NewEventRecorderAdapter(r.EventRecorder), privilegedMoverOk)

	// Using only external method
	if errors.Is(err, mover.ErrNoMoverFound) && inst.Spec.External != nil {
		return ctrl.Result{}, nil
	}
	// Both internal and external methods defined
	if rsm != nil && inst.Spec.External != nil {
		err = mover.ErrMultipleMoversFound
		apimeta.SetStatusCondition(&inst.Status.Conditions, metav1.Condition{
			Type:    volsyncv1alpha1.ConditionSynchronizing,
			Status:  metav1.ConditionFalse,
			Reason:  volsyncv1alpha1.SynchronizingReasonError,
			Message: err.Error(),
		})
	}
	// No method found
	if rsm == nil && inst.Spec.External == nil {
		err = mover.ErrNoMoverFound
		apimeta.SetStatusCondition(&inst.Status.Conditions, metav1.Condition{
			Type:    volsyncv1alpha1.ConditionSynchronizing,
			Status:  metav1.ConditionFalse,
			Reason:  volsyncv1alpha1.SynchronizingReasonError,
			Message: err.Error(),
		})
	}

	// All good, so run the state machine
	if err == nil {
		result, err = sm.Run(ctx, rsm, logger)
	}

	// Update instance status
	statusErr := r.Client.Status().Update(ctx, inst)
	if err == nil { // Don't mask previous error
		err = statusErr
	}
	return result, err
}

func (r *ReplicationSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&volsyncv1alpha1.ReplicationSource{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 100,
		}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&snapv1.VolumeSnapshot{}).
		Owns(&vgsnapv1alpha1.VolumeGroupSnapshot{}). //TODO: will need to only enable this on systems with support
		Complete(r)
}

func newRSMachine(rs *volsyncv1alpha1.ReplicationSource, c client.Client,
	l logr.Logger, er events.EventRecorder, privilegedMoverOk bool) (*rsMachine, error) {
	dataMover, err := mover.GetSourceMoverFromCatalog(c, l, er, rs, privilegedMoverOk)
	if err != nil {
		return nil, err
	}

	metrics := newVolSyncMetrics(prometheus.Labels{
		"obj_name":      rs.Name,
		"obj_namespace": rs.Namespace,
		"role":          "source",
		"method":        dataMover.Name(),
	})

	return &rsMachine{
		rs:      rs,
		client:  c,
		logger:  l,
		metrics: metrics,
		mover:   dataMover,
	}, nil
}

func (m *rsMachine) Cronspec() string {
	if m.rs.Spec.Trigger != nil && m.rs.Spec.Trigger.Schedule != nil {
		return *m.rs.Spec.Trigger.Schedule
	}
	return ""
}

func (m *rsMachine) ManualTag() string {
	if m.rs.Spec.Trigger != nil {
		return m.rs.Spec.Trigger.Manual
	}
	return ""
}

func (m *rsMachine) LastManualTag() string {
	return m.rs.Status.LastManualSync
}

func (m *rsMachine) SetLastManualTag(tag string) {
	m.rs.Status.LastManualSync = tag
}

func (m *rsMachine) NextSyncTime() *metav1.Time {
	return m.rs.Status.NextSyncTime
}

func (m *rsMachine) SetNextSyncTime(next *metav1.Time) {
	m.rs.Status.NextSyncTime = next
}

func (m *rsMachine) LastSyncStartTime() *metav1.Time {
	return m.rs.Status.LastSyncStartTime
}

func (m *rsMachine) SetLastSyncStartTime(last *metav1.Time) {
	m.rs.Status.LastSyncStartTime = last
}

func (m *rsMachine) LastSyncTime() *metav1.Time {
	return m.rs.Status.LastSyncTime
}

func (m *rsMachine) SetLastSyncTime(last *metav1.Time) {
	m.rs.Status.LastSyncTime = last
}

func (m *rsMachine) LastSyncDuration() *metav1.Duration {
	return m.rs.Status.LastSyncDuration
}

func (m *rsMachine) SetLastSyncDuration(duration *metav1.Duration) {
	m.rs.Status.LastSyncDuration = duration
}

func (m *rsMachine) Conditions() *[]metav1.Condition {
	return &m.rs.Status.Conditions
}

func (m *rsMachine) SetOutOfSync(isOutOfSync bool) {
	if isOutOfSync {
		m.metrics.OutOfSync.Set(1)
	} else {
		m.metrics.OutOfSync.Set(0)
	}
}

func (m *rsMachine) IncMissedIntervals() {
	m.metrics.MissedIntervals.Inc()
}

func (m *rsMachine) ObserveSyncDuration(duration time.Duration) {
	m.metrics.SyncDurations.Observe(duration.Seconds())
}

func (m *rsMachine) Synchronize(ctx context.Context) (mover.Result, error) {
	return m.mover.Synchronize(ctx)
}

func (m *rsMachine) Cleanup(ctx context.Context) (mover.Result, error) {
	return m.mover.Cleanup(ctx)
}
