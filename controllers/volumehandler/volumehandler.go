/*
Copyright 2021 The VolSync authors.

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

package volumehandler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	vgsnapv1alpha1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volsyncerrors "github.com/backube/volsync/controllers/errors"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
)

const (
	// Annotation used to track the name of the snapshot being created
	snapshotAnnotation = "volsync.backube/snapname"
	// Annotation used to track the name of the volume group snapshot being created
	groupSnapshotAnnotation = "volsync.backube/groupsnapname"
	// Time format for snapshot names and labels
	timeYYYYMMDDHHMMSS = "20060102150405"
)

type VolumeHandler struct {
	client                       client.Client
	eventRecorder                events.EventRecorder
	owner                        client.Object
	copyMethod                   volsyncv1alpha1.CopyMethodType
	capacity                     *resource.Quantity
	storageClassName             *string
	accessModes                  []corev1.PersistentVolumeAccessMode
	volumeSnapshotClassName      *string
	volumeGroupSnapshotClassName *string
}

/*
func (vh VolumeHandler) EnsurePVCsFromSrc(ctx context.Context, log logr.Logger,
  srcSelector *metav1.LabelSelector, isTemporary bool) (*corev1.Persi))
*/

func (vh *VolumeHandler) EnsureGroupPVCsFromSrc(ctx context.Context, log logr.Logger,
	vgPVCLabelSelector *metav1.LabelSelector, groupName string, isTemporary bool,
) (map[string]*corev1.PersistentVolumeClaim, error) {
	// TODO: do we care to check if the copyMethod is snapshot?  It will need to be unless we support
	// passing in a list of PVCs or an existing volumegroupsnapshot?

	groupSnap, err := vh.ensureGroupSnapshot(ctx, log, vgPVCLabelSelector, groupName, isTemporary)
	if groupSnap == nil || err != nil {
		return nil, err
	}

	// Go through the groupSnap and create temp PVCS from all the individual snapshots
	return vh.pvcsFromGroupSnapshot(ctx, log, groupSnap, isTemporary)
}

// EnsurePVCFromSrc ensures the presence of a PVC that is based on the provided
// src PVC. It is generated based on the VolumeHandler's configuration. It may
// be the same PVC as src. Note: it's possible to return nil, nil. In this case,
// the operation should be retried.
func (vh *VolumeHandler) EnsurePVCFromSrc(ctx context.Context, log logr.Logger,
	src *corev1.PersistentVolumeClaim, name string, isTemporary bool,
) (*corev1.PersistentVolumeClaim, error) {
	switch vh.copyMethod {
	case volsyncv1alpha1.CopyMethodNone:
		fallthrough // Same as CopyMethodDirect
	case volsyncv1alpha1.CopyMethodDirect:
		return src, nil
	case volsyncv1alpha1.CopyMethodClone:
		return vh.ensureClone(ctx, log, src, name, isTemporary)
	case volsyncv1alpha1.CopyMethodSnapshot:
		snap, err := vh.ensureSnapshot(ctx, log, src, name, isTemporary)
		if snap == nil || err != nil {
			return nil, err
		}
		return vh.pvcFromSnapshot(ctx, log, snap, src, name, isTemporary)
	default:
		return nil, fmt.Errorf("unsupported copyMethod: %v -- must be Direct, None, Clone, or Snapshot", vh.copyMethod)
	}
}

//nolint:funlen
func (vh *VolumeHandler) EnsureImages(ctx context.Context, log logr.Logger,
	srcPVCs map[string]*corev1.PersistentVolumeClaim,
	srcPVCSelector *metav1.LabelSelector,
) (*corev1.TypedLocalObjectReference, error) {
	// For non-volumegroup cases, src is just the first(only) pvc
	var src *corev1.PersistentVolumeClaim
	for _, srcPVC := range srcPVCs {
		src = srcPVC
		break
	}

	switch vh.copyMethod { //nolint: exhaustive
	case volsyncv1alpha1.CopyMethodNone:
		fallthrough // Same as CopyMethodDirect
	case volsyncv1alpha1.CopyMethodDirect:
		return &corev1.TypedLocalObjectReference{
			APIGroup: &corev1.SchemeGroupVersion.Group,
			Kind:     src.Kind,
			Name:     src.Name,
		}, nil
	case volsyncv1alpha1.CopyMethodSnapshot:
		// FIXME: volume group handling is here - which means it'll only work if CopyMethod=Snapshot
		var snapObj client.Object
		var snapObjKind string
		var snapObjAPIGroup *string

		if srcPVCSelector != nil {
			// If the selector is not nil, it's assumed we want a volume group snapshot
			vgSnap, err := vh.ensureImageGroupSnapshot(ctx, log, srcPVCs, srcPVCSelector)
			if vgSnap == nil || err != nil {
				return nil, err
			}
			snapObjKind = vgSnap.Kind
			snapObj = vgSnap
			snapObjAPIGroup = &vgsnapv1alpha1.SchemeGroupVersion.Group
		} else {
			snap, err := vh.ensureImageSnapshot(ctx, log, src)
			if snap == nil || err != nil {
				return nil, err
			}
			snapObjKind = snap.Kind
			snapObj = snap
			snapObjAPIGroup = &snapv1.SchemeGroupVersion.Group
		}

		if snapObjKind == "" {
			// In case kind is not filled out, although it should be when read from k8sclient cache
			// Unit tests that use a direct API client may not have kind - but we can get it from the scheme
			gvks, _, _ := vh.client.Scheme().ObjectKinds(snapObj)
			for _, gvk := range gvks {
				if gvk.Kind != "" {
					snapObjKind = gvk.Kind
					break
				}
			}
		}

		return &corev1.TypedLocalObjectReference{
			APIGroup: snapObjAPIGroup,
			Kind:     snapObjKind,
			Name:     snapObj.GetName(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported copyMethod: %v -- must be Direct, None, or Snapshot", vh.copyMethod)
	}
}

// EnsureImage ensures the presence of a representation of the provided src
// PVC. It is generated based on the VolumeHandler's configuration and could be
// of type PersistentVolumeClaim or VolumeSnapshot. It may even be the same PVC
// as src.
// TODO: remove - replace with ensureImages() above
func (vh *VolumeHandler) EnsureImage(ctx context.Context, log logr.Logger,
	src *corev1.PersistentVolumeClaim,
) (*corev1.TypedLocalObjectReference, error) {
	switch vh.copyMethod { //nolint: exhaustive
	case volsyncv1alpha1.CopyMethodNone:
		fallthrough // Same as CopyMethodDirect
	case volsyncv1alpha1.CopyMethodDirect:
		return &corev1.TypedLocalObjectReference{
			APIGroup: &corev1.SchemeGroupVersion.Group,
			Kind:     src.Kind,
			Name:     src.Name,
		}, nil
	case volsyncv1alpha1.CopyMethodSnapshot:
		snap, err := vh.ensureImageSnapshot(ctx, log, src)
		if snap == nil || err != nil {
			return nil, err
		}

		snapKind := snap.Kind
		if snapKind == "" {
			// In case kind is not filled out, although it should be when read from k8sclient cache
			// Unit tests that use a direct API client may not have kind - but we can get it from the scheme
			gvks, _, _ := vh.client.Scheme().ObjectKinds(snap)
			for _, gvk := range gvks {
				if gvk.Kind != "" {
					snapKind = gvk.Kind
					break
				}
			}
		}

		return &corev1.TypedLocalObjectReference{
			APIGroup: &snapv1.SchemeGroupVersion.Group,
			Kind:     snapKind,
			Name:     snap.Name,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported copyMethod: %v -- must be Direct, None, or Snapshot", vh.copyMethod)
	}
}

func (vh *VolumeHandler) UseProvidedPVC(ctx context.Context, pvcName string) (*corev1.PersistentVolumeClaim, error) {
	return vh.getPVCByName(ctx, pvcName)
}

func (vh *VolumeHandler) getPVCByName(ctx context.Context, pvcName string) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	err := vh.client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)
	return pvc, err
}

func (vh *VolumeHandler) EnsureNewPVC(ctx context.Context, log logr.Logger, name string,
) (*corev1.PersistentVolumeClaim, error) {
	return vh.EnsureNewPVCWithLabels(ctx, log, name, nil, nil, nil)
}

// Allows to specify additional labels to add to the PVC to be created
// Set accessModes/capacity if this individual pvc may have different values from the volumehandler settings. (i.e.
// in a group snapshot scenario each pvc may have different values).
// if accessModes is not set, vh.accessModes will be used
// if capacity is not set, vh.capacity will be used
// nolint: funlen
func (vh *VolumeHandler) EnsureNewPVCWithLabels(ctx context.Context, log logr.Logger,
	name string, pvcLabels map[string]string, accessModes []corev1.PersistentVolumeAccessMode, capacity *resource.Quantity,
) (*corev1.PersistentVolumeClaim, error) {
	logger := log.WithValues("PVC", name)

	// Ensure required configuration parameters have been provided in order to
	// create volume
	if len(accessModes) == 0 {
		// Use global access modes from the volumehandler
		accessModes = vh.accessModes
	}
	if len(accessModes) == 0 {
		err := errors.New("accessModes must be provided when destinationPVC is not")
		logger.Error(err, "error allocating new PVC")
		return nil, err
	}

	if capacity == nil {
		// Use global capacity from the volumehandler
		capacity = vh.capacity
	}
	if capacity == nil {
		err := errors.New("capacity must be provided when destinationPVC is not")
		logger.Error(err, "error allocating new PVC")
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vh.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, pvc, func() error {
		if err := ctrl.SetControllerReference(vh.owner, pvc, vh.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.AddAllLabels(pvc, pvcLabels)
		utils.SetOwnedByVolSync(pvc)
		if pvc.CreationTimestamp.IsZero() { // set immutable fields
			pvc.Spec.AccessModes = accessModes
			pvc.Spec.StorageClassName = vh.storageClassName
			volumeMode := corev1.PersistentVolumeFilesystem
			pvc.Spec.VolumeMode = &volumeMode
		}

		pvc.Spec.Resources.Requests = corev1.ResourceList{
			corev1.ResourceStorage: *capacity,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	logger.V(1).Info("PVC reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, pvc, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRPVCCreated, volsyncv1alpha1.EvACreatePVC,
			"created %s to receive incoming data",
			utils.KindAndName(vh.client.Scheme(), pvc))
	}
	if pvc.Status.Phase != corev1.ClaimBound &&
		!pvc.CreationTimestamp.IsZero() &&
		pvc.CreationTimestamp.Add(mover.PVCBindTimeout).Before(time.Now()) {
		vh.eventRecorder.Eventf(vh.owner, pvc, corev1.EventTypeWarning,
			volsyncv1alpha1.EvRPVCNotBound, "",
			"waiting for %s to bind; check StorageClass name and CSI driver capabilities",
			utils.KindAndName(vh.client.Scheme(), pvc))
	}

	return pvc, nil
}

func (vh *VolumeHandler) SetAccessModes(accessModes []corev1.PersistentVolumeAccessMode) {
	vh.accessModes = accessModes
}

func (vh *VolumeHandler) GetAccessModes() []corev1.PersistentVolumeAccessMode {
	return vh.accessModes
}

//nolint:funlen
func (vh *VolumeHandler) ensureImageGroupSnapshot(ctx context.Context, log logr.Logger,
	pvcs map[string]*corev1.PersistentVolumeClaim,
	pvcLabelSelector *metav1.LabelSelector,
) (*vgsnapv1alpha1.VolumeGroupSnapshot, error) {
	// create & record name (if necessary)
	groupSnapName := ""
	for _, pvc := range pvcs {
		// See if we already have a vg snap name annotated on one of the PVCs
		if name, ok := pvc.Annotations[groupSnapshotAnnotation]; ok {
			log.Info("### VGSNAP using existing annotation ###", "existing name", name) // FIXME: remove
			groupSnapName = name
			break
		}
	}
	if groupSnapName == "" {
		log.Info("### VGSNAP Creating new VGSNAP name") // FIXME: remove
		// no annotation set yet on any of the pvcs
		ts := time.Now().Format(timeYYYYMMDDHHMMSS)
		groupSnapName = vh.owner.GetName() + "-" + ts
	}
	// Make sure all the pvcs in the group have the annotation
	for _, pvc := range pvcs {
		log.Info("### VGSNAP - going to annotate pvcs ###", "vgSnapName", groupSnapName) // FIXME: remove
		if name, ok := pvc.Annotations[groupSnapshotAnnotation]; !ok || name != groupSnapName {
			pvc.Annotations[groupSnapshotAnnotation] = groupSnapName
			if err := vh.client.Update(ctx, pvc); err != nil {
				log.Error(err, "unable to annotate PVC")
				return nil, err
			}
		}
	}

	// ensure the object
	logger := log.WithValues("volumegroupsnapshot", groupSnapName)

	groupSnap := &vgsnapv1alpha1.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      groupSnapName,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, groupSnap, func() error {
		if utils.IsMarkedDoNotDelete(groupSnap) {
			// Remove adding ownership and potentially marking for cleanup if do-not-delete label is present
			utils.UnMarkForCleanupAndRemoveOwnership(groupSnap, vh.owner)
		} else {
			if err := ctrl.SetControllerReference(vh.owner, groupSnap, vh.client.Scheme()); err != nil {
				logger.Error(err, utils.ErrUnableToSetControllerRef)
				return err
			}
			utils.SetOwnedByVolSync(groupSnap)
		}
		if groupSnap.CreationTimestamp.IsZero() {
			groupSnap.Spec = vgsnapv1alpha1.VolumeGroupSnapshotSpec{
				Source: vgsnapv1alpha1.VolumeGroupSnapshotSource{
					Selector: pvcLabelSelector,
				},
				VolumeGroupSnapshotClassName: vh.volumeGroupSnapshotClassName,
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	logger.V(1).Info("VolumeGroupSnapshot reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, groupSnap, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSnapCreated, volsyncv1alpha1.EvACreateVGSnap, "created %s from labelSelector %v",
			utils.KindAndName(vh.client.Scheme(), groupSnap), pvcLabelSelector)
	}

	// We only continue reconciling if the snapshot has been bound & not deleted
	if !groupSnap.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	if groupSnap.Status == nil || groupSnap.Status.BoundVolumeGroupSnapshotContentName == nil {
		if groupSnap.CreationTimestamp.Add(mover.SnapshotBindTimeout).Before(time.Now()) {
			vh.eventRecorder.Eventf(vh.owner, groupSnap, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRVGSnapNotBound, volsyncv1alpha1.EvANone,
				"waiting for %s to bind; check VolumeGroupSnapshotClass name and ensure CSI driver supports volume group snapshots",
				utils.KindAndName(vh.client.Scheme(), groupSnap))
		}
		return nil, nil
	}

	return groupSnap, nil
}

// nolint: funlen
func (vh *VolumeHandler) ensureImageSnapshot(ctx context.Context, log logr.Logger,
	src *corev1.PersistentVolumeClaim,
) (*snapv1.VolumeSnapshot, error) {
	// create & record name (if necessary)
	if src.Annotations == nil {
		src.Annotations = make(map[string]string)
	}
	if _, ok := src.Annotations[snapshotAnnotation]; !ok {
		ts := time.Now().Format(timeYYYYMMDDHHMMSS)
		src.Annotations[snapshotAnnotation] = src.Name + "-" + ts
		if err := vh.client.Update(ctx, src); err != nil {
			log.Error(err, "unable to annotate PVC")
			return nil, err
		}
	}
	snapName := src.Annotations[snapshotAnnotation]

	// ensure the object
	logger := log.WithValues("snapshot", snapName)

	snap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapName,
			Namespace: src.Namespace,
		},
	}
	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, snap, func() error {
		if utils.IsMarkedDoNotDelete(snap) {
			// Remove adding ownership and potentially marking for cleanup if do-not-delete label is present
			utils.UnMarkForCleanupAndRemoveOwnership(snap, vh.owner)
		} else {
			if err := ctrl.SetControllerReference(vh.owner, snap, vh.client.Scheme()); err != nil {
				logger.Error(err, utils.ErrUnableToSetControllerRef)
				return err
			}
			utils.SetOwnedByVolSync(snap)
		}
		if snap.CreationTimestamp.IsZero() {
			snap.Spec = snapv1.VolumeSnapshotSpec{
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: &src.Name,
				},
				VolumeSnapshotClassName: vh.volumeSnapshotClassName,
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	logger.V(1).Info("Snapshot reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, snap, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSnapCreated, volsyncv1alpha1.EvACreateSnap, "created %s from %s",
			utils.KindAndName(vh.client.Scheme(), snap), utils.KindAndName(vh.client.Scheme(), src))
	}

	// We only continue reconciling if the snapshot has been bound & not deleted
	if !snap.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	if snap.Status == nil || snap.Status.BoundVolumeSnapshotContentName == nil {
		if snap.CreationTimestamp.Add(mover.SnapshotBindTimeout).Before(time.Now()) {
			vh.eventRecorder.Eventf(vh.owner, snap, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRSnapNotBound, volsyncv1alpha1.EvANone,
				"waiting for %s to bind; check VolumeSnapshotClass name and ensure CSI driver supports volume snapshots",
				utils.KindAndName(vh.client.Scheme(), snap))
		}
		return nil, nil
	}

	return snap, nil
}

func (vh *VolumeHandler) RemoveSnapshotAnnotationFromPVC(ctx context.Context, log logr.Logger, pvcName string) error {
	pvc, err := vh.getPVCByName(ctx, pvcName)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil // PVC no longer exists, nothing to do
		}
		return err
	}

	delete(pvc.Annotations, snapshotAnnotation)
	if err := vh.client.Update(ctx, pvc); err != nil {
		log.Error(err, "unable to remove snapshot annotation from PVC", "pvc", pvc)
		return err
	}
	return nil
}

func (vh *VolumeHandler) RemoveGroupSnapshotAnnotationFromPVC(ctx context.Context, log logr.Logger,
	pvcName string,
) error {
	pvc, err := vh.getPVCByName(ctx, pvcName)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil // PVC no longer exists, nothing to do
		}
		return err
	}

	delete(pvc.Annotations, groupSnapshotAnnotation)
	if err := vh.client.Update(ctx, pvc); err != nil {
		log.Error(err, "unable to remove snapshot annotation from PVC", "pvc", pvc)
		return err
	}
	return nil
}

// nolint: funlen
func (vh *VolumeHandler) ensureClone(ctx context.Context, log logr.Logger,
	src *corev1.PersistentVolumeClaim, name string, isTemporary bool,
) (*corev1.PersistentVolumeClaim, error) {
	clone := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	logger := log.WithValues("clone", client.ObjectKeyFromObject(clone))

	// See if the clone exists
	err := vh.client.Get(ctx, client.ObjectKeyFromObject(clone), clone)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}

		// Need to create the clone - see if we need to wait first for a copy-trigger
		wait, err := vh.waitForCopyTriggerBeforeCloneOrSnap(ctx, log, src)
		if wait || err != nil {
			return nil, err
		}
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, clone, func() error {
		if err := ctrl.SetControllerReference(vh.owner, clone, vh.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(clone)
		if isTemporary {
			utils.MarkForCleanup(vh.owner, clone)
		}
		if clone.CreationTimestamp.IsZero() {
			if vh.capacity != nil {
				clone.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *vh.capacity,
				}
			} else if src.Status.Capacity != nil && src.Status.Capacity.Storage() != nil {
				// check the src PVC capacity if set
				clone.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *src.Status.Capacity.Storage(),
				}
			} else {
				// Fallback to the pvc requested size
				clone.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *src.Spec.Resources.Requests.Storage(),
				}
			}
			if vh.storageClassName != nil {
				clone.Spec.StorageClassName = vh.storageClassName
			} else {
				clone.Spec.StorageClassName = src.Spec.StorageClassName
			}
			if vh.accessModes != nil {
				clone.Spec.AccessModes = vh.accessModes
			} else {
				clone.Spec.AccessModes = src.Spec.AccessModes
			}
			volumeMode := corev1.PersistentVolumeFilesystem
			if src.Spec.VolumeMode != nil {
				volumeMode = *src.Spec.VolumeMode
			}
			clone.Spec.VolumeMode = &volumeMode
			clone.Spec.DataSource = &corev1.TypedLocalObjectReference{
				APIGroup: nil,
				Kind:     "PersistentVolumeClaim",
				Name:     src.Name,
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	if !clone.DeletionTimestamp.IsZero() {
		logger.V(1).Info("PVC is being deleted-- need to wait")
		return nil, nil
	}
	logger.V(1).Info("clone reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, clone, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRPVCCreated, volsyncv1alpha1.EvACreatePVC,
			"created %s as a clone of %s",
			utils.KindAndName(vh.client.Scheme(), clone), utils.KindAndName(vh.client.Scheme(), src))
	}
	if !clone.CreationTimestamp.IsZero() &&
		clone.CreationTimestamp.Add(mover.PVCBindTimeout).Before(time.Now()) &&
		clone.Status.Phase != corev1.ClaimBound {
		vh.eventRecorder.Eventf(vh.owner, clone, corev1.EventTypeWarning,
			volsyncv1alpha1.EvRPVCNotBound, "",
			"waiting for %s to bind; check StorageClass name and ensure CSI driver supports volume cloning",
			utils.KindAndName(vh.client.Scheme(), clone))
	}

	if clone.Status.Phase == corev1.ClaimBound {
		// Clone is ready as it's gone into ClaimBound - update copy trigger if necessary
		err = vh.updateCopyTriggerAfterCloneOrSnap(ctx, src)
		if err != nil {
			return clone, err
		}
	}

	return clone, err
}

//nolint:funlen
func (vh *VolumeHandler) ensureGroupSnapshot(ctx context.Context, log logr.Logger,
	vgPVCLabelSelector *metav1.LabelSelector, groupName string, isTemporary bool,
) (*vgsnapv1alpha1.VolumeGroupSnapshot, error) {
	groupSnap := &vgsnapv1alpha1.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      groupName,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	logger := log.WithValues("groupsnapshot", client.ObjectKeyFromObject(groupSnap))

	// See if the vgsnapshot exists
	err := vh.client.Get(ctx, client.ObjectKeyFromObject(groupSnap), groupSnap)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}

		// Need to create the vgsnapshot - see if we need to wait first for a copy-trigger
		wait, err := vh.waitForCopyTriggerBeforeVGSnap(ctx, log, vgPVCLabelSelector)
		if wait || err != nil {
			return nil, err
		}
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, groupSnap, func() error {
		if err := ctrl.SetControllerReference(vh.owner, groupSnap, vh.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(groupSnap)
		if isTemporary {
			utils.MarkForCleanup(vh.owner, groupSnap)
		}
		if groupSnap.CreationTimestamp.IsZero() {
			groupSnap.Spec = vgsnapv1alpha1.VolumeGroupSnapshotSpec{
				Source: vgsnapv1alpha1.VolumeGroupSnapshotSource{
					Selector: vgPVCLabelSelector,
				},
				VolumeGroupSnapshotClassName: vh.volumeGroupSnapshotClassName,
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	if !groupSnap.DeletionTimestamp.IsZero() {
		logger.V(1).Info("groupsnapshot is being deleted-- need to wait")
		return nil, nil
	}
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, groupSnap, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSnapCreated, volsyncv1alpha1.EvACreateVGSnap,
			"created %s from labelSelector %s",
			utils.KindAndName(vh.client.Scheme(), groupSnap), vgPVCLabelSelector)
	}
	if groupSnap.Status == nil || groupSnap.Status.BoundVolumeGroupSnapshotContentName == nil {
		logger.V(1).Info("waiting for snapshot to be bound")
		if groupSnap.CreationTimestamp.Add(mover.SnapshotBindTimeout).Before(time.Now()) {
			vh.eventRecorder.Eventf(vh.owner, groupSnap, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRVGSnapNotBound, volsyncv1alpha1.EvANone,
				"waiting for %s to bind; check VolumeGroupSnapshotClass name and ensure CSI driver supports volume snapshots",
				utils.KindAndName(vh.client.Scheme(), groupSnap))
		}
		return nil, nil
	}
	if groupSnap.Status.ReadyToUse != nil && !*groupSnap.Status.ReadyToUse {
		// readyToUse is set to false for this volume group snapshot
		logger.V(1).Info("waiting for group snapshot to be ready")
		return nil, nil
	}
	// status.readyToUse either is not set by the driver at this point (even though
	// status.BoundVolumeGroupSnapshotContentName is set), or readyToUse=true

	// vgsnapshot is ready - update copy trigger on PVC(s) in group if necessary
	err = vh.updateCopyTriggerAfterVGSnap(ctx, groupSnap)
	if err != nil {
		return groupSnap, err
	}

	logger.V(1).Info("temporary group snapshot reconciled", "operation", op)
	return groupSnap, nil
}

func (vh *VolumeHandler) pvcsFromGroupSnapshot(ctx context.Context, log logr.Logger,
	groupSnap *vgsnapv1alpha1.VolumeGroupSnapshot, isTemporary bool,
) (map[string]*corev1.PersistentVolumeClaim, error) {
	if groupSnap.Status == nil || groupSnap.Status.BoundVolumeGroupSnapshotContentName == nil {
		// Shouldn't happen, we check for this already in ensureGroupSnapshot()
		return nil, nil
	}

	// Match each volumesnapshot in the group snapshot to the original PVC and then create a pvc from snapshot
	pvcsFromSnapMap := map[string]*corev1.PersistentVolumeClaim{}
	for _, pvcToSnapPair := range groupSnap.Status.PVCVolumeSnapshotRefList {
		sourcePVCName := pvcToSnapPair.PersistentVolumeClaimRef.Name
		sourcePVCSnapName := pvcToSnapPair.VolumeSnapshotRef.Name

		if sourcePVCName == "" || sourcePVCSnapName == "" {
			// Refs not correct in the status, shouldn't happen
			continue
		}

		sourcePVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sourcePVCName,
				Namespace: vh.owner.GetNamespace(),
			},
		}
		err := vh.client.Get(ctx, client.ObjectKeyFromObject(sourcePVC), sourcePVC)
		if err != nil {
			return nil, err
		}

		sourcePVCSnap := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sourcePVCSnapName,
				Namespace: vh.owner.GetNamespace(),
			},
		}
		err = vh.client.Get(ctx, client.ObjectKeyFromObject(sourcePVCSnap), sourcePVCSnap)
		if err != nil {
			return nil, err
		}

		// Now we have the snapshot and the original src pvc it snapshotted
		// Next, create a new temp pvc from snap to get our point in time copy of the source pvc
		tempPVCName := mover.VolSyncPrefix + vh.owner.GetName() + "-" + sourcePVC.GetName()

		tempPVC, err := vh.pvcFromSnapshot(ctx, log, sourcePVCSnap, sourcePVC, tempPVCName, isTemporary)
		if tempPVC == nil || err != nil {
			return nil, err
		}

		pvcsFromSnapMap[sourcePVC.GetName() /*original PVC*/] = tempPVC
	}

	if len(pvcsFromSnapMap) == 0 {
		log.Info("VolumeGroupSnapshot has no valid PVCVolumeSnapshotRefs, unable to create PVCs from group snapshot",
			"volume group snap name", groupSnap.GetName(), "namespace", groupSnap.GetNamespace())
		return nil, nil
	}

	return pvcsFromSnapMap, nil
}

// nolint: funlen
func (vh *VolumeHandler) ensureSnapshot(ctx context.Context, log logr.Logger,
	src *corev1.PersistentVolumeClaim, name string, isTemporary bool,
) (*snapv1.VolumeSnapshot, error) {
	snap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	logger := log.WithValues("snapshot", client.ObjectKeyFromObject(snap))

	// See if the snapshot exists
	err := vh.client.Get(ctx, client.ObjectKeyFromObject(snap), snap)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}

		// Need to create the snapshot - see if we need to wait first for a copy-trigger
		wait, err := vh.waitForCopyTriggerBeforeCloneOrSnap(ctx, log, src)
		if wait || err != nil {
			return nil, err
		}
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, snap, func() error {
		if err := ctrl.SetControllerReference(vh.owner, snap, vh.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(snap)
		if isTemporary {
			utils.MarkForCleanup(vh.owner, snap)
		}
		if snap.CreationTimestamp.IsZero() {
			snap.Spec.Source.PersistentVolumeClaimName = &src.Name
			snap.Spec.VolumeSnapshotClassName = vh.volumeSnapshotClassName
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	if !snap.DeletionTimestamp.IsZero() {
		logger.V(1).Info("snap is being deleted-- need to wait")
		return nil, nil
	}
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, snap, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSnapCreated, volsyncv1alpha1.EvACreateSnap,
			"created %s from %s",
			utils.KindAndName(vh.client.Scheme(), snap), utils.KindAndName(vh.client.Scheme(), src))
	}
	if snap.Status == nil || snap.Status.BoundVolumeSnapshotContentName == nil {
		logger.V(1).Info("waiting for snapshot to be bound")
		if snap.CreationTimestamp.Add(mover.SnapshotBindTimeout).Before(time.Now()) {
			vh.eventRecorder.Eventf(vh.owner, snap, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRSnapNotBound, volsyncv1alpha1.EvANone,
				"waiting for %s to bind; check VolumeSnapshotClass name and ensure CSI driver supports volume snapshots",
				utils.KindAndName(vh.client.Scheme(), snap))
		}
		return nil, nil
	}
	if snap.Status.ReadyToUse != nil && !*snap.Status.ReadyToUse {
		// readyToUse is set to false for this volume snapshot
		logger.V(1).Info("waiting for snapshot to be ready")
		return nil, nil
	}
	// status.readyToUse either is not set by the driver at this point (even though
	// status.BoundVolumeSnapshotContentName is set), or readyToUse=true

	// Snapshot is ready - update copy trigger if necessary
	err = vh.updateCopyTriggerAfterCloneOrSnap(ctx, src)
	if err != nil {
		return snap, err
	}

	logger.V(1).Info("temporary snapshot reconciled", "operation", op)
	return snap, nil
}

// Checks if copy trigger annotations are on any srcPVC for a volumegroup selector and updates accordingly
// This should be called when volumegroupsnapshot does not exist in order to determine
// if we should wait before creation.
//
// If any PVC in the volumegroup (found via the volumesnapshot group selector) has a copy trigger,
// go through the normal copy trigger flow. This way a user could just put/interact with the copy trigger
// annotations on a single PVC in their group.
func (vh *VolumeHandler) waitForCopyTriggerBeforeVGSnap(ctx context.Context, log logr.Logger,
	vgPVCLabelSelector *metav1.LabelSelector,
) (bool, error) {
	// FIXME: make sure covered by unit tests
	pvcSelector, err := metav1.LabelSelectorAsSelector(vgPVCLabelSelector)
	if err != nil {
		return false, err
	}
	listOptions := []client.ListOption{
		client.MatchingLabelsSelector{Selector: pvcSelector},
		client.InNamespace(vh.owner.GetNamespace()),
	}

	// This is the list of PVCs (currently) that would be grouped in a VGSnapshot using the vgPVCLabelSelector
	groupPVCList := &corev1.PersistentVolumeClaimList{}
	err = vh.client.List(ctx, groupPVCList, listOptions...)
	if err != nil {
		return false, err
	}

	waitForGroup := false
	for i := range groupPVCList.Items {
		pvc := groupPVCList.Items[i]
		waitForPVC, err := vh.waitForCopyTriggerBeforeCloneOrSnap(ctx, log, &pvc)
		if err != nil {
			return false, nil
		}
		// If any pvc needs to be waited for due to a copy trigger, we need to wait for the whole group
		// But keep processing the list so we update all copy triggers where necessary on all pvcs in the group
		waitForGroup = waitForGroup || waitForPVC
	}

	return waitForGroup, nil
}

// Checks if copy trigger annotations are on the srcPVC and updates accordingly
// This should be called when clone/snapshot does not exist in order to determine
// if we should wait before creation.
//
//nolint:funlen
func (vh *VolumeHandler) waitForCopyTriggerBeforeCloneOrSnap(ctx context.Context, log logr.Logger,
	srcPVC *corev1.PersistentVolumeClaim,
) (bool, error) {
	if !utils.PVCUsesCopyTrigger(srcPVC) {
		// No need to wait
		return false, nil
	}

	logger := log.WithValues("useCopyTrigger", "true")
	// Look for user supplied copy trigger
	copyTrigger := utils.GetCopyTriggerValue(srcPVC)
	latestCopyTrigger := utils.GetLatestCopyTriggerValue(srcPVC)
	if copyTrigger == "" || copyTrigger == latestCopyTrigger {
		// Need to block until the user updates the copyTrigger
		wait := true

		updated := utils.SetLatestCopyStatusWaitingForTrigger(srcPVC)
		if updated {
			err := vh.client.Update(ctx, srcPVC)
			if err != nil {
				return wait, err
			}
			vh.eventRecorder.Eventf(vh.owner, srcPVC, corev1.EventTypeNormal,
				volsyncv1alpha1.EvRSrcPVCWaitingForCopyTrigger, volsyncv1alpha1.EvACreateSrcCopyUsingCopyTrigger,
				"Waiting on copy trigger on src PVC %s before creating snapshot or clone",
				utils.KindAndName(vh.client.Scheme(), srcPVC))
		}

		waitingSinceTimeStr := utils.GetLatestCopyTriggerWaitingSinceValue(srcPVC)
		waitingSinceTime, err := time.Parse(time.RFC3339, waitingSinceTimeStr)
		if err != nil {
			logger.Error(err, "Waiting since pvc Annotation is not parseable, resetting ...")
			utils.SetLatestCopyTriggerWaitingSinceValueNow(srcPVC)
			err := vh.client.Update(ctx, srcPVC)
			if err != nil {
				return wait, err
			}
		}
		if waitingSinceTime.Add(volsyncv1alpha1.CopyTriggerWaitTimeout).Before(time.Now()) {
			logger.Info(
				fmt.Sprintf("Warning, still waiting on copy-trigger after %s", volsyncv1alpha1.CopyTriggerWaitTimeout.String()))
			vh.eventRecorder.Eventf(vh.owner, srcPVC, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRSrcPVCTimeoutWaitingForCopyTrigger, volsyncv1alpha1.EvACreateSrcCopyUsingCopyTrigger,
				"waiting on copy trigger on src PVC %s before creating snapshot or clone; check PVC annotations",
				utils.KindAndName(vh.client.Scheme(), srcPVC))
			return wait, &volsyncerrors.CopyTriggerTimeoutError{
				SourcePVC: srcPVC.GetName(),
			}
		}

		logger.Info("Waiting for copy-trigger annotation to be updated before creating a snapshot")
		return wait, nil
	}

	// Copy trigger has been modified, we can proceed with the snapshot
	updated := utils.SetLatestCopyStatusInProgress(srcPVC)
	if updated {
		err := vh.client.Update(ctx, srcPVC)
		if err != nil {
			return false, err
		}
		vh.eventRecorder.Eventf(vh.owner, srcPVC, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSrcPVCCopyTriggerReceived, volsyncv1alpha1.EvACreateSrcCopyUsingCopyTrigger,
			"Received updated copy trigger on src PVC %s, proceeding to create snapshot or clone",
			utils.KindAndName(vh.client.Scheme(), srcPVC))
	}

	// No need to wait, we're in progress (but snapshot/clone doesn't exist yet)
	return false, nil
}

func (vh *VolumeHandler) updateCopyTriggerAfterVGSnap(ctx context.Context, vgsnap *vgsnapv1alpha1.VolumeGroupSnapshot,
) error {
	// FIXME: unit tests

	// Go through and update each PVC that uses copy triggers - instead of using the vg pvc selector,
	// iterate over the list of PVCs that got included in the vg snapshot
	for _, pvcSnapshotPair := range vgsnap.Status.PVCVolumeSnapshotRefList {
		srcPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcSnapshotPair.PersistentVolumeClaimRef.Name,
				Namespace: vh.owner.GetNamespace(),
			},
		}
		err := vh.client.Get(ctx, client.ObjectKeyFromObject(srcPVC), srcPVC)
		if err != nil {
			return err
		}

		// Update copy triggers for this individual pvc in the group if necessary
		err = vh.updateCopyTriggerAfterCloneOrSnap(ctx, srcPVC)
		if err != nil {
			return err
		}
	}
	return nil
}

func (vh *VolumeHandler) updateCopyTriggerAfterCloneOrSnap(ctx context.Context,
	srcPVC *corev1.PersistentVolumeClaim,
) error {
	if !utils.PVCUsesCopyTrigger(srcPVC) {
		// Nothing to update
		return nil
	}

	updated := utils.SetLatestCopyStatusCompleted(srcPVC)
	updated = utils.SetLatestCopyTriggerValue(srcPVC, utils.GetCopyTriggerValue(srcPVC)) || updated
	if updated {
		err := vh.client.Update(ctx, srcPVC)
		if err != nil {
			return err
		}
		vh.eventRecorder.Eventf(vh.owner, srcPVC, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSrcPVCCopyUsingCopyTriggerCompleted, volsyncv1alpha1.EvACreateSrcCopyUsingCopyTrigger,
			"snapshot or clone complete for src PVC %s using copy trigger",
			utils.KindAndName(vh.client.Scheme(), srcPVC))
	}
	return nil
}

// nolint: funlen
func (vh *VolumeHandler) pvcFromSnapshot(ctx context.Context, log logr.Logger,
	snap *snapv1.VolumeSnapshot, original *corev1.PersistentVolumeClaim,
	name string, isTemporary bool,
) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: vh.owner.GetNamespace(),
		},
	}
	logger := log.WithValues("pvc", client.ObjectKeyFromObject(pvc))

	op, err := ctrlutil.CreateOrUpdate(ctx, vh.client, pvc, func() error {
		if err := ctrl.SetControllerReference(vh.owner, pvc, vh.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(pvc)
		if isTemporary {
			utils.MarkForCleanup(vh.owner, pvc)
		}
		if pvc.CreationTimestamp.IsZero() {
			if vh.capacity != nil {
				pvc.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *vh.capacity,
				}
			} else if snap.Status != nil && snap.Status.RestoreSize != nil && !snap.Status.RestoreSize.IsZero() {
				pvc.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *snap.Status.RestoreSize,
				}
			} else if original.Status.Capacity != nil && original.Status.Capacity.Storage() != nil {
				// check the original PVC capacity if set
				pvc.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *original.Status.Capacity.Storage(),
				}
			} else {
				// Fallback to the pvc requested size
				pvc.Spec.Resources.Requests = corev1.ResourceList{
					corev1.ResourceStorage: *original.Spec.Resources.Requests.Storage(),
				}
			}
			if vh.storageClassName != nil {
				pvc.Spec.StorageClassName = vh.storageClassName
			} else {
				pvc.Spec.StorageClassName = original.Spec.StorageClassName
			}
			if vh.accessModes != nil {
				pvc.Spec.AccessModes = vh.accessModes
			} else {
				pvc.Spec.AccessModes = original.Spec.AccessModes
			}
			volumeMode := corev1.PersistentVolumeFilesystem
			if original.Spec.VolumeMode != nil {
				volumeMode = *original.Spec.VolumeMode
			}
			pvc.Spec.VolumeMode = &volumeMode
			pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{
				APIGroup: &snapv1.SchemeGroupVersion.Group,
				Kind:     "VolumeSnapshot",
				Name:     snap.Name,
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}
	if op == ctrlutil.OperationResultCreated {
		vh.eventRecorder.Eventf(vh.owner, pvc, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRPVCCreated, volsyncv1alpha1.EvACreatePVC, "created %s from %s",
			utils.KindAndName(vh.client.Scheme(), pvc), utils.KindAndName(vh.client.Scheme(), snap))
	}
	if pvc.Status.Phase != corev1.ClaimBound &&
		!pvc.CreationTimestamp.IsZero() &&
		pvc.CreationTimestamp.Add(mover.PVCBindTimeout).Before(time.Now()) {
		vh.eventRecorder.Eventf(vh.owner, pvc, corev1.EventTypeWarning,
			volsyncv1alpha1.EvRPVCNotBound, "",
			"waiting for %s to bind; check StorageClass name and CSI driver capabilities",
			utils.KindAndName(vh.client.Scheme(), pvc))
	}

	logger.V(1).Info("pvc from snap reconciled", "operation", op)
	return pvc, nil
}

func (vh *VolumeHandler) IsCopyMethodDirect() bool {
	return vh.copyMethod == volsyncv1alpha1.CopyMethodDirect ||
		vh.copyMethod == volsyncv1alpha1.CopyMethodNone
}
