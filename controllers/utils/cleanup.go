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

package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	vgsnapv1alpha1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MarkForCleanup marks the provided "obj" to be deleted at the end of the
// synchronization iteration.
func MarkForCleanup(owner metav1.Object, obj metav1.Object) bool {
	uid := owner.GetUID()
	return AddLabel(obj, cleanupLabelKey, string(uid))
}

// UnmarkForCleanup removes any previously applied cleanup label
func UnmarkForCleanup(obj metav1.Object) bool {
	return RemoveLabel(obj, cleanupLabelKey)
}

// CleanupObjects deletes all objects that have been marked. The objects to be
// cleaned up must have been previously marked via MarkForCleanup() and
// associated with "owner". The "types" array should contain one object of each
// type to clean up.
func CleanupObjects(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object, types []client.Object,
) error {
	uid := owner.GetUID()
	l := logger.WithValues("owned-by", uid)
	options := []client.DeleteAllOfOption{
		client.MatchingLabels{cleanupLabelKey: string(uid)},
		client.InNamespace(owner.GetNamespace()),
		client.PropagationPolicy(metav1.DeletePropagationBackground),
	}
	l.Info("deleting temporary objects")
	for _, obj := range types {
		_, isSnap := obj.(*snapv1.VolumeSnapshot)
		_, isVGSnap := obj.(*vgsnapv1alpha1.VolumeGroupSnapshot)
		if isSnap {
			// Handle volumesnapshots differently - do not delete if they have a specific label
			err := cleanupSnapshots(ctx, c, logger, owner)
			if err != nil {
				l.Error(err, "unable to delete volume snapshot(s)")
				return err
			}
		} else if isVGSnap {
			// Handle volumegroupsnapshots - do not delete if they have a specific label
			err := cleanupVGSnapshots(ctx, c, logger, owner)
			if err != nil {
				l.Error(err, "unable to delete volumegroup snapshot(s)")
				return err
			}
		} else {
			err := c.DeleteAllOf(ctx, obj, options...)
			if client.IgnoreNotFound(err) != nil {
				l.Error(err, "unable to delete object(s)")
				return err
			}
		}
	}
	return nil
}

// Could be generalized to other types if we want to use unstructuredList - would need to pass in group, version, kind
func cleanupSnapshots(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object,
) error {
	// Load current list of snapshots with the cleanup label
	listOptions := []client.ListOption{
		client.MatchingLabels{cleanupLabelKey: string(owner.GetUID())},
		client.InNamespace(owner.GetNamespace()),
	}
	snapList := &snapv1.VolumeSnapshotList{}
	err := c.List(ctx, snapList, listOptions...)
	if err != nil {
		return err
	}

	// Make into generic slice of client.Object
	snapObjList := []client.Object{}
	for i := range snapList.Items {
		snap := snapList.Items[i]
		snapObjList = append(snapObjList, &snap)
	}

	return CleanupSnapshotsOrVGSnapshotsWithLabelCheck(ctx, c, logger, owner, snapObjList)
}

func cleanupVGSnapshots(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object,
) error {
	// Load current list of volumegroupsnapshots with the cleanup label
	listOptions := []client.ListOption{
		client.MatchingLabels{cleanupLabelKey: string(owner.GetUID())},
		client.InNamespace(owner.GetNamespace()),
	}
	vgsnapList := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
	err := c.List(ctx, vgsnapList, listOptions...)
	if err != nil {
		return err
	}

	// Make into generic slice of client.Object
	vgsnapObjList := []client.Object{}
	for i := range vgsnapList.Items {
		vgsnap := vgsnapList.Items[i]
		vgsnapObjList = append(vgsnapObjList, &vgsnap)
	}

	return CleanupSnapshotsOrVGSnapshotsWithLabelCheck(ctx, c, logger, owner, vgsnapObjList)
}

func CleanupSnapshotsOrVGSnapshotsWithLabelCheck(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object, snapAndVGSnapList []client.Object,
) error {
	// If marked as do-not-delete, remove the cleanup label and ownership
	snapsForCleanup, err := relinquishSnapshotsOrVGSnapshotsWithDoNotDeleteLabel(ctx, c, logger, owner, snapAndVGSnapList)
	if err != nil {
		return err
	}

	// Remaining snapshots/vgsnapshots should be cleaned up
	for i := range snapsForCleanup {
		snapForCleanup := snapsForCleanup[i]

		if snapInUseByOther(snapForCleanup, owner) {
			// If the snapshot has any other owner reference or used by vol populator pvc
			// while provisioning, then do not delete just remove our own owner reference
			updated := RemoveOwnerReference(snapForCleanup, owner)
			if updated {
				err := c.Update(ctx, snapForCleanup)
				if err != nil {
					logger.Error(err, "error removing ownerRef from snapshot or vgsnapshot",
						"name", snapForCleanup.GetName(), "namespace", snapForCleanup.GetNamespace())
					return err
				}
			}
		} else {
			// Use a delete precondition to avoid timing issues.
			// If the object was modified (for example by someone adding a new label) in-between us loading it and
			// performing the delete, the should throw an error as the resourceVersion will not match
			snapResourceVersion := snapForCleanup.GetResourceVersion()
			err := c.Delete(ctx, snapForCleanup, client.Preconditions{ResourceVersion: &snapResourceVersion})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Relinquish ownership of any volumesnapshots or volumegroupsnapshots that have the do-not-delete label
func RelinquishOwnedSnapshotsAndVGSnapshotsWithDoNotDeleteLabel(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object,
) error {
	// Find all snapshots in the namespace with the do not delete label
	ls, err := labels.Parse(DoNotDeleteLabelKey)
	if err != nil {
		return err
	}

	listOptions := []client.ListOption{
		client.MatchingLabelsSelector{
			Selector: ls,
		},
		client.InNamespace(owner.GetNamespace()),
	}

	// Find list of snapshots by label selector
	snapList := &snapv1.VolumeSnapshotList{}
	err = c.List(ctx, snapList, listOptions...)
	if err != nil {
		return err
	}

	// Find list of vgsnapshots by label selector
	vgsnapList := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
	err = c.List(ctx, vgsnapList, listOptions...)
	if err != nil {
		return err
	}

	// Add snaps and vgsnaps to a list
	snapAndVGSnapObjList := []client.Object{}
	for i := range snapList.Items {
		snap := snapList.Items[i]
		snapAndVGSnapObjList = append(snapAndVGSnapObjList, &snap)
	}
	for i := range vgsnapList.Items {
		vgsnap := vgsnapList.Items[i]
		snapAndVGSnapObjList = append(snapAndVGSnapObjList, &vgsnap)
	}

	_, err = relinquishSnapshotsOrVGSnapshotsWithDoNotDeleteLabel(ctx, c, logger, owner, snapAndVGSnapObjList)
	return err
}

// Returns a list of remaining VolumeSnapshots/VolumeGroupSnapshots that were not relinquished
func relinquishSnapshotsOrVGSnapshotsWithDoNotDeleteLabel(ctx context.Context, c client.Client,
	logger logr.Logger, owner client.Object, snapList []client.Object,
) ([]client.Object, error) {
	remainingSnapshots := []client.Object{}

	var snapRelinquishErr error

	for i := range snapList {
		snapshot := snapList[i]

		ownershipRemoved, err := RemoveSnapOrVGSnapOwnershipAndLabelsIfRequestedAndUpdate(ctx, c, logger,
			owner, snapshot)
		if err != nil {
			snapRelinquishErr = err // Will return the latest error at the end but keep processing the snaps
			continue
		}
		if !ownershipRemoved {
			remainingSnapshots = append(remainingSnapshots, snapshot)
		}
	}

	return remainingSnapshots, snapRelinquishErr
}

// For snapshots or volumegroupsnapshots
func RemoveSnapOrVGSnapOwnershipAndLabelsIfRequestedAndUpdate(ctx context.Context, c client.Client, logger logr.Logger,
	owner client.Object, snapshot client.Object,
) (bool, error) {
	ownershipRemoved := false

	if IsMarkedDoNotDelete(snapshot) {
		logger.Info(
			"Not deleting volumesnapshot/volumegroupsnapshot protected with label - will remove ownership and cleanup label",
			"name", snapshot.GetName(), "kind", snapshot.GetObjectKind(), "label", DoNotDeleteLabelKey)
		ownershipRemoved = true

		updated := UnMarkForCleanupAndRemoveOwnership(snapshot, owner)
		if updated {
			err := c.Update(ctx, snapshot)
			if err != nil {
				logger.Error(err, "error removing cleanup label or ownerRef from snapshot",
					"name", snapshot.GetName(), "namespace", snapshot.GetNamespace())
				return false, err
			}
		}
	}

	return ownershipRemoved, nil
}

// Check VolumeSnapshot or VolumeGroupSnapshot for DoNotDelete label
func IsMarkedDoNotDelete(snapshot metav1.Object) bool {
	return HasLabel(snapshot, DoNotDeleteLabelKey)
}

// Mark VolumeSnapshot or VolumeGroupSnapshot with DoNotDelete label
func MarkDoNotDelete(snapshot metav1.Object) bool {
	return AddLabel(snapshot, DoNotDeleteLabelKey, "true")
}

func UnMarkForCleanupAndRemoveOwnership(obj metav1.Object, owner client.Object) bool {
	updated := false

	// Remove volsync cleanup label & ownership label if present
	updated = UnmarkForCleanup(obj) || updated
	updated = RemoveOwnedByVolSync(obj) || updated

	// Remove ReplicationDestination owner reference if present
	return RemoveOwnerReference(obj, owner) || updated
}

func RemoveOwnerReference(obj metav1.Object, owner client.Object) bool {
	updated := false
	updatedOwnerRefs := []metav1.OwnerReference{}
	for _, ownerRef := range obj.GetOwnerReferences() {
		if ownerRef.UID == owner.GetUID() {
			// Do not add to updatedOwnerRefs
			updated = true
		} else {
			updatedOwnerRefs = append(updatedOwnerRefs, ownerRef)
		}
	}
	if updated {
		obj.SetOwnerReferences(updatedOwnerRefs)
	}

	return updated
}

// Checks if snapshot or volumegroupsnapshot is owned by someone else or is being used by the volume populator
func snapInUseByOther(snapshot client.Object, owner client.Object) bool {
	return hasOtherOwnerRef(snapshot, owner) || snapInUseByVolumePopulatorPVC(snapshot)
}

// Can be called for  snapshot or volumegroupsnapshot - but volumepopulatorlabelprefix only will apply to snapshots
func snapInUseByVolumePopulatorPVC(snapshot client.Object) bool {
	// Volume Populator will put on a label with a specific prefix on a snapshot while
	// it's populating the PVC from that snapshot - this indicates at least one pvc for
	// the volume populator is actively using this snapshot
	for _, labelKey := range snapshot.GetLabels() {
		if strings.HasPrefix(labelKey, SnapInUseByVolumePopulatorLabelPrefix) {
			return true
		}
	}
	return false
}

func hasOtherOwnerRef(obj metav1.Object, owner client.Object) bool {
	for _, ownerRef := range obj.GetOwnerReferences() {
		if ownerRef.UID != owner.GetUID() {
			return true
		}
	}
	return false
}

//nolint:funlen
func MarkOldSnapshotOrGroupSnapshotForCleanup(ctx context.Context, c client.Client, logger logr.Logger,
	owner metav1.Object, oldImage, latestImage *corev1.TypedLocalObjectReference,
) error {
	// Make sure we only delete an old snapshot (it's a snapshot, but not the
	// current one)

	// There's no latestImage or type != snapshot or volumegroupsnapshot
	if !IsSnapshot(latestImage) && !IsGroupSnapshot(latestImage) {
		return nil
	}
	// No oldImage or type != snapshot or volumegroupsnapshot
	if !IsSnapshot(oldImage) && !IsGroupSnapshot(oldImage) {
		return nil
	}

	// Also don't clean it up if it's the snap we're trying to preserve
	if latestImage.Name == oldImage.Name {
		return nil
	}

	var oldSnap client.Object
	oldSnapKind := oldImage.Kind

	if IsSnapshot(oldImage) {
		oldSnap = &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      oldImage.Name,
				Namespace: owner.GetNamespace(),
			},
		}
	} else {
		// VolumeGroupSnapshot
		oldSnap = &vgsnapv1alpha1.VolumeGroupSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      oldImage.Name,
				Namespace: owner.GetNamespace(),
			},
		}
	}

	err := c.Get(ctx, client.ObjectKeyFromObject(oldSnap), oldSnap)
	if kerrors.IsNotFound(err) {
		// Nothing to cleanup
		return nil
	}
	if err != nil {
		logger.Error(err, fmt.Sprintf("unable to get %s", oldSnapKind),
			"name", oldSnap.GetName(), "namespace", oldSnap.GetNamespace())
		return err
	}

	if IsMarkedDoNotDelete(oldSnap) {
		logger.Info(fmt.Sprintf("%s is marked do-not-delete, will not mark for cleanup", oldSnapKind))
		return nil
	}

	// Update the old snapshot/groupsnapshot with the cleanup label
	MarkForCleanup(owner, oldSnap)
	err = c.Update(ctx, oldSnap)
	if err != nil && !kerrors.IsNotFound(err) {
		logger.Error(err, fmt.Sprintf("unable to update %s with cleanup label", oldSnapKind),
			"name", oldSnap.GetName(), "namespace", oldSnap.GetNamespace())
		return err
	}
	return nil
}

func IsSnapshot(image *corev1.TypedLocalObjectReference) bool {
	if image == nil {
		return false
	}
	if image.Kind != "VolumeSnapshot" || image.APIGroup == nil || *image.APIGroup != snapv1.SchemeGroupVersion.Group {
		return false
	}
	return true
}

func IsGroupSnapshot(image *corev1.TypedLocalObjectReference) bool {
	if image == nil {
		return false
	}
	if image.Kind != "VolumeGroupSnapshot" ||
		image.APIGroup == nil || *image.APIGroup != vgsnapv1alpha1.SchemeGroupVersion.Group {
		return false
	}
	return true
}
