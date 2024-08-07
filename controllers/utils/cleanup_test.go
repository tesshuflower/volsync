/*
Copyright 2022 The VolSync authors.

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

package utils_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	vgsnapv1alpha1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/utils"
)

var _ = Describe("Cleanup", func() {
	logger := zap.New(zap.UseDevMode(true), zap.WriteTo(GinkgoWriter))

	var testNamespace *corev1.Namespace

	var rdA *volsyncv1alpha1.ReplicationDestination
	var rdB *volsyncv1alpha1.ReplicationDestination

	var snapA1 *snapv1.VolumeSnapshot
	var snapA2 *snapv1.VolumeSnapshot
	var snapB1 *snapv1.VolumeSnapshot

	var vgsnapA1 *vgsnapv1alpha1.VolumeGroupSnapshot
	var vgsnapA2 *vgsnapv1alpha1.VolumeGroupSnapshot
	var vgsnapB1 *vgsnapv1alpha1.VolumeGroupSnapshot

	var pvcA1 *corev1.PersistentVolumeClaim
	var pvcA2 *corev1.PersistentVolumeClaim

	BeforeEach(func() {
		// Create namespace for test
		testNamespace = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "ns-cleantests-",
			},
		}
		Expect(k8sClient.Create(ctx, testNamespace)).To(Succeed())
		Expect(testNamespace.Name).NotTo(BeEmpty())

		//
		// Create some replication destinations
		//
		rdA = &volsyncv1alpha1.ReplicationDestination{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rd-a-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: volsyncv1alpha1.ReplicationDestinationSpec{
				External: &volsyncv1alpha1.ReplicationDestinationExternalSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, rdA)).To(Succeed())

		rdB = &volsyncv1alpha1.ReplicationDestination{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rd-b-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: volsyncv1alpha1.ReplicationDestinationSpec{
				External: &volsyncv1alpha1.ReplicationDestinationExternalSpec{},
			},
		}
		Expect(k8sClient.Create(ctx, rdB)).To(Succeed())

		//
		// Create some volume snapshots owned by the ReplicationDestinations
		//
		snapA1 = &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "snap-a-1-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: snapv1.VolumeSnapshotSpec{
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: ptr.To("dummy"),
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, snapA1, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, snapA1)).To(Succeed())

		snapA2 = &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "snap-a-2-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: snapv1.VolumeSnapshotSpec{
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: ptr.To("dummy"),
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, snapA2, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, snapA2)).To(Succeed())

		snapB1 = &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "snap-b-1-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: snapv1.VolumeSnapshotSpec{
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: ptr.To("dummy"),
				},
			},
		}
		// Make this owned by rdB
		Expect(ctrl.SetControllerReference(rdB, snapB1, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, snapB1)).To(Succeed())

		//
		// Create some volumegroup snapshots owned by the ReplicationDestinations
		//
		vgsnapA1 = &vgsnapv1alpha1.VolumeGroupSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vgsnap-a-1-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: vgsnapv1alpha1.VolumeGroupSnapshotSpec{
				Source: vgsnapv1alpha1.VolumeGroupSnapshotSource{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"mypvclabel": "doesnotexist",
						},
					},
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, vgsnapA1, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, vgsnapA1)).To(Succeed())

		vgsnapA2 = &vgsnapv1alpha1.VolumeGroupSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vgsnap-a-2-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: vgsnapv1alpha1.VolumeGroupSnapshotSpec{
				Source: vgsnapv1alpha1.VolumeGroupSnapshotSource{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"mypvclabel-a2": "alsodoesnotexist",
						},
					},
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, vgsnapA2, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, vgsnapA2)).To(Succeed())

		vgsnapB1 = &vgsnapv1alpha1.VolumeGroupSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vgsnap-b-1-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: vgsnapv1alpha1.VolumeGroupSnapshotSpec{
				Source: vgsnapv1alpha1.VolumeGroupSnapshotSource{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"mypvclabel-b1": "alsodoesnotexist",
						},
					},
				},
			},
		}
		// Make this owned by rdB
		Expect(ctrl.SetControllerReference(rdB, vgsnapB1, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, vgsnapB1)).To(Succeed())

		// Create some PVCs as well
		capacity := resource.MustParse("1Gi")
		pvcA1 = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "pvc-a-1-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: capacity,
					},
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, pvcA1, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pvcA1)).To(Succeed())

		pvcA2 = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "pvc-a-2-",
				Namespace:    testNamespace.GetName(),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: capacity,
					},
				},
			},
		}
		// Make this owned by rdA
		Expect(ctrl.SetControllerReference(rdA, pvcA2, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pvcA2)).To(Succeed())
	})

	Describe("Relinquish Owned Snapshots and VolumeGroup Snapshots", func() {
		var allSnapsBefore *snapv1.VolumeSnapshotList
		var allVGSnapsBefore *vgsnapv1alpha1.VolumeGroupSnapshotList

		JustBeforeEach(func() {
			// Load all the snapshots in the namespace
			allSnapsBefore = &snapv1.VolumeSnapshotList{}
			Expect(k8sClient.List(ctx, allSnapsBefore, client.InNamespace(testNamespace.GetName()))).To(Succeed())

			// Load all the volumegroup snapshots in the namespace
			allVGSnapsBefore = &vgsnapv1alpha1.VolumeGroupSnapshotList{}
			Expect(k8sClient.List(ctx, allVGSnapsBefore, client.InNamespace(testNamespace.GetName()))).To(Succeed())

			Expect(utils.RelinquishOwnedSnapshotsAndVGSnapshotsWithDoNotDeleteLabel(ctx, k8sClient, logger, rdA)).To(Succeed())
		})

		Context("When no snapshots or volumegroupsnapshots have the do-not-delete label", func() {
			It("Should not modify any snapshots", func() {
				allSnapsAfter := &snapv1.VolumeSnapshotList{}
				Expect(k8sClient.List(ctx, allSnapsAfter, client.InNamespace(testNamespace.GetName()))).To(Succeed())

				for _, snapAfter := range allSnapsAfter.Items {
					for _, snapBefore := range allSnapsBefore.Items {
						if snapAfter.GetName() == snapBefore.GetName() {
							// Should not have been modified
							Expect(snapAfter.GetResourceVersion()).To(Equal(snapBefore.GetResourceVersion()))
						}
					}
				}
			})

			It("Should not modify any volumegroupsnapshots", func() {
				allVGSnapsAfter := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
				Expect(k8sClient.List(ctx, allVGSnapsAfter, client.InNamespace(testNamespace.GetName()))).To(Succeed())

				for _, vgsnapAfter := range allVGSnapsAfter.Items {
					for _, vgsnapBefore := range allVGSnapsBefore.Items {
						if vgsnapAfter.GetName() == vgsnapBefore.GetName() {
							// Should not have been modified
							Expect(vgsnapAfter.GetResourceVersion()).To(Equal(vgsnapBefore.GetResourceVersion()))
						}
					}
				}
			})
		})

		Context("When some snapshots have the do-not-delete label with different values", func() {
			BeforeEach(func() {
				snapA1.Labels = map[string]string{
					utils.DoNotDeleteLabelKey: "false", // any value should be ok
				}
				Expect(k8sClient.Update(ctx, snapA1)).To(Succeed())

				snapA2.Labels = map[string]string{
					utils.DoNotDeleteLabelKey: "donotdeleteme", // any value should be ok
				}
				Expect(k8sClient.Update(ctx, snapA2)).To(Succeed())

				// Also put volsync cleanup label on A2
				utils.MarkForCleanup(rdA, snapA2)
				Expect(k8sClient.Update(ctx, snapA2)).To(Succeed())
			})

			It("Should modify snapshots with the do-not-delete label and remove ownership", func() {
				// Re-load A1 and A2
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapA1), snapA1)).To(Succeed())
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapA2), snapA2)).To(Succeed())

				// A1 and A2 should have owner ref removed and volsync cleanup label if present
				Expect(validateCleanupLabelAndOwnerRefRemoved(snapA1)).To(Succeed())
				Expect(validateCleanupLabelAndOwnerRefRemoved(snapA2)).To(Succeed())

				for i := range allSnapsBefore.Items {
					snapBefore := allSnapsBefore.Items[i]
					// Everything except A1 and A2 should not have been touched
					if snapBefore.GetName() != snapA1.GetName() && snapBefore.GetName() != snapA2.GetName() {
						// re-load the obj and check - it should not have been modified
						snapAfter := snapv1.VolumeSnapshot{}
						Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&snapBefore), &snapAfter)).To(Succeed())
						Expect(snapAfter.GetResourceVersion()).To(Equal(snapBefore.GetResourceVersion()))
					}
				}
			})

			It("Should not modify any volumegroupsnapshots", func() {
				allVGSnapsAfter := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
				Expect(k8sClient.List(ctx, allVGSnapsAfter, client.InNamespace(testNamespace.GetName()))).To(Succeed())

				for _, vgsnapAfter := range allVGSnapsAfter.Items {
					for _, vgsnapBefore := range allVGSnapsBefore.Items {
						if vgsnapAfter.GetName() == vgsnapBefore.GetName() {
							// Should not have been modified
							Expect(vgsnapAfter.GetResourceVersion()).To(Equal(vgsnapBefore.GetResourceVersion()))
						}
					}
				}
			})
		})

		Context("When some snapshots and volumegroupsnapshots have the do-not-delete label with different values", func() {
			BeforeEach(func() {
				//
				// snapA2 will have do-not-delete
				//
				snapA2.Labels = map[string]string{
					utils.DoNotDeleteLabelKey: "false", // any value should be ok
				}
				Expect(k8sClient.Update(ctx, snapA2)).To(Succeed())

				//
				// vgsnapA1 and vgsnapA2 will have do-not-delete
				//
				vgsnapA1.Labels = map[string]string{
					utils.DoNotDeleteLabelKey: "false", // any value should be ok
				}
				Expect(k8sClient.Update(ctx, vgsnapA1)).To(Succeed())

				vgsnapA2.Labels = map[string]string{
					utils.DoNotDeleteLabelKey: "donotdeleteme", // any value should be ok
				}
				Expect(k8sClient.Update(ctx, vgsnapA2)).To(Succeed())

				// Also put volsync cleanup label on A2
				utils.MarkForCleanup(rdA, vgsnapA2)
				Expect(k8sClient.Update(ctx, vgsnapA2)).To(Succeed())
			})

			It("Should modify snapshots with the do-not-delete label and remove ownership", func() {
				// Re-load A2 snapshot
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapA2), snapA2)).To(Succeed())

				// A2 should have owner ref removed and volsync cleanup label if present
				Expect(validateCleanupLabelAndOwnerRefRemoved(snapA2)).To(Succeed())

				// The rest of the snapshots should not have been modified
				for i := range allSnapsBefore.Items {
					snapBefore := allSnapsBefore.Items[i]
					if snapBefore.GetName() != snapA2.GetName() {
						// re-load the obj and check - it should not have been modified
						snapAfter := snapv1.VolumeSnapshot{}
						Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&snapBefore), &snapAfter)).To(Succeed())
						Expect(snapAfter.GetResourceVersion()).To(Equal(snapBefore.GetResourceVersion()))
					}
				}
			})

			It("Should modify volumegroupsnapshots with the do-not-delete label and remove ownership", func() {
				// Re-load A1 and A2 volumegroup snapshots
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapA1), vgsnapA1)).To(Succeed())
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapA2), vgsnapA2)).To(Succeed())

				// vgsnap A1 and A2 should have owner ref removed and volsync cleanup label if present
				Expect(validateCleanupLabelAndOwnerRefRemoved(vgsnapA1)).To(Succeed())
				Expect(validateCleanupLabelAndOwnerRefRemoved(vgsnapA2)).To(Succeed())

				// The rest of the vgsnapshots should not have been modified
				for i := range allVGSnapsBefore.Items {
					vgsnapBefore := allVGSnapsBefore.Items[i]
					if vgsnapBefore.GetName() != vgsnapA1.GetName() && vgsnapBefore.GetName() != vgsnapA2.GetName() {
						// re-load the obj and check - it should not have been modified
						vgsnapAfter := vgsnapv1alpha1.VolumeGroupSnapshot{}
						Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&vgsnapBefore), &vgsnapAfter)).To(Succeed())
						Expect(vgsnapAfter.GetResourceVersion()).To(Equal(vgsnapBefore.GetResourceVersion()))
					}
				}
			})
		})
	})

	Describe("Cleanup Objects", func() {
		Context("When some objects have the cleanup label", func() {
			cleanupTypes := []client.Object{
				&corev1.PersistentVolumeClaim{},
				&snapv1.VolumeSnapshot{},
				&vgsnapv1alpha1.VolumeGroupSnapshot{},
			}

			BeforeEach(func() {
				// Mark snaps A1 and B1 for cleanup
				utils.MarkForCleanup(rdA, snapA1)
				Expect(k8sClient.Update(ctx, snapA1)).To(Succeed())
				utils.MarkForCleanup(rdB, snapB1)
				Expect(k8sClient.Update(ctx, snapB1)).To(Succeed())

				// Mark volumegroupsnapshots A1 and B1 for cleanup
				utils.MarkForCleanup(rdA, vgsnapA1)
				Expect(k8sClient.Update(ctx, vgsnapA1)).To(Succeed())
				utils.MarkForCleanup(rdB, vgsnapB1)
				Expect(k8sClient.Update(ctx, vgsnapB1)).To(Succeed())

				// Mark pvc A1 for cleanup
				utils.MarkForCleanup(rdA, pvcA1)
				Expect(k8sClient.Update(ctx, pvcA1)).To(Succeed())
			})

			It("Should cleanup only the objects matching the cleanup label owner", func() {
				Expect(utils.CleanupObjects(ctx, k8sClient, logger, rdA, cleanupTypes)).To(Succeed())

				remainingSnapList := &snapv1.VolumeSnapshotList{}
				Expect(k8sClient.List(ctx, remainingSnapList,
					client.InNamespace(testNamespace.GetName()))).To(Succeed())
				Expect(len(remainingSnapList.Items)).To(Equal(2))

				// snapA2 should remain, no cleanup label
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapA2), snapA1)).To(Succeed())
				// snapB1 should remain, different owner
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapB1), snapB1)).To(Succeed())

				// vgsnapA2 should remain, no cleanup label
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapA2), vgsnapA1)).To(Succeed())
				// vgsnapB1 should remain, different owner
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapB1), vgsnapB1)).To(Succeed())

				// Note pvcs have finalizer automatically so will not actually delete - but should be marked for deletion
				// pvcA1 should be marked for deletion
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pvcA1), pvcA1)
				if err == nil {
					Expect(pvcA1.DeletionTimestamp.IsZero()).To(BeFalse())
				} else {
					Expect(kerrors.IsNotFound(err)).To(BeTrue())
				}
				// pvcA2 should remain, no cleanup label
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvcA2), pvcA2)).To(Succeed())
				Expect(pvcA2.DeletionTimestamp.IsZero()).To(BeTrue())
			})

			Context("When a snapshot has the do-not-delete label and cleanup label", func() {
				BeforeEach(func() {
					// Mark B1 with do-not-delete
					snapB1.Labels[utils.DoNotDeleteLabelKey] = "true" // value should not matter
					Expect(k8sClient.Update(ctx, snapB1)).To(Succeed())
				})

				It("Should not cleanup the snapshot(s) marked with do-not-delete", func() {
					Expect(utils.CleanupObjects(ctx, k8sClient, logger, rdB, cleanupTypes)).To(Succeed())

					remainingSnapList := &snapv1.VolumeSnapshotList{}
					Expect(k8sClient.List(ctx, remainingSnapList,
						client.InNamespace(testNamespace.GetName()))).To(Succeed())
					Expect(len(remainingSnapList.Items)).To(Equal(3)) // Nothing should be deleted

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapB1), snapB1)).To(Succeed())
					Expect(validateCleanupLabelAndOwnerRefRemoved(snapB1)).To(Succeed())
				})
			})

			Context("When a volumegroupsnapshot has the do-not-delete label and cleanup label", func() {
				BeforeEach(func() {
					// Mark B1 with do-not-delete
					vgsnapB1.Labels[utils.DoNotDeleteLabelKey] = "true" // value should not matter
					Expect(k8sClient.Update(ctx, vgsnapB1)).To(Succeed())
				})

				It("Should not cleanup the volumegroupsnapshot(s) marked with do-not-delete", func() {
					Expect(utils.CleanupObjects(ctx, k8sClient, logger, rdB, cleanupTypes)).To(Succeed())

					remainingVGSnapList := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
					Expect(k8sClient.List(ctx, remainingVGSnapList,
						client.InNamespace(testNamespace.GetName()))).To(Succeed())
					Expect(len(remainingVGSnapList.Items)).To(Equal(3)) // Nothing should be deleted

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapB1), vgsnapB1)).To(Succeed())
					Expect(validateCleanupLabelAndOwnerRefRemoved(vgsnapB1)).To(Succeed())
				})
			})
		})
	})

	Describe("Delete with preconditions", func() {
		// Want to test preconditions here - when cleaning up snapshots we use a precondition with the
		// resourceVersion to ensure the snapshot has not been modified prior to us attempting to delete it.
		// Call CleanupSnapshotsWithLabelCheck() directly here so we can test the precondition
		Context("When cleaning up snapshots that have been modified since they were read", func() {
			var snapsForCleanup *snapv1.VolumeSnapshotList
			var vgsnapsForCleanup *vgsnapv1alpha1.VolumeGroupSnapshotList
			var listOptions []client.ListOption
			var err error

			BeforeEach(func() {
				// Mark snaps A1 and A2 for cleanup
				utils.MarkForCleanup(rdA, snapA1)
				Expect(k8sClient.Update(ctx, snapA1)).To(Succeed())
				utils.MarkForCleanup(rdA, snapA2)
				Expect(k8sClient.Update(ctx, snapA2)).To(Succeed())

				// Mark vgsnaps A1 and A2 for cleanup
				utils.MarkForCleanup(rdA, vgsnapA1)
				Expect(k8sClient.Update(ctx, vgsnapA1)).To(Succeed())
				utils.MarkForCleanup(rdA, vgsnapA2)
				Expect(k8sClient.Update(ctx, vgsnapA2)).To(Succeed())

				// Query by our volsync cleanup label
				listOptions = []client.ListOption{
					client.MatchingLabels{"volsync.backube/cleanup": string(rdA.GetUID())},
					client.InNamespace(rdA.GetNamespace()),
				}

				// Load our list of snapshots
				snapsForCleanup = &snapv1.VolumeSnapshotList{}
				Expect(k8sClient.List(ctx, snapsForCleanup, listOptions...)).To(Succeed())

				// Now modify one of the snapshots before calling the cleanup func
				snapA2.Labels["test-label"] = "modified"
				Expect(k8sClient.Update(ctx, snapA2)).To(Succeed())

				// Load our list of volumegroupsnapshots
				vgsnapsForCleanup = &vgsnapv1alpha1.VolumeGroupSnapshotList{}
				Expect(k8sClient.List(ctx, vgsnapsForCleanup, listOptions...)).To(Succeed())

				snapObjsForCleanup := []client.Object{}
				for i := range snapsForCleanup.Items {
					snapObjsForCleanup = append(snapObjsForCleanup, &snapsForCleanup.Items[i])
				}
				for i := range vgsnapsForCleanup.Items {
					snapObjsForCleanup = append(snapObjsForCleanup, &vgsnapsForCleanup.Items[i])
				}

				err = utils.CleanupSnapshotsOrVGSnapshotsWithLabelCheck(ctx, k8sClient, logger, rdA, snapObjsForCleanup)
			})

			It("Should not delete snapshots that have been modified", func() {
				Expect(err).To(HaveOccurred()) // Should get an error, snapA2 was modified
				Expect(kerrors.IsConflict(err)).To(BeTrue())
			})

			Context("When re-running cleanup (on next reconcile)", func() {
				It("Should cleanup successfully after reloading the objects", func() {
					// Re-load the list of snaps
					Expect(k8sClient.List(ctx, snapsForCleanup, listOptions...)).To(Succeed())
					Expect(k8sClient.List(ctx, vgsnapsForCleanup, listOptions...)).To(Succeed())

					// make slice of client.Object
					updatedSnapSlice := []client.Object{}
					for i := range snapsForCleanup.Items {
						updatedSnapSlice = append(updatedSnapSlice, &snapsForCleanup.Items[i])
					}
					for i := range vgsnapsForCleanup.Items {
						updatedSnapSlice = append(updatedSnapSlice, &vgsnapsForCleanup.Items[i])
					}

					// Now the func should succeed
					err = utils.CleanupSnapshotsOrVGSnapshotsWithLabelCheck(ctx, k8sClient, logger, rdA, updatedSnapSlice)
					Expect(err).NotTo(HaveOccurred())

					// Confirm all snaphots except snapB1 are gone
					remainingSnapList := &snapv1.VolumeSnapshotList{}
					Expect(k8sClient.List(ctx, remainingSnapList,
						client.InNamespace(testNamespace.GetName()))).To(Succeed())
					Expect(len(remainingSnapList.Items)).To(Equal(1)) // only snapB1 should be left

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(snapB1), snapB1)).To(Succeed())

					// Confirm all volumegroupsnaphots except vgsnapB1 are gone
					remainingVGSnapList := &vgsnapv1alpha1.VolumeGroupSnapshotList{}
					Expect(k8sClient.List(ctx, remainingVGSnapList,
						client.InNamespace(testNamespace.GetName()))).To(Succeed())
					Expect(len(remainingVGSnapList.Items)).To(Equal(1)) // only vgsnapB1 should be left

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vgsnapB1), vgsnapB1)).To(Succeed())
				})
			})
		})
	})
})

// This assumes there was only 1 owner ref at the start
func validateCleanupLabelAndOwnerRefRemoved(obj client.Object) error {
	// Check for cleanup label, expecting it to be gone
	labels := obj.GetLabels()
	_, ok := labels["volsync.backube/cleanup"]
	if ok {
		return fmt.Errorf("Label volsync.backube/cleanup is still present but should be removed")
	}

	// Owner ref should be removed as well
	if len(obj.GetOwnerReferences()) > 0 {
		return fmt.Errorf("Owner reference is still present, expecting them to have been removed")
	}

	return nil
}
