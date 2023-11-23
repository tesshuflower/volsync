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

package rsynctls

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
)

const (
	baseMountPath    = "/data"
	devicePath       = "/dev/block"
	dataVolumeName   = "data"
	tlsContainerPort = 8000

	volSyncRsyncTLSPrefix = mover.VolSyncPrefix + "rsync-tls-"
)

// Mover is the reconciliation logic for the Rsync-based data mover.
type Mover struct {
	client                       client.Client
	logger                       logr.Logger
	eventRecorder                events.EventRecorder
	owner                        client.Object
	vh                           *volumehandler.VolumeHandler
	saHandler                    utils.SAHandler
	containerImage               string
	key                          *string
	serviceType                  *corev1.ServiceType
	serviceAnnotations           map[string]string
	address                      *string
	port                         *int32
	isSource                     bool
	paused                       bool
	mainPVCName                  *string
	mainPVCGroupSelector         *volsyncv1alpha1.SourcePVCGroup             //FIXME:
	mainPVCGroup                 []volsyncv1alpha1.DestinationPVCGroupMember //FIXME:
	privileged                   bool
	moverSecurityContext         *corev1.PodSecurityContext
	sourceStatus                 *volsyncv1alpha1.ReplicationSourceRsyncTLSStatus
	destStatus                   *volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus
	latestMoverStatus            *volsyncv1alpha1.MoverStatus
	latestVolumeGroupMoverStatus map[string]*volsyncv1alpha1.MoverStatus
}

var _ mover.Mover = &Mover{}

// All object types that are temporary/per-iteration should be listed here. The
// individual objects to be cleaned up must also be marked.
var cleanupTypes = []client.Object{
	&corev1.PersistentVolumeClaim{},
	&snapv1.VolumeSnapshot{},
	&batchv1.Job{},
}

func (m *Mover) Name() string { return "rsync-tls" }

func (m *Mover) Synchronize(ctx context.Context) (mover.Result, error) {
	var err error

	// Allocate temporary data PVC
	//var dataPVC *corev1.PersistentVolumeClaim
	var dataPVCGroup *pvcGroup
	if m.isSource {
		//dataPVC, err = m.ensureSourcePVCs(ctx)
		dataPVCGroup, err = m.ensureSourcePVCs(ctx)
	} else {
		dataPVCGroup, err = m.ensureDestinationPVCs(ctx)
	}
	if dataPVCGroup == nil || err != nil {
		return mover.InProgress(), err
	}

	pvcNamesSorted := dataPVCGroup.GetPVCNamesSorted()

	// Ensure service (if required) and publish the address in the status
	cont, namedTargetPortsMapping, err := m.ensureServiceAndPublishAddress(ctx, dataPVCGroup)
	if !cont || err != nil {
		return mover.InProgress(), err
	}

	// Ensure Secrets/keys
	rsyncPSKSecretName, err := m.ensureSecrets(ctx)
	if rsyncPSKSecretName == nil || err != nil {
		return mover.InProgress(), err
	}

	// Prepare ServiceAccount, role, rolebinding
	sa, err := m.saHandler.Reconcile(ctx, m.logger)
	if sa == nil || err != nil {
		return mover.InProgress(), err
	}

	// Ensure mover Jobs
	jobs := map[string]*batchv1.Job{}
	for i, pvcName := range pvcNamesSorted {
		jobName := volSyncRsyncTLSPrefix + m.direction() + "-" + m.owner.GetName() + "-" + strconv.Itoa(i)
		job, err := m.ensureJob(ctx, jobName, dataPVCGroup.GetPVCByName(pvcName), pvcName, /* original pvc name from the src */
			len(pvcNamesSorted) /*pvcCount*/, namedTargetPortsMapping, sa, *rsyncPSKSecretName)
		if err != nil {
			return mover.InProgress(), err
		}
		jobs[pvcName] = job
	}

	// After scheduling all the jobs, make sure they are non-nil (this means they have completed)
	for pvcName := range jobs {
		job := jobs[pvcName]
		if job == nil { // Job has not completed
			return mover.InProgress(), nil
		}
	}

	// On the destination, preserve the image and return it
	if !m.isSource {
		//FIXME: remove - Take 1 volumegroup backup once Vgroups are available
		//FIXME: May need to ensure that the label selector is properly applied to the PVCs
		var dataPVC *corev1.PersistentVolumeClaim
		for _, v := range dataPVCGroup.pvcs {
			dataPVC = v
			break
		}
		//FIXME: end fixme

		image, err := m.vh.EnsureImage(ctx, m.logger, dataPVC)
		if image == nil || err != nil {
			return mover.InProgress(), err
		}
		return mover.CompleteWithImage(image), nil
	}

	// On the source, just signal completion
	return mover.Complete(), nil
}

func (m *Mover) ensureServiceAndPublishAddress(ctx context.Context, pvcGroup *pvcGroup) (bool, string, error) {
	if m.address != nil || m.isSource {
		// Connection will be outbound. Don't need a Service
		return true, "", nil
	}

	var namedTargetPortsMapping string
	var namedTargetPorts []namedTargetPort

	if pvcGroup.isVolumeGroup {
		namedTargetPorts = []namedTargetPort{}
		for _, pvcName := range pvcGroup.GetPVCNamesSorted() {
			port := pvcGroup.GetDestinationPort(pvcName)

			namedTargetPorts = append(namedTargetPorts, namedTargetPort{
				port:           port,
				portName:       "rsync-tls-" + pvcName,
				targetPortName: pvcName,
			})

			namedTargetPortsMapping = namedTargetPortsMapping + ">" + pvcName + " " + strconv.Itoa(int(port)) + "\n"
		}
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volSyncRsyncTLSPrefix + m.direction() + "-" + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}
	svcDesc := rsyncSvcDescription{
		Context:          ctx,
		Client:           m.client,
		Service:          service,
		Owner:            m.owner,
		Type:             m.serviceType,
		Selector:         m.serviceSelector(),
		Port:             m.port,
		NamedTargetPorts: namedTargetPorts,
		Annotations:      m.serviceAnnotations,
	}
	err := svcDesc.Reconcile(m.logger)
	if err != nil {
		return false, namedTargetPortsMapping, err
	}

	a, err := m.publishSvcAddress(service)
	return a, namedTargetPortsMapping, err
}

func (m *Mover) publishSvcAddress(service *corev1.Service) (bool, error) {
	address := utils.GetServiceAddress(service)
	if address == "" {
		// We don't have an address yet, try again later
		m.updateStatusAddress(nil)
		if service.CreationTimestamp.Add(mover.ServiceAddressTimeout).Before(time.Now()) {
			m.eventRecorder.Eventf(m.owner, service, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRSvcNoAddress, volsyncv1alpha1.EvANone,
				"waiting for an address to be assigned to %s; ensure the proper serviceType was specified",
				utils.KindAndName(m.client.Scheme(), service))
		}
		return false, nil
	}
	m.updateStatusAddress(&address)

	m.logger.V(1).Info("Service addr published", "address", address)
	return true, nil
}

func (m *Mover) updateStatusAddress(address *string) {
	publishEvent := false
	if !m.isSource {
		if m.destStatus.Address == nil ||
			address != nil && *m.destStatus.Address != *address {
			publishEvent = true
		}
		m.destStatus.Address = address
	}
	if publishEvent && address != nil {
		m.eventRecorder.Eventf(m.owner, nil, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSvcAddress, volsyncv1alpha1.EvANone,
			"listening on address %s for incoming connections", *address)
	}
}

func (m *Mover) updateStatusPSK(pskSecretName *string) {
	if m.isSource {
		m.sourceStatus.KeySecret = pskSecretName
	} else {
		m.destStatus.KeySecret = pskSecretName
	}
}

// Will ensure the secret exists or create secrets if necessary
// - Returns the name of the secret that should be used in the replication job
func (m *Mover) ensureSecrets(ctx context.Context) (*string, error) {
	// If user provided key, use that
	if m.key != nil {
		keySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      *m.key,
				Namespace: m.owner.GetNamespace(),
			},
		}
		fields := []string{"psk.txt"}
		if err := utils.GetAndValidateSecret(ctx, m.client, m.logger, keySecret, fields...); err != nil {
			m.logger.Error(err, "Key Secret does not contain the proper fields")
			return nil, err
		}
		return m.key, nil
	}

	keySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volSyncRsyncTLSPrefix + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}

	err := m.client.Get(ctx, client.ObjectKeyFromObject(keySecret), keySecret)
	if client.IgnoreNotFound(err) != nil {
		m.logger.Error(err, "error retreiving key")
		return nil, err
	}

	if kerrors.IsNotFound(err) {
		keyData := make([]byte, 64)
		if _, err := rand.Read(keyData); err != nil {
			m.logger.Error(err, "error generating key")
			return nil, err
		}
		keySecret.StringData = map[string]string{
			"psk.txt": "volsync:" + hex.EncodeToString(keyData),
		}
		if err := ctrl.SetControllerReference(m.owner, keySecret, m.client.Scheme()); err != nil {
			m.logger.Error(err, utils.ErrUnableToSetControllerRef)
			return nil, err
		}
		utils.SetOwnedByVolSync(keySecret)

		if err := m.client.Create(ctx, keySecret); err != nil {
			m.logger.Error(err, "error creating key Secret")
			return nil, err
		}
	}

	m.updateStatusPSK(&keySecret.Name)
	return &keySecret.Name, nil
}

func (m *Mover) direction() string {
	dir := "src"
	if !m.isSource {
		dir = "dst"
	}
	return dir
}

func (m *Mover) serviceSelector() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      m.direction() + "-" + m.owner.GetName(),
		"app.kubernetes.io/component": "rsync-tls-mover",
		"app.kubernetes.io/part-of":   "volsync",
	}
}

func (m *Mover) Cleanup(ctx context.Context) (mover.Result, error) {
	m.logger.V(1).Info("Starting cleanup", "m.mainPVCName", m.mainPVCName, "m.isSource", m.isSource)
	if !m.isSource {
		m.logger.V(1).Info("removing snapshot annotations from pvc")
		// Cleanup the snapshot annotation on pvc for replicationDestination scenario so that
		// on the next sync (if snapshot CopyMethod is being used) a new snapshot will be created rather than re-using
		_, destPVCName := m.getDestinationPVCName()
		err := m.vh.RemoveSnapshotAnnotationFromPVC(ctx, m.logger, destPVCName)
		if err != nil {
			return mover.InProgress(), err
		}
	}

	err := utils.CleanupObjects(ctx, m.client, m.logger, m.owner, cleanupTypes)
	if err != nil {
		return mover.InProgress(), err
	}
	m.logger.V(1).Info("Cleanup complete")
	return mover.Complete(), nil
}

func (m *Mover) ensureSourcePVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *m.mainPVCName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(srcPVC), srcPVC); err != nil {
		m.logger.Error(err, "unable to get source PVC", "PVC", client.ObjectKeyFromObject(srcPVC))
		return nil, err
	}
	dataName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction()
	return m.vh.EnsurePVCFromSrc(ctx, m.logger, srcPVC, dataName, true)
}

// func (m *Mover) ensureSourcePVCs(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
// TODO: can this be moved to the volumehandler?
func (m *Mover) ensureSourcePVCs(ctx context.Context) (*pvcGroup, error) {
	srcPVCGroup := pvcGroup{}

	if m.mainPVCGroupSelector != nil {
		srcPVCGroup.isVolumeGroup = true

		/*
			if m.mainPVCGroup.Selector == nil {
				err := fmt.Errorf("unable to get source PVC - sourcePVC or sourcePVCGroupSelector must be specified")
				m.logger.Error(err, "ensureSourcePVCs")
				return nil, err
			}
		*/

		//FIXME: temp workaround without volumegroup stuff
		// 1. lookup PVCs by selector
		pvcSelector, err := metav1.LabelSelectorAsSelector(&m.mainPVCGroupSelector.Selector)
		if err != nil {
			m.logger.Error(err, "Unable to parse pvc volume group selector")
			return nil, err
		}
		listOptions := []client.ListOption{
			client.MatchingLabelsSelector{
				Selector: pvcSelector,
			},
			client.InNamespace(m.owner.GetNamespace()),
		}
		pvcGroupList := &corev1.PersistentVolumeClaimList{}
		err = m.client.List(ctx, pvcGroupList, listOptions...)
		if err != nil {
			return nil, err
		}

		if len(pvcGroupList.Items) == 0 {
			return nil, fmt.Errorf("No src PVCs found")
		}

		srcPVCGroup.pvcs = map[string]*corev1.PersistentVolumeClaim{}

		// 2. take individual snapshots (reusing vh code temporarily - will only work for CopyMode: Snapshot)
		for i := range pvcGroupList.Items {
			srcPVC := &pvcGroupList.Items[i]
			dataName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction() + "-" + srcPVC.GetName()
			pvcSnapshotted, err := m.vh.EnsurePVCFromSrc(ctx, m.logger, srcPVC, dataName, true)
			if err != nil || pvcSnapshotted == nil {
				//don't care if these are going to happen 1 at a time, will be replaced with volumesnapshot
				return nil, err
			}
			srcPVCGroup.pvcs[srcPVC.GetName() /*original PVC name*/] = pvcSnapshotted
		}
		//FIXME: end - should take a volumegroupsnapshot instead

	} else {
		srcPVCGroup.isVolumeGroup = false

		srcPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      *m.mainPVCName,
				Namespace: m.owner.GetNamespace(),
			},
		}
		if err := m.client.Get(ctx, client.ObjectKeyFromObject(srcPVC), srcPVC); err != nil {
			m.logger.Error(err, "unable to get source PVC", "PVC", client.ObjectKeyFromObject(srcPVC))
			return nil, err
		}
		dataName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction()
		pvc, err := m.vh.EnsurePVCFromSrc(ctx, m.logger, srcPVC, dataName, true)
		if err != nil || pvc == nil {
			return nil, err
		}
		srcPVCGroup.pvcs = map[string]*corev1.PersistentVolumeClaim{"data": pvc}
	}

	return &srcPVCGroup, nil
}

func (m *Mover) ensureDestinationPVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	isProvidedPVC, dataPVCName := m.getDestinationPVCName()
	if isProvidedPVC {
		return m.vh.UseProvidedPVC(ctx, dataPVCName)
	}
	// Need to allocate the incoming data volume
	return m.vh.EnsureNewPVC(ctx, m.logger, dataPVCName)
}

// func (m *Mover) ensureDestinationPVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
func (m *Mover) ensureDestinationPVCs(ctx context.Context) (*pvcGroup, error) {
	dstPVCGroup := pvcGroup{}

	if len(m.mainPVCGroup) > 0 {
		// Volume Group case
		dstPVCGroup.isVolumeGroup = true

		dstPVCGroup.pvcs = map[string]*corev1.PersistentVolumeClaim{}

		for _, memberPVC := range m.mainPVCGroup {
			// Need to allocate the incoming data volumes
			newPVCName := memberPVC.TempPVCName
			if newPVCName == "" {
				newPVCName = memberPVC.Name // Will match the original PVC name from the src in this case
			}
			newPVC, err := m.vh.EnsureNewPVC(ctx, m.logger, newPVCName)
			//FIXME: the above won't work if any pvcs have differing sizes or if they already exist.
			//FIXME: this is just temporary to test out for the moment
			if err != nil || newPVC == nil {
				return nil, err
			}
			dstPVCGroup.pvcs[memberPVC.Name /*The original src pvc name*/] = newPVC
		}
	} else {
		// Single PVC case
		dstPVCGroup.isVolumeGroup = false

		isProvidedPVC, dataPVCName := m.getDestinationPVCName()
		if isProvidedPVC {
			pvc, err := m.vh.UseProvidedPVC(ctx, dataPVCName)
			if err != nil || pvc == nil {
				return nil, err
			}
		}
		// Need to allocate the incoming data volume
		pvc, err := m.vh.EnsureNewPVC(ctx, m.logger, dataPVCName)
		if err != nil || pvc == nil {
			return nil, err
		}
		dstPVCGroup.pvcs = map[string]*corev1.PersistentVolumeClaim{"data": pvc}
	}

	return &dstPVCGroup, nil
}

func (m *Mover) getDestinationPVCName() (bool, string) {
	if m.mainPVCName == nil {
		newPvcName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction()
		return false, newPvcName
	}
	return true, *m.mainPVCName
}

//nolint:funlen
func (m *Mover) ensureJob(ctx context.Context, jobName string, dataPVC *corev1.PersistentVolumeClaim,
	origPVCName string, pvcCount int, namedTargetPortsMapping string,
	sa *corev1.ServiceAccount, rsyncSecretName string) (*batchv1.Job, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	logger := m.logger.WithValues("job", client.ObjectKeyFromObject(job))

	op, err := utils.CreateOrUpdateDeleteOnImmutableErr(ctx, m.client, job, logger, func() error {
		if err := ctrl.SetControllerReference(m.owner, job, m.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(job)
		utils.MarkForCleanup(m.owner, job)

		job.Spec.Template.ObjectMeta.Name = job.Name
		utils.AddAllLabels(&job.Spec.Template, m.serviceSelector())
		utils.SetOwnedByVolSync(&job.Spec.Template) // ensure the Job's Pod gets the ownership label
		backoffLimit := int32(2)
		job.Spec.BackoffLimit = &backoffLimit

		parallelism := int32(1)
		if m.paused {
			parallelism = int32(0)
		}
		job.Spec.Parallelism = &parallelism

		readOnlyVolume := false
		blockVolume := utils.PvcIsBlockMode(dataPVC)

		containerEnv := []corev1.EnvVar{}

		podSpec := &job.Spec.Template.Spec
		podSpec.Containers = []corev1.Container{{
			Name:  "rsync-tls",
			Image: m.containerImage,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(false),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				Privileged:             ptr.To(false),
				ReadOnlyRootFilesystem: ptr.To(true),
			},
		}}

		containerCmd := []string{"/bin/bash", "-c", "/mover-rsync-tls/server.sh"} // cmd for replicationDestination job
		if m.isSource {
			// Set dest address/port if necessary
			if m.address != nil {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "DESTINATION_ADDRESS", Value: *m.address})
			}
			if m.port != nil {
				connectPort := strconv.Itoa(int(*m.port))
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "DESTINATION_PORT", Value: connectPort})
			}
			// Set container cmd for the replicationSource job
			containerCmd = []string{"/bin/bash", "-c", "/mover-rsync-tls/client.sh"}

			// Set read-only for volume in repl source job spec if the PVC only supports read-only
			readOnlyVolume = utils.PvcIsReadOnly(dataPVC)
		} else {
			// Set dest named port in container spec to match the origPVCName
			podSpec.Containers[0].Ports = []corev1.ContainerPort{
				{
					Name:          origPVCName,
					ContainerPort: tlsContainerPort,
					Protocol:      corev1.ProtocolTCP,
				},
			}

			// Set env var for dest to contain pvc to ports mapping
			containerEnv = append(containerEnv, corev1.EnvVar{Name: "PVC_PORTS", Value: namedTargetPortsMapping})
		}
		podSpec.Containers[0].Command = containerCmd //FIXME: move this into the above if/else?

		volumeMounts := []corev1.VolumeMount{}
		//pvcList := ""
		if !blockVolume {
			mountPath := baseMountPath // Default (for one pvc with no volume group) mount at /data
			if pvcCount > 1 {          // Part of a volume group - mount at /data/<pvcName>
				mountPath = mountPath + "/" + origPVCName
				//TODO: do this for block too (outside if statement)
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "PVC_NAME", Value: origPVCName})
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "PVC_COUNT", Value: strconv.Itoa(pvcCount)})
			}
			volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: dataVolumeName, MountPath: mountPath})
		}

		podSpec.Containers[0].Env = containerEnv

		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "keys", MountPath: "/keys"},
			corev1.VolumeMount{Name: "tempdir", MountPath: "/tmp"})
		job.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		if blockVolume {
			job.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
				{Name: dataVolumeName, DevicePath: devicePath},
			}
		}
		podSpec.RestartPolicy = corev1.RestartPolicyNever
		podSpec.SecurityContext = m.moverSecurityContext
		podSpec.ServiceAccountName = sa.Name
		podSpec.Volumes = []corev1.Volume{
			{Name: dataVolumeName, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: dataPVC.Name,
					ReadOnly:  readOnlyVolume,
				}},
			},
			{Name: "keys", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  rsyncSecretName,
					DefaultMode: ptr.To[int32](0600),
				}},
			},
			{Name: "tempdir", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				}},
			},
		}

		if m.vh.IsCopyMethodDirect() {
			affinity, err := utils.AffinityFromVolume(ctx, m.client, logger, dataPVC)
			if err != nil {
				logger.Error(err, "unable to determine proper affinity", "PVC", client.ObjectKeyFromObject(dataPVC))
				return err
			}
			podSpec.NodeSelector = affinity.NodeSelector
			podSpec.Tolerations = affinity.Tolerations
		}
		if m.privileged {
			podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
				Name:  "PRIVILEGED_MOVER",
				Value: "1",
			})
			podSpec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{
				"DAC_OVERRIDE", // Read/write all files
				"CHOWN",        // chown files
				"FOWNER",       // Set permission bits & times
				"SETGID",       // Set process GID/supplemental groups
			}
			podSpec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](0)
		} else {
			podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
				Name:  "PRIVILEGED_MOVER",
				Value: "0",
			})
		}
		logger.V(1).Info("Job has PVC", "PVC", dataPVC, "DS", dataPVC.Spec.DataSource)
		return nil
	})
	// If Job had failed, delete it so it can be recreated
	if job.Status.Failed >= *job.Spec.BackoffLimit {
		// Update status with mover logs from failed job
		utils.UpdateMoverStatusForFailedJob(ctx, m.logger, m.latestMoverStatus, job.GetName(), job.GetNamespace(),
			LogLineFilterFailure)

		logger.Info("deleting job -- backoff limit reached")
		m.eventRecorder.Eventf(m.owner, job, corev1.EventTypeWarning,
			volsyncv1alpha1.EvRTransferFailed, volsyncv1alpha1.EvADeleteMover, "mover Job backoff limit reached")
		err = m.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		return nil, err
	}
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}

	logger.V(1).Info("Job reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		dir := "receive"
		if m.isSource {
			dir = "transmit"
		}
		m.eventRecorder.Eventf(m.owner, job, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRTransferStarted, volsyncv1alpha1.EvACreateMover, "starting %s to %s data",
			utils.KindAndName(m.client.Scheme(), job), dir)
	}

	// Stop here if the job hasn't completed yet
	if job.Status.Succeeded == 0 {
		return nil, nil
	}

	logger.Info("job completed")

	// update status with mover logs from successful job
	moverStatusToUpdate := m.latestMoverStatus
	if pvcCount > 1 {
		// VolumeGroup scenario - update the mover status for this particular pvc
		_, ok := m.latestVolumeGroupMoverStatus[origPVCName]
		if !ok {
			m.latestVolumeGroupMoverStatus[origPVCName] = &volsyncv1alpha1.MoverStatus{}
		}
		moverStatusToUpdate = m.latestVolumeGroupMoverStatus[origPVCName]
	}
	utils.UpdateMoverStatusForSuccessfulJob(ctx, m.logger, moverStatusToUpdate, job.GetName(), job.GetNamespace(),
		LogLineFilterSuccess)

	// We only continue reconciling if the rsync job has completed
	return job, nil
}

// TODO: Move me
type pvcGroup struct {
	isVolumeGroup  bool
	pvcs           map[string]*corev1.PersistentVolumeClaim
	pvcNamesSorted []string
	pvcPorts       map[string]int32
}

func (pg *pvcGroup) GetPVCByName(pvcName string) *corev1.PersistentVolumeClaim {
	return pg.pvcs[pvcName]
}

func (pg *pvcGroup) GetPVCNamesSorted() []string {
	if pg.pvcNamesSorted != nil {
		// Assume we've already sorted
		return pg.pvcNamesSorted
	}

	// Sort by name so we do not keep re-ordering Volumes/VolumeMounts
	// (range over map keys will give us inconsistent sorting)
	pg.pvcNamesSorted = make([]string, len(pg.pvcs))
	i := 0
	for key := range pg.pvcs {
		pg.pvcNamesSorted[i] = key
		i++
	}
	sort.Strings(pg.pvcNamesSorted)

	return pg.pvcNamesSorted
}

func (pg *pvcGroup) GetDestinationPort(pvcName string) int32 {
	if pg.pvcPorts == nil {
		pg.pvcPorts = map[string]int32{}

		pvcNamesSorted := pg.GetPVCNamesSorted()
		for i, pvcName := range pvcNamesSorted {
			pg.pvcPorts[pvcName] = int32(tlsContainerPort + 1 + i)
		}
	}

	return pg.pvcPorts[pvcName]
}

/*
func appendSortedPVCVolumeMounts(pvcGroup *pvcGroup, volumeMounts []corev1.VolumeMount) []corev1.VolumeMount {
	// Sort by name so we do not keep re-ordering VolumeMounts (range over map keys will give us inconsistent sorting)
	pvcNames := make([]string, len(pvcGroup.pvcs))
	i := 0
	for key := range pvcGroup.pvcs {
		pvcNames[i] = key
		i++
	}
	sort.Strings(pvcNames)

	for _, pvcName := range pvcNames {
		mountPath := baseMountPath // Default (for one pvc with no volume group) mount at /data
		if pvcGroup.isVolumeGroup {
			// If we're doing a volume group, mount each pvc at path that contains the original pvc name
			mountPath = baseMountPath + "/" + pvcName
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: pvcName, MountPath: mountPath})
	}

	return volumeMounts
}
*/
