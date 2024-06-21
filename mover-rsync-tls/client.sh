#! /bin/bash

set -e -o pipefail

DESTINATION_PORT="${DESTINATION_PORT:-8000}"
if [[ -z "$DESTINATION_ADDRESS" ]]; then
    echo "Remote host address must be provided in DESTINATION_ADDRESS"
    exit 1
fi


STUNNEL_CONF=/tmp/stunnel-client.conf
STUNNEL_PID_FILE=/tmp/stunnel-client.pid
PSK_FILE=/keys/psk.txt
STUNNEL_LISTEN_PORT=9000
SOURCE="/data"
BLOCK_SOURCE="/dev/block"

SCRIPT_FULLPATH="$(realpath "$0")"
SCRIPT="$(basename "$SCRIPT_FULLPATH")"
SCRIPT_DIR="$(dirname "$SCRIPT_FULLPATH")"

# Do not do this debug mover code if this is already the
# mover script copy in /tmp
if [[ $DEBUG_MOVER -eq 1 && "$SCRIPT_DIR" != "/tmp" ]]; then
  MOVER_SCRIPT_COPY="/tmp/$SCRIPT"
  cp "$SCRIPT_FULLPATH" "$MOVER_SCRIPT_COPY"

  END_DEBUG_FILE="/tmp/exit-debug-if-removed"
  touch $END_DEBUG_FILE

  echo ""
  echo "##################################################################"
  echo "DEBUG_MOVER is enabled, this pod will sleep indefinitely."
  echo ""
  echo "The mover script that would normally run has been copied to"
  echo "$MOVER_SCRIPT_COPY".
  echo ""
  echo "To debug, you can modify this file and run it with:"
  echo "$MOVER_SCRIPT_COPY" "$@"
  echo ""
  echo "If you wish to exit this pod after debugging, delete the"
  echo "file $END_DEBUG_FILE from the system."
  echo "##################################################################"

  # Wait for user to delete the file before exiting
  while [[ -f "${END_DEBUG_FILE}" ]]; do
    sleep 10
  done

  echo ""
  echo "##################################################################"
  echo "Debug done, exiting."
  echo "##################################################################"
  sleep 2
  exit 0
fi

PVC_LIST="${PVC_LIST:-}"
PVC_NAME="${PVC_NAME:-}"
PVC_COUNT="${PVC_COUNT:-1}"
PVC_PORTS_FILE=/tmp/pvc-ports
STUNNEL_PORT_MAPPING_FILE=/tmp/stunnel-port-mapping

cd "$SCRIPT_DIR"

# shellcheck disable=SC2317  # It's reachable due to the TRAP
function stop_stunnel() {
    ## Terminate stunnel
    kill -TERM "$(<"$STUNNEL_PID_FILE")"
}

if [[ ! -r $PSK_FILE ]]; then
    echo "ERROR: Pre-shared key not found - $PSK_FILE"
    exit 1
fi

if [[ ! -d $SOURCE ]] && ! test -b $BLOCK_SOURCE; then
    echo "ERROR: source location not found"
    exit 1
fi

if ! test -b $BLOCK_SOURCE; then
    echo "Source PVC volumeMode is filesystem"

    cat - > "$STUNNEL_CONF" <<STUNNEL_CONF
; Global options
debug = debug
foreground = no
output = /dev/stdout
pid = $STUNNEL_PID_FILE
socket = l:SO_KEEPALIVE=1
socket = l:TCP_KEEPIDLE=180
socket = r:SO_KEEPALIVE=1
socket = r:TCP_KEEPIDLE=180
syslog = no

[rsync]
ciphers = PSK
PSKsecrets = $PSK_FILE
; Port to listen for incoming connection from rsync
accept = 127.0.0.1:$STUNNEL_LISTEN_PORT
; We are the client
client = yes
connect = $DESTINATION_ADDRESS:$DESTINATION_PORT
STUNNEL_CONF

    # For Volume group scenario - setup ports in sequence to map to
    # additional ports on the destination - 1 extra port per pvc in the group.
    # port 9001 locally will map to 8001 remotely, 9002->8002 etc.
    for (( i=1 ; i<="${PVC_COUNT}" ; i++ ));
    do
        ADDITIONAL_DESTINATION_PORT=$((DESTINATION_PORT + i))
        ADDITIONAL_LISTEN_PORT=$((STUNNEL_LISTEN_PORT + i))

        cat - >> "${STUNNEL_CONF}" <<STUNNEL_ADDITIONAL_CONF

[rsync-${ADDITIONAL_DESTINATION_PORT}]
ciphers = PSK
PSKsecrets = ${PSK_FILE}
; Port to listen for incoming connection from rsync
accept = 127.0.0.1:${ADDITIONAL_LISTEN_PORT}
; We are the client
client = yes
connect = ${DESTINATION_ADDRESS}:${ADDITIONAL_DESTINATION_PORT}
STUNNEL_ADDITIONAL_CONF

        # Map local to remote ports in STUNNEL_PORT_MAPPING_FILE file
        echo ">${ADDITIONAL_DESTINATION_PORT} ${ADDITIONAL_LISTEN_PORT}" >> "${STUNNEL_PORT_MAPPING_FILE}"
    done

    ##############################
    ## Print version information
    rsync --version
else
    echo "Source PVC volumeMode is block"

    cat - > "$STUNNEL_CONF" <<STUNNEL_CONF
; Global options
debug = debug
foreground = no
output = /dev/stdout
pid = $STUNNEL_PID_FILE
socket = l:SO_KEEPALIVE=1
socket = l:TCP_KEEPIDLE=180
socket = r:SO_KEEPALIVE=1
socket = r:TCP_KEEPIDLE=180
syslog = no

[diskrsync]
ciphers = PSK
PSKsecrets = $PSK_FILE
; Port to listen for incoming connection from diskrsync
accept = 127.0.0.1:$STUNNEL_LISTEN_PORT
; We are the client
client = yes
connect = $DESTINATION_ADDRESS:$DESTINATION_PORT
STUNNEL_CONF
fi
stunnel -version "$STUNNEL_CONF"

##############################
## Start stunnel to wait for incoming connections
stunnel "$STUNNEL_CONF"
trap stop_stunnel EXIT

# Sync files
START_TIME=$SECONDS
MAX_RETRIES=5
RETRY=0
DELAY=2
FACTOR=2
rc=1
set +e  # Don't exit on command failure
echo "Syncing data to ${DESTINATION_ADDRESS}:${DESTINATION_PORT} ..."
while [[ ${rc} -ne 0 && ${RETRY} -lt ${MAX_RETRIES} ]]; do
    RETRY=$(( RETRY + 1 ))
    if test -b $BLOCK_SOURCE; then
      echo "calling diskrsync-tcp $BLOCK_SOURCE --source --target-address 127.0.0.1 --port $STUNNEL_LISTEN_PORT"
      /diskrsync-tcp $BLOCK_SOURCE --source --target-address 127.0.0.1 --port $STUNNEL_LISTEN_PORT
      rc=$?
    else
        # Find all files/dirs at root of pvc, prepend / to each (rsync will use SOURCE as the base dir for these files)
        find "${SOURCE}" -mindepth 1 -maxdepth 1 -printf '/%P\n' > /tmp/filelist.txt
        if [[ -s /tmp/filelist.txt ]]; then
            # 1st run preserves as much as possible, but excludes the root directory
            rsync -aAhHSxz -r --exclude=lost+found --itemize-changes --info=stats2,misc2 --files-from=/tmp/filelist.txt ${SOURCE}/ rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/data
        fi
        #PVC_LIST="${SOURCE}"
        #if [[ ${IS_VOLUME_GROUP} -eq 1 ]]; then
        #  PVC_LIST=$(ls "${SOURCE}")
        #  echo "Copying volume group of pvcs: PVC_LIST=${PVC_LIST} ..."
        #fi
        echo "==============================="
        echo "PVC_LIST: ${PVC_LIST} ..."
        echo "==============================="
        echo ""
        rc=0
        SOURCE_DIR=${SOURCE}
        SOURCE_STUNNEL_LISTEN_PORT=${STUNNEL_LISTEN_PORT}

        if [[ "${PVC_COUNT}" -gt 1 ]]; then
            echo "==== Volume Group scenario ====" #TODO: remove

            # Copy down pvc-ports file from remote common port
            echo CALLING rsync://127.0.0.1:"${STUNNEL_LISTEN_PORT}"/control/pvc-ports "${PVC_PORTS_FILE}"
            echo
            rsync rsync://127.0.0.1:"${STUNNEL_LISTEN_PORT}"/control/pvc-ports "${PVC_PORTS_FILE}"
            rc=$?

            if [[ ${rc} -eq 0 ]]; then
                SOURCE_DIR="${SOURCE}/${PVC_NAME}"

                REMOTE_PORT=$(grep ">${PVC_NAME} " "${PVC_PORTS_FILE}" | awk '{print $2}')
                SOURCE_STUNNEL_LISTEN_PORT=$(grep ">${REMOTE_PORT} " "${STUNNEL_PORT_MAPPING_FILE}" | awk '{print $2}')
                rc=$?
            fi
        fi

        if [[ ${rc} -eq 0 ]]; then
            ls -A "${SOURCE_DIR}"/ > /tmp/filelist.txt
            if [[ -s /tmp/filelist.txt ]]; then
                #TODO: remove this debug stuff
                echo "###### First pass #####"
                echo ""
                find "${SOURCE}"
                echo ">> SOURCE_DIR: ${SOURCE_DIR}"
                echo ">> SOURCE_STUNNEL_LISTEN_PORT: ${SOURCE_STUNNEL_LISTEN_PORT}"
                echo ""
                #TODO: end TODO

                # 1st run preserves as much as possible, but excludes the root directory
                rsync -aAhHSxz -r --exclude=lost+found --itemize-changes --info=stats2,misc2 --files-from=/tmp/filelist.txt "${SOURCE_DIR}/" rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}""${SOURCE_DIR}"
            else
                echo "Skipping sync of empty source directory"
            fi
            rc_a=$?
            shopt -u dotglob  # Back to default * behavior

            echo "###### Second pass (cleanup) #####"
            # To delete extra files, must sync at the directory-level, but need to avoid
            # trying to modify the directory itself. This pass will only delete files
            # that exist on the destination but not on the source, not make updates.
            echo CALLING rsync -rx --exclude=lost+found --ignore-existing --ignore-non-existing --delete --itemize-changes --info=stats2,misc2 "${SOURCE_DIR}"/ rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}""${SOURCE_DIR}"
            echo
            #rsync -rx -vvv --exclude=lost+found --ignore-existing --ignore-non-existing --delete --itemize-changes --info=stats2,misc2 "${SOURCE_DIR}"/ rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}""${SOURCE_DIR}"
            rsync -rx --exclude=lost+found --ignore-existing --ignore-non-existing --delete --itemize-changes --info=stats2,misc2 "${SOURCE_DIR}"/ rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}""${SOURCE_DIR}"
            rc_b=$?
            rc=$(( rc_a * 100 + rc_b ))
        fi
    fi
    if [[ $rc -ne 0 ]]; then
        echo "Syncronization failed. Retrying in $DELAY seconds. Retry ${RETRY}/${MAX_RETRIES}."
        sleep $DELAY
        DELAY=$(( DELAY * FACTOR ))
    fi
done
set -e  # Exit on command failure

if test -b $BLOCK_SOURCE; then
    echo "diskrsync completed in $(( SECONDS - START_TIME ))s"
else
    echo "rsync completed in $(( SECONDS - START_TIME ))s"

    if [[ $rc -eq 0 ]]; then
        # Tell server to shutdown. Actual file contents don't matter
        echo "Sending shutdown to remote..."
        rsync "$SCRIPT_FULLPATH" rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/control/complete
        echo CALLING rsync "${SCRIPT}" rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}"/control/complete
        echo
        rsync "${SCRIPT}" rsync://127.0.0.1:"${SOURCE_STUNNEL_LISTEN_PORT}"/control/complete
        echo "...done"
        sleep 5  # Give time for the remote to shut down
    else
        echo "Synchronization failed. rsync returned: $rc"
    fi
fi

exit $rc
