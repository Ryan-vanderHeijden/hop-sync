#!/bin/sh
# Install the client's public key, make host keys, run sshd in the foreground.
# $SSHD_CONFIG chooses which auth setup this container uses.
set -e
mkdir -p /home/tester/.ssh
cp /authorized_keys_src /home/tester/.ssh/authorized_keys
chown -R tester:tester /home/tester/.ssh
chmod 700 /home/tester/.ssh
chmod 600 /home/tester/.ssh/authorized_keys
ssh-keygen -A
exec /usr/sbin/sshd -D -e -f "$SSHD_CONFIG"
