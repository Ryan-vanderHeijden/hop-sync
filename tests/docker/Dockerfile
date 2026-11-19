FROM alpine:3.20
RUN apk add --no-cache openssh-server openssh-sftp-server coreutils \
 && adduser -D -s /bin/sh tester \
 && echo "tester:hunter2pass" | chpasswd
COPY entrypoint.sh /entrypoint.sh
COPY sshd_key_only.conf /etc/ssh/sshd_key_only.conf
COPY sshd_key_and_password.conf /etc/ssh/sshd_key_and_password.conf
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
