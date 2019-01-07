#!/usr/bin/env bash
1. ssh-keygen -t rsa
Press enter for each line
2. cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
3. chmod og-wx ~/.ssh/authorized_keys
4. chmod 750 $HOME




putty:

Ok, it is fixed however I don't see how this is different from what I tried already.

What I did:

generate a key pair with puttygen.exe (length: 1024 bits)
load the private key in the PuTTY profile
enter the public key in ~/.ssh/authorized_keys in one line (needs to start with ssh-rsa)
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
chown $USER:$USER ~/.ssh -R
change /etc/ssh/sshd_config so it contains AuthorizedKeysFile %h/.ssh/authorized_keys
sudo service ssh restart
For troubleshooting do # tail -f /var/log/auth.log.

Thanks for your help!