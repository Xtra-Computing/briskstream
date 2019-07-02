#!/usr/bin/env bash
1. ssh-keygen -t rsa
Press enter for each line
2. cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
3. chmod og-wx ~/.ssh/authorized_keys
4. chmod 750 $HOME