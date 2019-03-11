# rabbitmq-rebalance
a rebalance script for rabbitmq cluster, using the manangement api

this script balances the number of queues across the nodes of a cluster.  
the script tries to move queues that have synced mirrors on destination node first, 
and also favours small queues over big ones.


## setup:  

there are 5 parameters that should be set before running the script. all are consts in the head of the script:
```
HOST = "rabbitmq-host"       # hostname of a cluster node that has the management plugin enabled
VHOST = "/"                  # the vhost in the cluster, to connect to
USER = "rabbituser"          # username with enough permissions to create policies on the vhost
PASSWORD = "rabbitpassword"  # the password for this user

SYNC_TIMEOUT = 60            # the number of seconds to wait for a queue to sync/move to a new node
```

change those to control the connection parameters to the cluster

## usage:
`python rabbitmq_rebalance.py`