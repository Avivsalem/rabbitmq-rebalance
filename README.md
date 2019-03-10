# rabbitmq-rebalance
a rebalance script for rabbitmq cluster, using the manangement api

this script balances the number of queues across the nodes of a cluster.  
the script tries to move queues that have synced mirrors on destination node first, 
and also favours small queues over big ones.
