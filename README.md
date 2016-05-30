pacemaker-etcd
---

pacemaker-etcd attempts to manage pacemaker cluster members in etcd.
The usual procedure to add a node to pacemaker clusters requires
starting pacemaker on the new node, then adding the node from a current
member of the pacemaker cluster, and then authenticating to the cluster from
the new node.

With pacemaker-etcd, existing cluster-members run 
```
python pcs_etcd.py watch <etcd-cluster-ip:port>
```

When a new node requests to join the cluster, it writes its IP into /hacluster/newnode,
sets /hacluster/<ip> to False and waits for existing members to authenticate the node.

When a new node has been authentified, /hacluster/newnode will be set back to "",
and /hacluster/<ip> to True. The new node then finishes joining the cluster and removes
/hacluster/<ip> and adds itself to /hacluster/nodelist.

