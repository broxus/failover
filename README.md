## failover

etcd-based consensus for [hot spare

### Algorithm

#### Lease part

0. Client asks etcd for an initial lease for N seconds and receives a lease id.
1. The lease is renewed every N/2 seconds.
2. If connection is lost then client uses the same lease id to renew the lease.

> NOTE: It's &frac12;<sup>64</sup> chance that the lease ID is reused.

#### Election part

1. Client starts a campaign with a random timeout. If election expires then there is another leader. Client asks **etcd** for the leader lease id and waits until it dies.
2. Client polls **etcd** every lease lifetime seconds. If **etcd** returns -1 then leader is dead, goto 1.
