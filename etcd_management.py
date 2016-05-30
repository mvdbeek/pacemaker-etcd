import etcd
import os
import time
import subprocess
from pcs_cmds import authorize_new_node
from pcs_cmds import bootstrap_cluster
from pcs_cmds import join_cluster


class CurrentHost(object):

    def __init__(self):
        self.ip = os.getenv("MY_IP", None)


class EtcdBase(object):

    def __init__(self, host, protocol, allow_redirect=True, prefix="/hacluster"):
        self.host = CurrentHost()
        self.client = etcd.Client(host=host, protocol=protocol, allow_reconnect=True, allow_redirect=allow_redirect)
        self.prefix = prefix

    @property
    def user(self):
        return self.client.read("{prefix}/user".format(prefix=self.prefix)).value

    @property
    def nodelist(self):
        return self.client.read("{prefix}/nodelist".format(prefix=self.prefix)).value

    @property
    def password(self):
        return self.client.read("{prefix}/password".format(prefix=self.prefix)).value

    def am_member(self):
        if self.host.ip in self.nodelist:
            return True
        else:
            return False


class EtcdWatch(EtcdBase):

    def __init__(self, watch_key, timeout=None, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        self.key = "{prefix}/{key}".format(prefix=self.prefix, key=watch_key)
        self.result = self.client.watch(self.key, timeout=timeout)


class AuthorizationRequest(EtcdBase):

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        self.key = "{prefix}/{key}".format(prefix=self.prefix, key="newnode")
        self.count = 0
        self.make_request()
        success = self.wait_for_request()
        if success:
            join_cluster(user=self.user, password=self.password)
            self.add_to_nodelist()

    def make_request(self):
        self.count += 1
        try:
            newnode = self.client.read(self.key)
        except etcd.EtcdKeyNotFound:
            newnode = self.client.write(key=self.key, value="")
        if newnode == "":
            self.client.write(key=self.key, value=self.host.ip, ttl=120)
            self.client.write(key="%s/%s" % (self.prefix, self.host.ip), value='False', ttl=120)
        elif newnode != "":
            if self.count > 120:
                raise Exception("Could not authorize node {ip} within 2 minutes".format(ip=self.host.ip))
            time.sleep(1)
            self.make_request()

    def wait_for_request(self):
        self.count += 1
        watch = EtcdWatch("%s/%s" % (self.prefix, self.host.ip), timeout=300)
        if watch.result.value == 'True':
            return True
        return False

    def add_to_nodelist(self):
        nodelist = "%s,%s" % (self.nodelist, self.host.ip)
        self.client.write("{prefix}/nodelist", value=nodelist)


class Authorizer(EtcdBase):
    """
    Watches for changes in /newnode and attempts to authorize these (if a lock can be acquired).
    Requires that current host is cluster member
    """
    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        if self.am_member():
            while True:
                self.watch = EtcdWatch('newnode', **kwargs)
                if self.watch.result.value == '':  # This is the default key value when no hosts are to be added.
                    continue
                with etcd.Lock(self.client, 'newnode') as lock:
                    lock.acquire(timeout=120)
                    if lock.is_acquired():
                        try:
                            authorize_new_node(lock.value, self.user, self.password)
                            lock.release()
                            self.client.write(key="%s/%s" % (self.prefix, "newnode"), value="")
                            self.client.write(key="%s/%s" % (self.prefix, self.watch.result.value), value='True')
                        except subprocess.CalledProcessError:
                            # TODO: log this
                            continue


class CreateCluster(EtcdBase):
    '''
    Explicitly create a new cluster (overwriting any existing nodelist).
    TODO: Set and update TTL on nodelist as long as cluster exists. If nodelist doesn't exist,
    let first client trying to join the cluster use this class.
    '''

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        success = bootstrap_cluster(user=self.user, password=self.password, node=self.host.ip)
        if success:
            self.client.write("%s/nodelist" % self.prefix)
