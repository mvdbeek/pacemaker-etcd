import etcd
import logging
import time
import subprocess
from pcs_cmds import authorize_new_node
from pcs_cmds import bootstrap_cluster
from pcs_cmds import change_pass
from pcs_cmds import join_cluster

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
log.addHandler(ch)

class EtcdBase(object):

    def __init__(self, ip, host, protocol, allow_redirect=True, prefix="/hacluster"):
        self.ip = ip
        self.host = host
        self.protocol = protocol
        self.prefix = prefix
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
        if self.ip in self.nodelist:
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
        try:
            success = self.wait_for_request()
        except etcd.EtcdWatchTimedOut:
            self.client.write(key=self.key, value="None").value  # TODO: replace this logic with a lock, may result in race condition
            self.client.delete(key="%s/%s" % (self.prefix, self.ip))
            raise
        if success:
            join_cluster(user=self.user, password=self.password)
            self.add_to_nodelist()

    def make_request(self):
        self.count += 1
        try:
            newnode = self.client.read(self.key).value
        except etcd.EtcdKeyNotFound:
            newnode = self.client.write(key=self.key, value="None").value
        if newnode == "None":
            self.client.write(key=self.key, value=self.ip, ttl=120)
            self.client.write(key="%s/%s" % (self.prefix, self.ip), value='False', ttl=120)
        elif newnode != "":
            if self.count > 120:
                raise Exception("Could not authorize node {ip} within 2 minutes".format(ip=self.ip))
            time.sleep(1)
            self.make_request()
        elif newnode == self.ip:
            return True
        return True

    def wait_for_request(self):
        self.count += 1
        watch = EtcdWatch(watch_key="%s/%s" % (self.prefix, self.ip),
                          timeout=300,
                          ip=self.ip,
                          host=self.host,
                          protocol=self.protocol,
                          prefix=self.prefix)
        if watch.result.value == 'True':
            return True
        return False

    def add_to_nodelist(self):
        nodelist = "%s,%s" % (self.nodelist, self.ip)
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
                log.info("looking for newnode changes")
                self.watch = EtcdWatch(watch_key='newnode',
                                       ip=self.ip,
                                       host=self.host,
                                       protocol=self.protocol,
                                       prefix=self.prefix)
                if self.watch.result.value == 'None':  # This is the default key value when no hosts are to be added.
                    continue
                log.info("newnode changed to %s" % self.watch.result.value)
                lock = etcd.Lock(self.client, self.watch.result.value)
                lock.acquire(lock_ttl=60)
                try:
                    authorize_new_node(user=self.user, password=self.password, node=self.watch.result.value)
                    lock.release()
                    self.client.write(key="%s/%s" % (self.prefix, "newnode"), value="None")
                    self.client.write(key="%s/%s" % (self.prefix, self.watch.result.value), value='True')
                except subprocess.CalledProcessError as e:
                    log.error(e)
                    lock.release()
                    continue
        else:
            raise Exception("Could not start watcher, node is not member of cluster")


class WatchPassword(EtcdBase):
    '''
    Watches for changes in /password  and updates the password
    '''

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        change_pass(self.user, self.password)
        while True:
            try:
                self.watch = EtcdWatch(watch_key='password',
                                       ip=self.ip,
                                       host=self.host,
                                       protocol=self.protocol,
                                       prefix=self.prefix)
            except etcd.EtcdWatchTimedOut:
                continue
            change_pass(self.user, self.password)


class CreateCluster(EtcdBase):
    '''
    Explicitly create a new cluster (overwriting any existing nodelist).
    TODO: Set and update TTL on nodelist as long as cluster exists. If nodelist doesn't exist,
    let first client trying to join the cluster use this class.
    '''

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        success = bootstrap_cluster(user=self.user, password=self.password, node=self.ip)
        if success:
            self.client.write("%s/nodelist" % self.prefix, value=self.ip)
