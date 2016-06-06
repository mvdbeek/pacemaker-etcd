import etcd
import logging
import pcs_cmds
import threading
import time
from alive import call_repeatedly

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
log.addHandler(ch)

class EtcdBase(object):

    def __init__(self, ip, host, protocol, allow_redirect=True, prefix="/hacluster", ttl=60):
        self.ip = ip
        self.host = host
        self.protocol = protocol
        self.allow_redirect = allow_redirect
        self.prefix = prefix
        self.ttl = ttl
        self.client = etcd.Client(host=host, protocol=protocol, allow_reconnect=True, allow_redirect=allow_redirect)

    @property
    def user(self):
        return self.client.read("{prefix}/user".format(prefix=self.prefix)).value

    @property
    def members(self):
        return [child.key for child in self.client.read("%s/nodes" % self.prefix, recursive=True).children
                if not child.dir and child.value == "ready"]

    @property
    def password(self):
        return self.client.read("{prefix}/password".format(prefix=self.prefix)).value

    @property
    def cluster_is_up(self):
        return len(self.members) > 0

    @property
    def am_member(self):
        if self.ip in self.members:
            return True
        else:
            return False


class EtcdWatch(EtcdBase):

    def __init__(self, watch_key, timeout=None, recursive=False, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        self.key = "{prefix}/{key}".format(prefix=self.prefix, key=watch_key)
        self.result = self.watch(timeout=timeout, recursive=recursive)

    def watch(self, timeout, recursive):
        try:
            result = self.client.watch(self.key, timeout=timeout, recursive=recursive)
        except etcd.EtcdWatchTimedOut:
            result = self.watch(timeout=timeout, recursive=recursive)
        return result


class JoinOrCreateCluster(EtcdBase):
    """
    Join an existing cluster or create a new cluster if no nodes are registered in etcd
    """

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        if not self.cluster_is_up:
            success = self.create_new_cluster()
            if success:
                self.load_cib_config()
            else:
                self.join_cluster()  # In case another node has created a new cluster
        else:
            self.join_cluster()

    def join_cluster(self):
        self.make_request()
        success = self.wait_for_request()
        if success:
            pcs_cmds.join_cluster(user=self.user, password=self.password)
            log.info("Succesfully joined cluster")

    def make_request(self):
        self.client.write(key="%s/nodes/%s" % (self.prefix, self.ip),
                          value="request_join",
                          ttl=120)
        return True

    def wait_for_request(self):
        log.info("Waiting for approval of IP")
        watch = EtcdWatch(watch_key="nodes/%s" % self.ip,
                          ip=self.ip,
                          host=self.host,
                          protocol=self.protocol,
                          allow_redirect=self.allow_redirect,
                          prefix=self.prefix)
        if watch.result.value == 'ready':
            return True
        return False

    def create_new_cluster(self):
        success = pcs_cmds.bootstrap_cluster(self, user=self.user, password=self.password, node=self.ip)
        if success:
            self.load_cib_config()
            self.client.write("%s/nodes/%s" % (self.prefix, self.ip), value="ready", ttl=self.ttl)
            return True
        return False

    def load_cib_config(self):
        try:
            conf = self.client.read("{prefix}/cib".format(prefix=self.prefix)).value
            pcs_cmds.load_config(conf)
        except etcd.EtcdKeyNotFound:
            log.info("No CIB config found at {prefix}/cib".format(prefix=self.prefix))
            pass


class WatchCluster(EtcdBase):

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        try:
            self.do_watch()
        finally:
            if self.am_member:
                self.stop_signal()

    def do_watch(self):
        if not self.am_member:
            time.sleep(1)
            self.do_watch()
        while True:
            self.stop_signal = self.send_alive_signal()
            self.watch = EtcdWatch(watch_key='nodes',
                                   ip=self.ip,
                                   host=self.host,
                                   protocol=self.protocol,
                                   allow_redirect=self.allow_redirect,
                                   prefix=self.prefix,
                                   recursive=True)
            ip = self.watch.result.key.split(self.prefix, 1)[-1].split("/")[-1]
            if self.watch.result.action == "expire":
                pcs_cmds.remove_node(self, ip)
            else:
                value = self.watch.result.value
                if value == "ready":
                    continue
                if self.am_member and value == "request_join":
                    success = pcs_cmds.authorize_new_node(self, user=self.user, password=self.password, node=self.watch.result.value)
                    if success:
                        self.client.write("%s/nodes/%s" % (self.prefix, self.ip), value='ready', ttl=self.ttl)

    @call_repeatedly
    def send_alive_signal(self):
        self.client.refresh("%s/nodes/%s" % (self.prefix, self.ip), ttl=self.ttl)


class WatchPassword(EtcdBase):
    """
    Watches for changes in /password  and updates the password
    """

    def __init__(self, **kwargs):
        EtcdBase.__init__(self, **kwargs)
        pcs_cmds.change_pass(self.user, self.password)
        while True:
            self.watch = EtcdWatch(watch_key='password',
                                   ip=self.ip,
                                   host=self.host,
                                   protocol=self.protocol,
                                   allow_redirect=self.allow_redirect,
                                   prefix=self.prefix)
            pcs_cmds.change_pass(self.user, self.password)
