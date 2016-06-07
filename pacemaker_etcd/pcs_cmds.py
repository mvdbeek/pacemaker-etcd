import subprocess
import tempfile
import time
import logging
from contextlib import contextmanager
from lock import with_etcd_lock


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
log.addHandler(ch)


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions:
        log.exception("An exception occured:")
        pass


def change_pass(user, password):
    process = subprocess.Popen(['chpasswd'], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    process.communicate("{user}:{password}".format(user=user, password=password))


@with_etcd_lock
def bootstrap_cluster(user, password, node, name='Master'):
    with ignored(subprocess.CalledProcessError):
        _auth(user, password, node)
        time.sleep(1)
        _setup(name, node)
        time.sleep(1)
        _start()
        time.sleep(1)
        _enable()
        return True


@with_etcd_lock
def authorize_new_node(user, password, node):
    with ignored(subprocess.CalledProcessError):
        _auth(user, password, node)
        try:
            _add(node)
        except subprocess.CalledProcessError as e:
            if "node is already in a cluster" in e.output or "Error connecting to" in e.output:
                log.exception("Removing node from cluster")
                time.sleep(1)
                _remove(node)
                time.sleep(1)
                _add(node)
            else:
                raise
        return True


@with_etcd_lock
def remove_node(node):
    with ignored(subprocess.CalledProcessError):
        return _remove(node)


def join_cluster(user, password):
    _auth(user, password)
    time.sleep(1)
    _start()
    time.sleep(1)
    _enable()
    return True


def load_config(conf):
    """write conf to file and load with crm util"""
    tmp = tempfile.NamedTemporaryFile("w", delete=False)
    tmp.write(conf)
    tmp.close()
    load = ["crm", "configure", "load", "update", tmp.name]
    return subprocess.check_output(load)


def _start():
    start = ['pcs', 'cluster', 'start', '--all']
    return subprocess.check_output(start)


def _enable():
    enable = ['pcs', 'cluster', 'enable', '--all']
    return subprocess.check_output(enable)


def _auth(user, password, node=None):
    auth = ['pcs', 'cluster', 'auth', '-u', user, '-p', password]
    if node:
        auth.append(node)
    return subprocess.check_output(auth)


def _add(node):
    add = ['pcs', 'cluster', 'node', 'add', node]
    return subprocess.check_output(add, stderr=subprocess.STDOUT)


def _remove(node):
    remove = ["pcs", "cluster", "node", "remove", node]
    return subprocess.check_output(remove)


def _setup(name, node):
    setup = ['pcs', 'cluster', 'setup', '--name', name, node]
    return subprocess.check_output(setup)
