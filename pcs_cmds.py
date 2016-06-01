import subprocess
import tempfile
import time
import logging


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
log.addHandler(ch)


def change_pass(user, password):
    process = subprocess.Popen(['chpasswd'], stdout=subprocess.PIPE, stdin=subprocess.PIPE,stderr=subprocess.PIPE)
    process.communicate("{user}:{password}".format(user=user, password=password))


def bootstrap_cluster(user, password, node, name='Master'):
    _auth(user, password, node)
    time.sleep(1)
    _setup(name, node)
    time.sleep(1)
    _start()
    time.sleep(1)
    _enable()
    return True


def authorize_new_node(user, password, node):
    _auth(user, password, node)
    try:
        _add(node)
    except subprocess.CalledProcessError as e:
        if "node is already in a cluster" in e.output or "Error connecting to" in e.output:
            log.info("Removing node from cluster")
            time.sleep(1)
            _remove(node)
            time.sleep(1)
            _add(node)
        else:
            raise
    return True


def join_cluster(user, password):
    _auth(user, password)
    time.sleep(1)
    _start()
    time.sleep(1)
    _enable()
    return True


def load_config(conf):
    """write conf to file and load with crm util"""
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open(tmp, "w") as conf_file:
        conf_file.write(tmp)
    load = ["crm", "configure", "load", "update", conf]
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
    remove = ["pcs", "cluster", "node", "remove", node ]
    return subprocess.check_output(remove)


def _setup(name, node):
    setup = ['pcs', 'cluster', 'setup', '--name', name, node]
    return subprocess.check_output(setup)
