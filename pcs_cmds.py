import subprocess


def bootstrap_cluster(user, password, node, name='Master'):
    _auth(user, password, node)
    _setup(name, node)
    _start()
    _enable()
    return True


def authorize_new_node(user, password, node):
    _auth(user, password, node)
    _add(node)
    return True


def join_cluster(user, password):
    _auth(user, password)
    _start()
    _enable()
    return True


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
    return subprocess.check_output(add)


def _setup(name, node):
    setup = ['pcs', 'cluster', 'setup', '-name', name, node]
    return subprocess.check_output(setup)
