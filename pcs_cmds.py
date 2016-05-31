import subprocess


def change_pass(user, password):
    process = subprocess.Popen(['chpasswd'], stdout=subprocess.PIPE, stdin=subprocess.PIPE,stderr=subprocess.PIPE)
    process.communicate("{user}:{password}".format(user=user, password=password))


def bootstrap_cluster(user, password, node, name='Master'):
    _auth(user, password, node)
    _setup(name, node)
    _start()
    _enable()
    return True


def authorize_new_node(user, password, node):
    _auth(user, password, node)
    try:
        _add(node)
    except subprocess.CalledProcessError as e:
        if "node is already in a cluster" in e.output:
            _remove(node)
            _add(node)
        else:
            raise
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
    return subprocess.check_output(add, stderr=subprocess.STDOUT)


def _remove(node):
    remove = ["pcs", "cluster", "node", "remove", node ]
    return subprocess.check_output(remove)


def _setup(name, node):
    setup = ['pcs', 'cluster', 'setup', '--name', name, node]
    return subprocess.check_output(setup)
