import argparse
import os
import sys

from etcd_management import WatchCluster
from etcd_management import JoinOrCreateCluster
from etcd_management import WatchPassword
from etcd_management import RemoveSelf


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


class PcsEtcdArgs(object):

    def __init__(self):
        self.parents = [self._parentparser()]
        parser = argparse.ArgumentParser(
            description='Script to create, watch and join PCS clusters with etcd',
            usage='''pcs_etcd.py <command> [<args>]

   join_or_create       Join or create a new cluster from scratch
   watch                Monitor etcd directory for requests from new nodes.
                        When cancelled will remove itself from the nodelist.
   watch_pass           Watch /prefix/password for changes and update password
''')
        parser.add_argument('command', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def _parentparser(self):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('--etcd_nodes', type=self.hosts,
                            action=EnvDefault, envvar="ETCD",
                            help='Etcd cluster connection, e.g 127.0.0.1:4001. '
                                 'Provide multiple etcd nodes separated by commas')
        parser.add_argument('--my_ip', action=EnvDefault, envvar="MY_IP",
                            help='IP of current node to join pacemaker cluster')
        parser.add_argument('--protocol', default='http', help='Protocol to use to connect to etcd')
        parser.add_argument('--prefix', default="/hacluster", help='prefix for etcd directory'),
        return parser

    def hosts(self, s):
        """
        Returns tuple of tuples of the form ((host1, port1), (host2, port2)) from s.

        >>> s = "localhost:2379,localhost2:2379"
        >>> hosts(object, s) == (('localhost', 2379), ('localhost2', 2379))
        True
        """
        s=s.strip()
        try:
            if ',' in s:
                hosts_ports = [host_port.split(':') for host_port in s.split(',')]
            elif ' ' in s:
                hosts_ports = [host_port.split(':') for host_port in s.split(' ')]
            else:
                hosts_ports = [s.split(':')]
            hosts = tuple(((host_port[0], int(host_port[1])) for host_port in hosts_ports),)
            return hosts
        except:
            raise argparse.ArgumentTypeError("Nodes must be defined as <ip:port> or <ip2:port2,ip2:port2>")

    def watch(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            parents=self.parents,
            description='Watch etcd directory for new nodes')
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command and the subcommand
        args = parser.parse_args(sys.argv[2:])
        try:
            WatchCluster(ip=args.my_ip, host=args.etcd_nodes, protocol=args.protocol, prefix=args.prefix)
        finally:
            RemoveSelf(ip=args.my_ip, host=args.etcd_nodes, protocol=args.protocol, prefix=args.prefix)

    def watch_pass(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            parents=self.parents,
            description='Watch etcd directory for new password')
        args = parser.parse_args(sys.argv[2:])
        WatchPassword(ip=args.my_ip, host=args.etcd_nodes, protocol=args.protocol, prefix=args.prefix)

    def join_or_create(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            parents=self.parents,
            description='Create a new cluster from scratch')
        args = parser.parse_args(sys.argv[2:])
        JoinOrCreateCluster(ip=args.my_ip, host=args.etcd_nodes, protocol=args.protocol, prefix=args.prefix)


if __name__ == '__main__':
    PcsEtcdArgs()
