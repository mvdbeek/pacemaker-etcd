#!/usr/bin/env python

from distutils.core import setup

setup(name='pacemaker-etcd',
      version='0.1',
      description='Manage pacemaker nodes in etcd',
      author='Marius van den Beek',
      author_email='m.vandenbeek@gmail.com',
      url='https://github.com/mvdbeek/pacemaker-etcd/',
      py_modules=['etcd_management', 'pcs_cmds', 'pcs_etcd'],
      scripts=["pcs-etcd"],
      install_requires=["python-etcd"],
      )
