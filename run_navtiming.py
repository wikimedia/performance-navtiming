#!/usr/bin/env python

import navtiming
import os


if os.path.exists('/etc/wikimedia-cluster'):
    cluster = open('/etc/wikimedia-cluster', 'r').read().strip()
else:
    cluster = None

conf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
if os.path.exists(conf_path):
    import configparser
    config = configparser.ConfigParser(default_section='defaults')
    config.read(conf_path)
else:
    config = None

navtiming.main(cluster=cluster, config=config)
