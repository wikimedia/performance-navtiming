# navtiming

The navtiming service processes web beacons containing performance data from web browsers, and submits
aggregate metrics to Prometheus and Statsd.

Python 3.9 or later is required.

## Local development

### Testing

1. Ensure [pip](https://pip.pypa.io/en/stable/installing/) is installed. If your system has Python installed without pip (such as on macOS), you can either install it with [EasyInstall](https://setuptools.readthedocs.io/en/latest/easy_install.html) using `sudo easy_install pip`, or choose to install Python locally from Homebrew (which comes with pip).
2. Ensure [tox](https://tox.readthedocs.io/en/latest/) is installed. Install it with `pip install tox`, or (if pip was installed with sudo) `sudo pip install tox`.

To run the tests, simply run `tox -v`.

### Simulate Kafka locally

To ease local debugging, you can start navtiming locally without Kafka. Use the `--kafka-fixture` option to instead read lines of JSON from a static file. In this mode, it will still formulate Statsd messages and expose a Prometheus client.

```
$ tox -v
$ .tox/py3/bin/navtiming --dry-run --kafka-fixture ./events.json
```

## Configuration

navtiming.py can be configured either via an .ini file in the base of the repository, via command line, or a combination of the two.  If a configuration variable is provided in both the config.ini file and on the command line, the command line value is the one that is used.

In general, the configs that are used will be a merge of the values in the section [defaults] with the values in the section named after the wikimedia-cluster.  For testing, it's suggested that you set this value to "local".

## Contributing

This repository is maintained on [Gerrit](https://gerrit.wikimedia.org/g/performance/navtiming/). See https://www.mediawiki.org/wiki/Developer_access for how to submit patches.

Submit bug reports or feature requests to [Phabricator](https://phabricator.wikimedia.org/maniphest/task/edit/form/1/?project=mediawiki-extensions-navigationtiming,performance-team).

This project is looking for a team to maintain it.
