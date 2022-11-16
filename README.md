# navtiming

The navtiming service processes web beacons containing performance data from web browsers, and submits
aggregate metrics to Prometheus and Statsd.

Python 2.7 or 3.10 or later is required.

## Testing

1. Ensure [pip](https://pip.pypa.io/en/stable/installing/) is installed. If your system has Python installed without pip (such as on macOS), you can either install it with [EasyInstall](https://setuptools.readthedocs.io/en/latest/easy_install.html) using `sudo easy_install pip`, or choose to install Python locally from Homebrew (which comes with pip).
2. Ensure [tox](https://tox.readthedocs.io/en/latest/) is installed. Install it with `pip install tox`, or (if pip was installed with sudo) `sudo pip install tox`.

To run the tests, simply run `tox -v`.

## Configuration

navtiming.py can be configured either via an .ini file in the base of the repository, via command line, or a combination of the two.  If a configuration variable is provided in both the config.ini file and on the command line, the command line value is the one that is used.

In general, the configs that are used will be a merge of the values in the section [defaults] with the values in the section named after the wikimedia-cluster.  For testing, it's suggested that you set this value to "local".

## Contributing

This repository is maintained on [Gerrit](https://gerrit.wikimedia.org/g/performance/navtiming/). See https://www.mediawiki.org/wiki/Developer_access for how to submit patches.

Submit bug reports or feature requests to [Phabricator](https://phabricator.wikimedia.org/maniphest/task/edit/form/1/?project=mediawiki-extensions-navigationtiming,performance-team).

This project is maintained by the [Performance Team](https://www.mediawiki.org/wiki/Performance_Team).
