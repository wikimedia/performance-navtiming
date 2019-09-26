# navtiming

This is a [performance team](https://phabricator.wikimedia.org/tag/performance-team/) utility that receives Navigation Timing beacons from clients, parses out metrics, and submits them to a statsd server.

Code is hosted in [Gerrit](https://github.com/wikimedia/performance-navtiming), please see https://www.mediawiki.org/wiki/Developer_access for information about access.

Python 2.7 or later is required.

## Testing

1. Ensure [pip](https://pip.pypa.io/en/stable/installing/) is installed. If your system has Python installed without pip (such as on macOS), you can either install it with [EasyInstall](https://setuptools.readthedocs.io/en/latest/easy_install.html) using `sudo easy_install pip`, or choose to install Python locally from Homebrew (which comes with pip).
2. Ensure [tox](https://tox.readthedocs.io/en/latest/) is installed. Install it with `pip install tox`, or (if pip was installed with sudo) `sudo pip install tox`.

To run the tests, simply run `tox -v`.

## Configuration
navtiming.py can be configured either via an .ini file in the base of the repository, or via command line, or a combination of the two.  If a configuration variable is provided in both the config.ini file and on the command line, the command line value is the one that is used.

In general, the configs that are used will be a merge of the values in the section [defaults] with the values in the section named after the wikimedia-cluster.  For testing, it's suggested that you set this value to "local".
