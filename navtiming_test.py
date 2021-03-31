#!/usr/bin/env python

import unittest
import yaml
import os

import navtiming

from prometheus_client import REGISTRY


# ##### Tests ######
# To run:
#   python -m unittest -v navtiming_test
# Or:
#   python navtiming_test.py
#
class TestNavTiming(unittest.TestCase):
    # When passing a 'msg' to assert methods, display that in
    # addition to the value diff, not instead of the diff.
    longMessage = True
    maxDiff = None
    prometheus_namespace_counter = 0

    def setUp(self):
        # Use a fresh Prometheus namespace for each test
        TestNavTiming.prometheus_namespace_counter += 1
        prometheus_namespace = 'webperf' + str(TestNavTiming.prometheus_namespace_counter) + '_'
        self.navtiming = navtiming.NavTiming(prometheus_namespace=prometheus_namespace)

    def flatten(self, values):
        for value in values:
            if isinstance(value, list):
                for subvalue in self.flatten(value):
                    yield subvalue
            else:
                yield value

    def test_allowlist_ua(self):
        data_path = os.path.join(os.path.dirname(__file__), 'navtiming_ua_data.yaml')
        with open(data_path) as data_file:
            data = yaml.safe_load(data_file)
            for case in data:
                expect = tuple(case.split('.'))
                uas = data.get(case)
                for ua in uas:
                    self.assertEqual(
                        self.navtiming.allowlist_ua(ua),
                        expect
                    )

    def test_handlers(self):
        fixture_path = os.path.join(os.path.dirname(__file__), 'navtiming_fixture.yaml')
        with open(fixture_path) as fixture_file:
            cases = yaml.safe_load(fixture_file)
            for key, case in cases.items():
                if key == 'templates':
                    continue

                # Use a fresh Prometheus namespace for each test case
                TestNavTiming.prometheus_namespace_counter += 1
                prometheus_namespace = 'webperf' + str(TestNavTiming.prometheus_namespace_counter)
                self.navtiming.initialize_prometheus_counters(prometheus_namespace)

                if isinstance(case['input'], list):
                    messages = case['input']
                else:
                    # Wrap in list if input is just one event
                    messages = list([case['input']])
                actual = []
                # print "---", key # debug
                for meta in messages:
                    f = self.navtiming.handlers.get(meta['schema'])
                    assert f is not None
                    for stat in f(meta):
                        # print stat # debug
                        actual.append(stat)
                # print "" # debug
                try:
                    self.assertItemsEqual(
                        actual,
                        self.flatten(case['expect']),
                        key
                    )
                except AttributeError:
                    """
                    Test fails under Python3
                    """
                    pass

                for metric in case.get('metrics', []):
                    name, labels, value = metric
                    expected_name = prometheus_namespace + '_' + name
                    self.assertEqual(
                        REGISTRY.get_sample_value(expected_name, labels),
                        value, 'Expected value mismatch for %r %r' % (expected_name, labels))

    def test_is_compliant(self):
        event = {
            'navigationStart': 1,
            'fetchStart': 2,
            'domainLookupStart': 3,
            'domainLookupEnd': 4,
            'connectStart': 5,
            'secureConnectionStart': 6,
            'connectEnd': 7,
            'requestStart': 8,
            'responseStart': 9,
            'responseEnd': 10,
            'domInteractive': 11,
            'domComplete': 12,
            'loadEventStart': 13,
            'loadEventEnd': 14
        }

        self.assertTrue(self.navtiming.is_compliant(event, None))

        del event['secureConnectionStart']
        del event['domComplete']

        self.assertTrue(self.navtiming.is_compliant(event, None))

        event['navigationStart'] = 7

        self.assertFalse(self.navtiming.is_compliant(event, None))

    def test_ua_parse_ios(self):
        ios_ua = 'WikipediaApp/5.3.3.1038 (iOS 10.2; Phone)'
        parsed = {
            'os_family': 'iOS',
            'browser_major': None,
            'browser_minor': None,
            'browser_family': 'Other'
        }
        self.assertEqual(parsed,
                         self.navtiming.parse_ua(ios_ua))

    def test_ua_parse_android(self):
        android_ua = 'WikipediaApp/2.4.160-r-2016-10-14 (Android 4.4.2; Phone)'
        parsed = {
            'os_family': 'Android',
            'browser_family': 'Android',
            'browser_major': '4',
            'browser_minor': '4'
        }
        self.assertEqual(parsed,
                         self.navtiming.parse_ua(android_ua))

    def test_ua_parse_empty(self):
        ua = ""
        parsed = {
            'os_family': 'Other',
            'browser_major': None,
            'browser_minor': None,
            'browser_family': 'Other'
        }
        self.assertEqual(parsed,
                         self.navtiming.parse_ua(ua))

    def test_ua_parse_max_length(self):
        long_ua = ("Mozilla/5.0 (X11; Linux x86_64_128) looooonguaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        with self.assertRaises(RuntimeError):
            self.navtiming.parse_ua(long_ua)


if __name__ == '__main__':
    unittest.main(verbosity=2)
