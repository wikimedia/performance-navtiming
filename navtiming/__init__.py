#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import etcd
import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import logging
import socket
import time

from prometheus_client import start_http_server, Counter, Gauge, Histogram
from ua_parser import user_agent_parser


class NavTiming(object):
    def __init__(self,
                 kafka_brokers=None, kafka_security_protocol='PLAINTEXT',
                 kafka_ssl_cafile=None, kafka_consumer_group='navtiming',
                 kafka_fixture=None,
                 statsd_host='localhost', statsd_port=8125,
                 datacenter=None,
                 etcd_domain=None, etcd_path=None, etcd_refresh=10,
                 prometheus_namespace='webperf',
                 verbose=False,
                 dry_run=False):
        if kafka_brokers is None:
            if kafka_security_protocol in ('SSL', 'SASL_SSL'):
                self.kafka_brokers = ('localhost:9093',)
            else:
                self.kafka_brokers = ('localhost:9092',)
        else:
            self.kafka_brokers = kafka_brokers
        self.kafka_consumer_group = kafka_consumer_group
        self.kafka_security_protocol = kafka_security_protocol
        self.kafka_ssl_cafile = kafka_ssl_cafile
        self.kafka_fixture = kafka_fixture
        self.statsd_host = statsd_host
        self.statsd_port = statsd_port
        self.verbose = verbose
        self.dry_run = dry_run

        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] (%(funcName)s:%(lineno)d) %(msg)s')
        formatter.converter = time.gmtime
        ch.setFormatter(formatter)
        self.log.addHandler(ch)

        # Set up etcd, if needed, and establish whether this is a session leader
        self.master = True
        self.master_last_updated = 0
        self.datacenter = datacenter
        self.etcd_path = etcd_path
        self.etcd_refresh = etcd_refresh
        if datacenter is not None and etcd_domain is not None and etcd_path is not None:
            self.log.info('Using etcd to check whether {} is master'.format(self.datacenter))
            self.etcd = etcd.Client(srv_domain=etcd_domain, protocol='https',
                                    allow_reconnect=True)
        else:
            self.etcd = None

        self.statsd_addr = self.statsd_host, self.statsd_port
        self.statsd_sock = None

        self.handlers = {
            'NavigationTiming': self.handle_navigation_timing,
            'SaveTiming': self.handle_save_timing,
            'QuickSurveysResponses': self.handle_quick_surveys_responses,
            'QuickSurveyInitiation': self.handle_quick_survey_initiation,
            'PaintTiming': self.handle_paint_timing,
            'CpuBenchmark': self.handle_cpu_benchmark
        }

        # Mapping of continent names to ISO 3166 country codes.
        # Originally from https://dev.maxmind.com/geoip/legacy/codes/country_continent/.
        # See also:
        # https://en.wikipedia.org/wiki/List_of_sovereign_states_and_dependent_territories_by_continent_(data_file)
        # Antarctica excluded on account of its low population.
        self.iso_3166_by_continent = {
            'Africa': [
                'AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ',
                'DZ', 'EG', 'EH', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE',
                'KM', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA',
                'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SH', 'SL', 'SN', 'SO', 'SS', 'ST',
                'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'YT', 'ZA', 'ZM', 'ZW'
            ],
            'Asia': [
                'AE', 'AF', 'AM', 'AP', 'AZ', 'BD', 'BH', 'BN', 'BT', 'CC', 'CN', 'CX',
                'CY', 'GE', 'HK', 'ID', 'IL', 'IN', 'IO', 'IQ', 'IR', 'JO', 'JP', 'KG',
                'KH', 'KP', 'KR', 'KW', 'KZ', 'LA', 'LB', 'LK', 'MM', 'MN', 'MO', 'MV',
                'MY', 'NP', 'OM', 'PH', 'PK', 'PS', 'QA', 'SA', 'SG', 'SY', 'TH', 'TJ',
                'TL', 'TM', 'TW', 'UZ', 'VN', 'YE'
            ],
            'Europe': [
                'AD', 'AL', 'AT', 'AX', 'BA', 'BE', 'BG', 'BY', 'CH', 'CZ', 'DE', 'DK',
                'EE', 'ES', 'EU', 'FI', 'FO', 'FR', 'FX', 'GB', 'GG', 'GI', 'GR', 'HR',
                'HU', 'IE', 'IM', 'IS', 'IT', 'JE', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD',
                'ME', 'MK', 'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'RU', 'SE', 'SI',
                'SJ', 'SK', 'SM', 'TR', 'UA', 'VA', 'XK'
            ],
            'North America': [
                'AG', 'AI', 'AN', 'AW', 'BB', 'BL', 'BM', 'BS', 'BZ', 'CA', 'CR', 'CU',
                'DM', 'DO', 'GD', 'GL', 'GP', 'GT', 'HN', 'HT', 'JM', 'KN', 'KY', 'LC',
                'MF', 'MQ', 'MS', 'MX', 'NI', 'PA', 'PM', 'PR', 'SV', 'TC', 'TT', 'US',
                'VC', 'VG', 'VI'
            ],
            'Oceania': [
                'AS', 'AU', 'CK', 'FJ', 'FM', 'GU', 'KI', 'MH', 'MP', 'NC', 'NF', 'NR',
                'NU', 'NZ', 'PF', 'PG', 'PN', 'PW', 'SB', 'TK', 'TO', 'TV', 'UM', 'VU',
                'WF', 'WS'
            ],
            'South America': [
                'AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'FK', 'GF', 'GY', 'PE', 'PY', 'SR',
                'UY', 'VE'
            ]
        }
        self.iso_3166_to_continent = {}
        for continent, countries in self.iso_3166_by_continent.items():
            for country in countries:
                self.iso_3166_to_continent[country] = continent

        # For this shortlist of ISO ISO 3166-1 country codes, we create an additional
        # copy of the Navigation Timing data in Graphite by country.
        # <https://grafana.wikimedia.org/dashboard/db/navtiming-count-by-country>
        #
        # This list is based on the top 40 most populous countries.
        # <https://en.wikipedia.org/wiki/List_of_countries_by_population_%28United_Nations%29>
        #
        # 1. China, 2. India, 3. United States, 4. Indonesia, 5. Pakistan, 6. Brazil, 7. Nigeria,
        # 8. Bangladesh, 9. Russia, 10. Mexico, 11. Japan, 12. Ethiopia, 13. Philippines,
        # 14. Egypt, 15. Vietnam, 16. Democratic Republic of the Congo, 17. Germany, 18. Turkey,
        # 19. Iran, 20. Thailand, 21. United Kingdom, 22. France, 23. Italy, 24. South Africa,
        # 25. Tanzania, 26. Myanmar, 27. Kenya, 28. South Korea, 29. Colombia, 30. Spain,
        # 31. Argentina, 32. Uganda, 33. Ukraine, 34. Algeria, 35. Sudan, 36. Iraq,
        # 37. Afghanistan, 38. Poland, 39. Canada, 40. Morocco
        #
        # This list is reduced to those from which enough NavigationTiming events are received
        # for a per-minute aggregate and percectile to be useful. Currently cut-off is set at
        # 2.4% from a 24 hour window, where 2.4% maps to 4-7 page view samples per minute.
        #
        # The original unfiltered data from EventLogging is privately available
        # via the Analytics infrastructure (EventLogging DB).
        # - <https://wikitech.wikimedia.org/wiki/EventLogging>
        # - <https://wikitech.wikimedia.org/wiki/EventLogging#Accessing_data>
        # - <https://wikitech.wikimedia.org/wiki/Analytics/Data_access#Access_Groups>
        self.iso_3166_whitelist = {
            # Annotated based on results from this Hive query:
            # > SELECT COUNT(*), event.originCountry FROM navigationtiming WHERE year=2019
            #   AND month=8 AND day=4 AND event.isOversample=0 GROUP BY event.originCountry;
            # List updated late 2022 to include three more countries to make the list a little
            # less Western-centrism, countries from
            # https://stats.wikimedia.org/#/all-projects/reading/page-views-by-country
            'BR': 'Brazil',          # 9. BR 2.4%
            'CA': 'Canada',          # 10. CA 2.4%
            'DE': 'Germany',         # 4. DE 5.6%
            'EG': 'Egypt',
            'FR': 'France',          # 6. FR 3.9%
            'GB': 'United Kingdom',  # 3. GB 4.7%
            'IN': 'India',           # 5. IN 5.4%
            'IR': 'Iran',
            'IT': 'Italy',           # 7. IT 3.7%
            'JP': 'Japan',           # 2. JP 8.9%
            'RU': 'Russia',          # 8. RU 2.9%
            'US': 'United States',   # 1. US 20.7%
            'ZA': 'South Africa',
        }
        # The list of wikis in the groups is non-exhaustive
        self.group_mapping = {
            'commonswiki': 'group1',   # commons.wikimedia.org
            'enwiktionary': 'group1',  # en.wiktionary.org
            'frwiktionary': 'group1',  # fr.wiktionary.org
            'wikidatawiki': 'group1',  # www.wikidata.org
            'cawiki': 'group1',        # ca.wikipedia.org
            'enwiki': 'group2',        # en.wikipedia.org
            'frwiki': 'group2',        # fr.wikipedia.org
            'ruwiki': 'group2',        # ru.wikipedia.org
        }

        self.namespace_mapping = {
            0: 'main',                 # "Real" content articles.
        }

        # We reducae cardinality in Prometheus and when we remove Graphite
        # we can do this directly in the ua logic
        self.browser_family_mapping = {
            'Chrome': 'Chrome',
            'Chrome_Mobile': 'Chrome',
            'Chrome_Mobile_iOS': 'iOS_other',
            'Chromium': 'Chrome',
            'Safari': 'Safari',
            'Mobile_Safari': 'Safari',
            'Firefox': 'Firefox',
            'Firefox_mobile': 'Firefox',
            'Edge': 'Edge',
            'iOS_other': 'iOS_other'
        }

        # Define which metrics we take from the Navigation Timing event (+ deltas) and
        # send to Prometheus
        self.prometheus_metrics_mapping = {
            # 'mediaWikiLoadEnd': 'usertiming_mediawikiloadend_seconds',
            # 'domInteractive': 'navigationtiming_dominteractive_seconds',
            'loadEventEnd': 'navigationtiming_loadeventend_seconds',
            'responseStart': 'navigationtiming_responsestart_seconds',
            # 'tcp': 'navigationtimingdelta_tcp_seconds',
            # 'onLoad': 'navigationtimingdelta_onload_seconds',
            # 'dns': 'navigationtimingdelta_dns_seconds',
            # 'redirect': 'navigationtimingdelta_redirect_seconds'
        }

        # Two level of buckets, one for the small/early ones and one for late ones
        self.navtiming_low_buckets = [.01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5]
        self.navtiming_high_buckets = [0.1, 0.3, 0.5, 0.7, 0.8, 1, 2, 3, 5, 10]

        self.prometheus_bucket_mapping = {
            # 'usertiming_mediawikiloadend_seconds': self.navtiming_high_buckets,
            # 'navigationtiming_dominteractive_seconds': self.navtiming_high_buckets,
            'navigationtiming_loadeventend_seconds': self.navtiming_high_buckets,
            'navigationtiming_responsestart_seconds': self.navtiming_low_buckets,
            # 'navigationtimingdelta_tcp_seconds': self.navtiming_low_buckets,
            # 'navigationtimingdelta_onload_seconds': self.navtiming_low_buckets,
            # 'navigationtimingdelta_dns_seconds': self.navtiming_low_buckets,
            # 'navigationtimingdelta_redirect_seconds': self.navtiming_low_buckets
        }

        self.initialize_prometheus_counters(prometheus_namespace)

    def initialize_prometheus_counters(self, namespace):
        self.prometheus_counters = {}

        # Generic
        self.prometheus_counters['consumed_messages'] = \
            Counter('consumed_messages', 'Messages consumed from Kafka', namespace=namespace)
        self.prometheus_counters['handled_messages'] = \
            Counter('handled_messages', 'Messages handled', ['schema'], namespace=namespace)
        self.prometheus_counters['latest_handled_time_seconds'] = \
            Gauge('latest_handled_time_seconds', 'UNIX timestamp of most recent message',
                  ['schema'], namespace=namespace)
        self.prometheus_counters['errors'] = \
            Counter('errors', 'Unhandled exceptions while processing', namespace=namespace)

        # Performance Survey
        self.prometheus_counters['performance_survey_responses'] = \
            Counter('performance_survey_responses', 'Performance survey responses',
                    ['wiki', 'response'], namespace=namespace)
        self.prometheus_counters['performance_survey_initiations'] = \
            Counter('performance_survey_initiations', 'Performance survey initiations',
                    ['wiki', 'event'], namespace=namespace)

        # Validation
        self.prometheus_counters['painttiming_invalid_events'] = \
            Counter('painttiming_invalid_events', 'Invalid data found when processing PaintTiming',
                    namespace=namespace)
        self.prometheus_counters['navtiming_invalid_events'] = \
            Counter('navtiming_invalid_events', 'Invalid data found when processing NavTiming',
                    namespace=namespace)
        self.prometheus_counters['savetiming_invalid_events'] = \
            Counter('savetiming_invalid_events', 'Invalid data found when processing saveTiming',
                    namespace=namespace)

        # Navigation Timing and Paint Timing
        #
        # We use <=10 buckets per metric, and generally prefer to use the same buckets
        # for metrics that happen close to each other in the page load cycle,
        # to make visualisations and quantiles more consistently comparable
        # and give them the same kind of accuracy and breakdown.
        #
        # Early stage:
        # * responseStart     p50-p95 is 0.2s-1.2s with p99 at 3s (Dec 2022)
        #
        # Late stage:
        # * domInteractive    p50-p95 is 0.4s-2.5s with p99 at 20s (Nov 2022)
        # * loadEventEnd      p50-p95 is 0.7s-3.7s with p99 at 30s (Nov 2022)
        # * firstContentPaint p50-p95 is 0.5s-3.0s with p99 at 50s (Nov 2022)

        # The labels we want to use for our navigation timing data that is collected through
        # the Navigation Timing extension and for paint timings.
        navigation_timing_labels = [
            'mw_auth', 'geo_country', 'geo_continent', 'ua_family', 'is_oversample', 'mw_action',
            'mw_namespace', 'mw_group', 'mw_skin']

        self.prometheus_counters['navtiming_responsestart_by_cache_host_seconds'] = \
            Histogram('navtiming_responsestart_by_cache_host_seconds',
                      'responseStart from the Navigation Timing API',
                      ['cache_host', 'cache_response_type'],
                      buckets=self.navtiming_low_buckets,
                      namespace=namespace)
        self.prometheus_counters['painttiming_firstcontentfulpaint_seconds'] = \
            Histogram('painttiming_firstcontentfulpaint_seconds',
                      'first-contentful-paint from the Paint Timing API',
                      navigation_timing_labels,
                      buckets=self.navtiming_high_buckets,
                      namespace=namespace)
        self.prometheus_counters['painttiming_largestcontentfulpaint_seconds'] = \
            Histogram('painttiming_largestcontentfulpaint_seconds',
                      'Largest contentful paint from the largest contentful paint API',
                      navigation_timing_labels,
                      buckets=self.navtiming_high_buckets,
                      namespace=namespace)
        self.prometheus_counters['cumulativelayoutshift_score'] = \
            Histogram('cumulativelayoutshift_score',
                      'Cumulative layout shift from the layout shift API',
                      navigation_timing_labels,
                      buckets=[0.05, 0.1, 0.2, 0.25, 0.3, 0.5, 1.0],
                      namespace=namespace)
        self.prometheus_counters['cpubenchmark_seconds'] = \
            Histogram('cpubenchmark_seconds', 'CPU benchmarking data from CpuBenchmark schema',
                      ['battery_level', 'ua_family', 'origin_country', 'is_oversample'],
                      # Most observed CPU benchmark times are between 50ms and 500ms
                      buckets=[0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0],
                      namespace=namespace)
        # Navigation timing, deltas and user timings
        for name, prometheus_name in self.prometheus_metrics_mapping.items():
            self.prometheus_counters[prometheus_name] = \
                Histogram(
                        prometheus_name,
                        'Metric ' + name,
                        navigation_timing_labels,
                        buckets=self.prometheus_bucket_mapping[prometheus_name],
                        namespace=namespace)

    def wiki_to_group(self, wiki):
        return self.group_mapping.get(wiki, 'other')

    def is_master(self):
        if self.etcd is None:
            return True

        if time.time() - self.master_last_updated < self.etcd_refresh:
            # Whether this is the master data center was checked no more than
            # etcd_refresh seconds ago
            return self.master

        # Update the last_update timestamp whether success or no -- don't want
        # to pummel etcd if we're not able to update, and multiple instances
        # writing wouldn't be that big a deal
        self.master_last_updated = time.time()
        try:
            # >>> client.get('/conftool/v1/mediawiki-config/common/WMFMasterDatacenter').value
            # u'{"val": "eqiad"}'
            master_datacenter = json.loads(self.etcd.get(self.etcd_path).value)['val']
            if master_datacenter == self.datacenter and not self.master:
                self.log.info('{} is the master datacenter, going live'.format(
                                self.datacenter))
                self.master = True
            elif master_datacenter != self.datacenter and self.master:
                self.log.info('{} is not the master datacenter, disabling consumer'.format(
                                self.datacenter))
                self.master = False
            else:
                self.log.debug('{} was already the {} datacenter'.format(
                                self.datacenter,
                                'master' if master_datacenter == self.datacenter else 'secondary'))
        except KeyError:
            self.log.warning('etcd key {} may be malformed (KeyError raised)'.format(self.etcd_path))
        except etcd.EtcdKeyNotFound:
            self.log.warning('etcd key {} not found'.format(self.etcd_path))
        return self.master

    def parse_ua(self, user_agent, max_length=800):
        """
        Returns a dict containing the parsed User Agent data
        from a request's UA string. Uses the following format:
        {
            "device_family": "Other",
            "browser_family": "IE",
            "browser_major": "11",
            "browser_major": "0",
            "os_family": "Windows Vista",
            "os_major": null,
            "os_minor": null,
            "wmf_app_version": "-"
        }
        App version in user agents is parsed as follows:
        WikipediaApp/5.3.1.1011 (iOS 10.0.2; Phone)
        "wmf_app_version":"5.3.1.1011"
        WikipediaApp/2.4.160-r-2016-10-14 (Android 4.4.2; Phone) Google Play
        "wmf_app_version":"2.4.160-r-2016-10-14"
        """
        if len(user_agent) > max_length:
            raise RuntimeError("User Agent string length ({}) longer "
                               "than the allowed {} chars"
                               .format(len(user_agent), max_length))
        formatted_ua = {}
        parsed_ua = user_agent_parser.Parse(user_agent)
        formatted_ua['browser_family'] = parsed_ua['user_agent']['family']
        formatted_ua['browser_major'] = parsed_ua['user_agent']['major']
        formatted_ua['browser_minor'] = parsed_ua['user_agent']['minor']
        formatted_ua['os_family'] = parsed_ua['os']['family']

        return formatted_ua

    # Only return a small subset of browsers, this to avoid arbitrary growth
    # in Graphite with low-sampled properties that are not useful
    # (which has lots of other negative side effects too).
    # Anything not in the allowlist should go into "Other" instead.
    def allowlist_ua(self, ua):
        """Return a tuple of browser_family and browser_major, or None.

        Can parse a raw user agent or a json object alredy digested by ua-parser

        Inspired by https://github.com/ua-parser/uap-core

        - Add unit test with sample user agent string for each match.
        - Must return a string in form "<browser_family>.<browser_major>".
        - Use the same family name as ua-parser.
        - Ensure version number match doesn't contain dots (or transform them).

        """
        # If already a dict, then this is a digested user agent.
        if isinstance(ua, dict):
            return self.allowlist_ua_obj(ua)

        # Else it's a json-string of the digested user agent.
        return self.allowlist_ua_obj(json.loads(ua))

    def allowlist_ua_obj(self, ua_obj):
        """
        Parses user agent digested by ua-parser
        Note that only browser major is reported
        """
        browser_family = 'Other'
        version = ua_obj['browser_major']

        # Chrome for iOS
        if ua_obj['browser_family'] == 'Chrome Mobile iOS' and ua_obj['os_family'] == 'iOS':
            browser_family = 'Chrome_Mobile_iOS'

        # Mobile Safari on iOS
        elif ua_obj['browser_family'] == 'Mobile Safari' and ua_obj['os_family'] == 'iOS':
            browser_family = 'Mobile_Safari'
            version = "{0}_{1}".format(
                ua_obj['browser_major'], ua_obj['browser_minor'])

        # iOS WebView
        elif ua_obj['os_family'] == 'iOS' and ua_obj['browser_family'] == 'Mobile Safari UIWebView':
            browser_family = 'iOS_WebView'

        # Opera >=14 for Android (WebKit-based)
        elif ua_obj['browser_family'] == 'Opera Mobile' and ua_obj['os_family'] == 'Android':
            browser_family = 'Opera_Mobile'

        # Android browser (pre Android 4.4)
        elif ua_obj['browser_family'] == 'Android' and ua_obj['os_family'] == 'Android':
            browser_family = 'Android'

        # Chrome for Android
        elif ua_obj['browser_family'] == 'Chrome Mobile' and ua_obj['os_family'] == 'Android':
            browser_family = 'Chrome_Mobile'

        # Opera >= 15 (Desktop)
        # todo assuming all operas not iOS or Android are desktop
        elif (ua_obj['browser_family'] == 'Opera' and int(ua_obj['browser_major']) >= 15
                and ua_obj['os_family'] != 'Android' and ua_obj['os_family'] != 'iOS'):
            browser_family = 'Opera'

        # Internet Explorer 11
        elif ua_obj['browser_family'] == 'IE' and ua_obj['browser_major'] == '11':
            browser_family = 'MSIE'

        # Internet Explorer <= 10
        elif ua_obj['browser_family'] == 'IE' and int(ua_obj['browser_major']) < 11:
            browser_family = 'MSIE'

        # Firefox for Android
        elif ua_obj['browser_family'] == 'Firefox Mobile' and ua_obj['os_family'] == 'Android':
            browser_family = 'Firefox_Mobile'

        # Firefox (Desktop)
        elif ua_obj['browser_family'] == 'Firefox':
            browser_family = 'Firefox'

        # Microsoft Edge (but note, not 'Edge Mobile')
        elif ua_obj['browser_family'] == 'Edge':
            browser_family = 'Edge'

        # Chrome/Chromium
        elif ua_obj['browser_family'] == 'Chrome' or ua_obj['browser_family'] == 'Chromium':
            browser_family = ua_obj['browser_family']

        # Safari (Desktop)
        elif ua_obj['browser_family'] == 'Safari' and ua_obj['os_family'] != 'iOS':
            browser_family = 'Safari'

        # Misc iOS
        elif ua_obj['os_family'] == 'iOS':
            browser_family = 'iOS_other'

        # Opera (Desktop)
        elif ua_obj['browser_family'] == 'Opera':
            browser_family = 'Opera'

        # 'Other' should report no version
        else:
            browser_family == 'Other'
            version = '_'

        # Catch partial ua-parser result (T176149)
        if version is None:
            browser_family = 'Other'
            version = '_'

        return (browser_family, version)

    def dispatch_stat(self, stat):
        if self.statsd_sock and self.statsd_addr and not self.dry_run:
            self.statsd_sock.sendto(stat, self.statsd_addr)
        else:
            self.log.debug(stat)

    def make_stat(self, *args, value):
        """
        Create a statsd packet for adding a measure to a Timing metric
        """
        name = '.'.join(arg.replace(' ', '_') for arg in args)
        stat = '%s:%s|ms' % (name, value)
        return stat.encode('utf-8')

    def make_count(self, *args):
        """
        Create a statsd packet for incrementing a Counter metric
        """
        value = 1
        name = '.'.join(arg.replace(' ', '_') for arg in args)
        stat = '%s:%s|c' % (name, value)
        return stat.encode('utf-8')

    def is_sane_navtiming2(self, value):
        return isinstance(value, int) and value >= 0

    def is_sane_cumulative_layout_shift(self, value):
        return isinstance(value, float) and value >= 0

    #
    # Verify that the values in a NavTiming event are in order.
    #
    # Metrics may be missing or 0 if they're not implemented by the browser, or are
    # not relevant to the current page
    #
    # return {boolean}
    #
    def is_compliant(self, event, userAgent):
        sequences = [
            [
                'navigationStart',
                'fetchStart',
                'domainLookupStart',
                'domainLookupEnd',
                'connectStart',
                'connectEnd',
                'requestStart',
                'responseStart',
                'responseEnd',
                'domInteractive',
                'domComplete',
                'loadEventStart',
                'loadEventEnd'
            ], [
                'secureConnectionStart',
                'requestStart'
            ]
        ]

        for sequence in sequences:
            previous = 0
            for metric in sequence:
                if metric in event and event[metric] > 0:
                    if event[metric] < previous:
                        self.log.info('Discarding event because {} is out of order [{}]'.format(
                            metric, userAgent))
                        return False
                    previous = event[metric]

        return True

    def handle_save_timing(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        duration = event.get('saveTiming')
        group = self.wiki_to_group(wiki)
        if duration is not None:
            yield self.make_stat('mw.performance.save', value=duration)
            yield self.make_stat('mw.performance.save_by_group', group, value=duration)
        else:
            self.prometheus_counters['savetiming_invalid_events'].inc()

    def handle_quick_surveys_responses(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        surveyCodeName = event.get('surveyCodeName')
        surveyResponseValue = event.get('surveyResponseValue')

        if surveyCodeName != 'perceived-performance-survey' or not wiki or not surveyResponseValue:
            return

        # Example: ext-quicksurveys-example-internal-survey-answer-neutral
        response = surveyResponseValue[48:]

        self.prometheus_counters['performance_survey_responses'].labels(wiki, response).inc()

        yield self.make_count('performance.survey', wiki, response)

    def handle_quick_survey_initiation(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        surveyCodeName = event.get('surveyCodeName')
        eventName = event.get('eventName')

        if surveyCodeName != 'perceived-performance-survey' or not wiki or not eventName:
            return

        self.prometheus_counters['performance_survey_initiations'].labels(wiki, eventName).inc()

        yield self.make_count('performance.survey_initiation', wiki, eventName)

    def handle_paint_timing(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        group = self.wiki_to_group(wiki)

        try:
            platform, auth, ua, continent, country_name, is_oversample, action, namespace, skin = \
                self.get_navigation_timing_context(meta)
        except Exception:
            return

        ua_family, ua_version = ua
        # Map for Prometheus
        browser_family = self.browser_family_mapping.get(ua_family, 'Other')

        value = event['startTime']

        if event['name'] == 'first-paint':
            metric = 'firstPaint'
        elif event['name'] == 'first-contentful-paint':
            metric = 'firstContentfulPaint'
            self.prometheus_counters['painttiming_firstcontentfulpaint_seconds'].labels(
                auth, country_name, continent, browser_family, is_oversample, action, namespace, group, skin
            ).observe(value / 1000.0)
        else:
            self.prometheus_counters['painttiming_invalid_events'].inc()
            yield self.make_count('eventlogging.client_errors.PaintTiming', 'isValidName')
            return

        if not self.is_sane_navtiming2(value):
            self.prometheus_counters['painttiming_invalid_events'].inc()
            yield self.make_count('eventlogging.client_errors.PaintTiming', 'isSane')
            return

        # PaintTiming is funneled to navtiming2 for backwards compatibility
        for stat in self.make_navigation_timing_stats(
                platform,
                auth,
                ua,
                continent,
                country_name,
                is_oversample,
                metric,
                value):
            yield stat

        yield self.make_count('frontend.painttiming_group', group)

    def handle_cpu_benchmark(self, meta):
        event = meta['event']

        try:
            platform, auth, ua, continent, country_name, is_oversample, action, namespace, skin = \
                self.get_navigation_timing_context(meta)
        except Exception:
            return

        ua_family, _ = ua
        value = event['score']
        if 'batteryLevel' in event:
            if event['batteryLevel'] < 0.33:
                bucketed_battery_level = "low"
            elif event['batteryLevel'] <= 0.67:
                bucketed_battery_level = "medium"
            else:
                bucketed_battery_level = "high"
        else:
            bucketed_battery_level = "unknown"

        self.prometheus_counters['cpubenchmark_seconds'].labels(
            bucketed_battery_level,
            ua_family,
            country_name,
            str(is_oversample),
        ).observe(value / 1000.0)

        # We need at least one yield statement, so that Python will treat this
        # as a generator function.  We don't actually yield any Graphite
        # metrics for this schema, so the yield is unreachable.
        return
        yield

    def get_navigation_timing_context(self, meta):
        event = meta['event']

        if 'mobileMode' in event:
            if event['mobileMode'] == 'stable':
                platform = 'mobile'
            else:
                platform = 'mobile-beta'
        else:
            platform = 'desktop'
        auth = 'anonymous' if event.get('isAnon') else 'authenticated'

        if 'skin' in event:
            skin = event['skin']
        else:
            skin = 'unknown'

        country_code = event.get('originCountry')
        continent = self.iso_3166_to_continent.get(country_code, 'other')
        country_name = self.iso_3166_whitelist.get(country_code, 'other')

        # Handle oversampling
        if 'isOversample' in event and event['isOversample']:
            is_oversample = True

            # decode oversampleReason (string-encoded json)
            try:
                oversample_reasons = json.loads(event['oversampleReason'])
            except ValueError:
                # We know that we're oversampling, but we don't know why.
                # Which means we're not going to aggregate to anywhere useful,
                # so no reason to even make the metrics
                self.log.warning('Invalid oversampleReason value: "{}"'.format(
                    event['oversampleReason']))
                raise
            except Exception:
                # Same case here, but we can't even log anything useful
                self.log.exception('Unknown exception trying to decode oversample reasons')
                raise

            # If we're oversampling by geo, use the country code.  The
            # whitelist is helpful for normal sampling, but defeats the
            # purpose in this case
            #
            # oversample_reasons looks like this (defined in
            #                ext.navigationTiming.js#L417-L435 ):
            #    ['geo:XX', 'ua:Browser Agent String']
            if len([reason for reason in oversample_reasons if reason[0:4] == 'geo:']):
                country_name = country_code
        else:
            # Not an oversample
            is_oversample = False

        # Legacy EventLogging, UA parsing is done for us
        if 'userAgent' in meta:
            ua = self.allowlist_ua(meta['userAgent'])
        # EventGate, we have to parse the UA string ourselves
        elif 'http' in meta:
            try:
                ua = self.allowlist_ua(self.parse_ua(meta['http']['request_headers']['user-agent']))
            except RuntimeError:
                self.log.error('Could not parse UA string: too long. Defaulting to "Other".')
                ua = ('Other', '_')

        if not ua:
            ua = ('Other', '_')

        # Handle action https://www.mediawiki.org/wiki/Manual:$wgActions
        # Only collect views|other
        if 'action' in event:
            if event['action'] != 'view':
                action = 'other'
            else:
                action = 'view'
        else:
            # Special pages do not have an action
            action = 'other'

        # Handle namespaces https://www.mediawiki.org/wiki/Manual:Namespace
        if 'namespaceId' in event:
            # All uneven namespaces over 0 is talk pages
            if event['namespaceId'] % 2 > 0 and event['namespaceId'] > 0:
                namespace = 'talk'
            else:
                namespace = self.namespace_mapping.get(event['namespaceId'], 'other')
        else:
            namespace = 'other'
        return platform, auth, ua, continent, country_name, is_oversample, action, namespace, skin

    def make_navigation_timing_stats(self, platform, auth, ua, continent, country_name, is_oversample, metric, value):
        if is_oversample:
            prefix = 'frontend.navtiming2_oversample'
        else:
            prefix = 'frontend.navtiming2'

        yield self.make_stat(prefix, metric, platform, auth, value=value)
        yield self.make_stat(prefix, metric, platform, 'overall', value=value)
        yield self.make_stat(prefix, metric, 'overall', value=value)
        yield self.make_stat(prefix, metric, 'by_browser', ua[0], ua[1], value=value)
        yield self.make_stat(prefix, metric, 'by_browser', ua[0], 'all', value=value)
        yield self.make_stat(prefix, metric, 'by_continent', continent, value=value)
        yield self.make_stat(prefix, metric, 'by_country', country_name, value=value)

    def handle_navigation_timing(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        group = self.wiki_to_group(wiki)

        try:
            platform, auth, ua, continent, country_name, is_oversample, action, namespace, skin = \
                self.get_navigation_timing_context(meta)
        except Exception:
            self.log.exception('Exception occured in get_navigation_timing_context')
            return

        if not self.is_compliant(event, ua):
            self.prometheus_counters['navtiming_invalid_events'].inc()
            yield self.make_count('eventlogging.client_errors.NavigationTiming', 'nonCompliant')
            return

        metrics_nav2 = {}
        isSane = True

        for metric in (
            'domComplete',
            'domInteractive',
            'firstPaint',
            'loadEventEnd',
            'loadEventStart',
            'mediaWikiLoadEnd',
            'responseStart',
        ):
            if metric in event:
                # The new way is to fetch start as base, so if we got it, rebase
                if 'fetchStart' in event:
                    metrics_nav2[metric] = event[metric] - event['fetchStart']
                else:
                    metrics_nav2[metric] = event[metric]
        # https://www.w3.org/TR/navigation-timing/#process
        # Also see https://www.w3.org/TR/navigation-timing-2/#dfn-domloading for domLoading
        for difference, minuend, subtrahend in (
            ('tcp', 'connectEnd', 'connectStart'),
            ('request', 'responseStart', 'requestStart'),
            ('response', 'responseEnd', 'responseStart'),
            ('processing', 'domComplete', 'responseEnd'),
            ('onLoad', 'loadEventEnd', 'loadEventStart'),
            ('ssl', 'connectEnd', 'secureConnectionStart'),
        ):
            if minuend in event and subtrahend in event:
                metrics_nav2[difference] = event[minuend] - event[subtrahend]

        # Adding the deltas that we have from the extension already
        # We miss out appCache since we don't have domainLookupStart
        if 'dnsLookup' in event:
            metrics_nav2['dns'] = event['dnsLookup']
        if 'unload' in event:
            metrics_nav2['unload'] = event['unload']
        if 'redirecting' in event:
            metrics_nav2['redirect'] = event['redirecting']

        # Cumulative layout shift and largest contentful paint is tagged along with the
        # navigation timing data
        if 'cumulativeLayoutShift' in event:
            metrics_nav2['cumulativeLayoutShift'] = event['cumulativeLayoutShift']
        if 'largestContentfulPaint' in event:
            metrics_nav2['largestContentfulPaint'] = event['largestContentfulPaint']

        # If we got gaps in the Navigation Timing metrics, collect them
        if 'gaps' in event:
            metrics_nav2['gaps'] = event['gaps']

        # If one of the metrics are wrong, don't send them at all
        for metric, value in metrics_nav2.items():
            # CLS score is not int so special handling
            if metric != 'cumulativeLayoutShift':
                isSane = self.is_sane_navtiming2(value)
                if not isSane:
                    break
            else:
                isSane = self.is_sane_cumulative_layout_shift(value)
        # If one of the metrics are under the min then skip it entirely
        if not isSane:
            self.prometheus_counters['navtiming_invalid_events'].inc()
            yield self.make_count('eventlogging.client_errors.NavigationTiming', 'isSane')
        else:
            for metric, value in metrics_nav2.items():
                for stat in self.make_navigation_timing_stats(
                    platform,
                    auth,
                    ua,
                    continent,
                    country_name,
                    is_oversample,
                    metric,
                    value
                ):
                    yield stat

                # Map for Prometheus
                browser_family = self.browser_family_mapping.get(ua[0], 'Other')

                if metric == 'responseStart' and not is_oversample:
                    try:
                        cache_response_type = 'unknown'
                        cache_host = 'unknown'

                        if 'cacheResponseType' in event and len(event['cacheResponseType']):
                            cache_response_type = event['cacheResponseType']

                        if 'cacheHost' in event and len(event['cacheHost']):
                            cache_host = event['cacheHost']

                        self.prometheus_counters['navtiming_responsestart_by_cache_host_seconds'].labels(
                            cache_host, cache_response_type
                        ).observe(value / 1000.0)
                    except (KeyError, ValueError):
                        pass

                if metric == 'cumulativeLayoutShift':
                    self.prometheus_counters['cumulativelayoutshift_score'].labels(
                        auth, country_name, continent, browser_family, is_oversample, action, namespace, group, skin
                    ).observe(value)
                elif metric == 'largestContentfulPaint':
                    self.prometheus_counters['painttiming_largestcontentfulpaint_seconds'].labels(
                        auth, country_name, continent, browser_family, is_oversample, action, namespace, group, skin
                    ).observe(value / 1000.0)
                else:
                    if metric in self.prometheus_metrics_mapping:
                        self.prometheus_counters[self.prometheus_metrics_mapping[metric]].labels(
                            auth, country_name, continent, browser_family, is_oversample, action, namespace, group, skin
                        ).observe(value / 1000.0)

            yield self.make_count('frontend.navtiming_group', group)

    def kafka_return_commit_callback(self):
        # Closure so that log config carries over
        def commit_callback(offsets, response):
            if isinstance(response, Exception):
                self.log.error('Exception trying to commit offsets {}: {}'.format(offsets, response))
            else:
                self.log.debug('Committed offsets [{}]'.format(offsets))
        return commit_callback

    def get_kafka_iterator(self):
        kafka_bootstrap_servers = tuple(self.kafka_brokers)
        kafka_topics = ['eventlogging_' + key for key in self.handlers.keys()]

        if self.kafka_fixture:
            with open(self.kafka_fixture) as fixture_file:
                for line in fixture_file:
                    yield line
            self.log.info('Reached end of kafka fixture file, going to sleep for 1 hour...')
            time.sleep(3600)
            return

        consumer = None
        while True:
            try:
                if not self.is_master():
                    time.sleep(self.etcd_refresh + 1)
                    self.log.info('Checking whether datacenter has been promoted')
                    continue

                self.log.info('Starting Kafka connection to brokers ({})'.format(
                              kafka_bootstrap_servers))
                consumer = KafkaConsumer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    security_protocol=self.kafka_security_protocol,
                    ssl_cafile=self.kafka_ssl_cafile,
                    # WMF Kafka uses the cluster name as CN (instead of hostname).
                    ssl_check_hostname=False,
                    group_id=self.kafka_consumer_group,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    default_offset_commit_callback=self.kafka_return_commit_callback()
                )

                self.log.info('Subscribing to topics: {}'.format(kafka_topics))
                # This is a quirk of the library where this call triggers actually fetching
                # the metadata, whereas partitions_for_topic doesn't
                # https://github.com/dpkp/kafka-python/issues/1774
                consumer.topics()
                assignments = []
                for topic in kafka_topics:
                    self.log.info('Fetching partitions for topic: {}'.format(topic))
                    partitions = consumer.partitions_for_topic(topic)
                    # Safety net in case partitions_for_topic failed,
                    # but shouldn't happen whend primed with .topics()
                    if partitions is None:
                        self.log.info('No partitions found for topic: {}, defaulting to partition 0'.format(topic))
                        assignments.append(TopicPartition(topic, 0))
                    else:
                        for p in partitions:
                            assignments.append(TopicPartition(topic, p))

                self.log.info('Assigning partitions: {}'.format(assignments))
                consumer.assign(assignments)
                consumer.seek_to_end()

                self.log.info('Starting Kafka consumer')
                for message in consumer:
                    # Check whether we should be running
                    if not self.is_master():
                        self.log.info('No longer running in the master datacenter')
                        self.log.info('Stopping Kafka consumer')
                        if consumer is not None:
                            consumer.close()
                        break

                    yield message.value.decode('utf-8')

            except KeyboardInterrupt:
                self.log.info('Stopping the Kafka consumer and shutting down')
                consumer.close()
                break
            except Exception:
                self.prometheus_counters['errors'].inc()
                self.log.exception('Unhandled exception in main loop, restarting consumer')

    def run(self):
        self.statsd_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        for line in self.get_kafka_iterator():
            self.prometheus_counters['consumed_messages'].inc()
            meta = json.loads(line)

            # Canary events are fake events used to monitor the event pipeline
            if 'meta' in meta and 'domain' in meta['meta'] and meta['meta']['domain'] == 'canary':
                self.log.info('Canary event')
                continue

            if 'schema' in meta:
                f = self.handlers.get(meta['schema'])
                if f is not None:
                    self.prometheus_counters['handled_messages'].labels(meta['schema']).inc()
                    self.prometheus_counters['latest_handled_time_seconds'].labels(
                        meta['schema']
                    ).set_to_current_time()
                    for stat in f(meta):
                        self.dispatch_stat(stat)


def main(cluster=None, config=None):
    parsed_configs = {}
    if cluster and config and cluster in config.sections():
        parsed_configs = config[cluster]

    ap = argparse.ArgumentParser(description='NavigationTiming subscriber')

    ka = ap.add_mutually_exclusive_group(required=False if parsed_configs.get('brokers') else True)
    ka.add_argument('--brokers',
                    default=parsed_configs.get('brokers', ''),
                    help='Comma-separated list of kafka brokers')
    ka.add_argument('--kafka-fixture',
                    default=None,
                    help='Filename to simulate a Kafka consumer that reads lines from a fixture')
    ap.add_argument('--security-protocol',
                    default=parsed_configs.get('security_protocol', 'PLAINTEXT'),
                    help='Protocol used to communicate with Kafka brokers. '
                         'Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL. Default: PLAINTEXT')
    ap.add_argument('--ssl-cafile',
                    default=parsed_configs.get('ssl_cafile'),
                    help='Optional filename of certificate authority file to use in certificate verification')
    ap.add_argument('--consumer-group',
                    default=parsed_configs.get('consumer_group', 'navtiming'),
                    help='Consumer group to register with Kafka')
    ap.add_argument('--statsd-host',
                    default=parsed_configs.get('statsd_host', 'localhost'),
                    type=socket.gethostbyname)
    ap.add_argument('--statsd-port',
                    default=int(parsed_configs.get('statsd_port', 8125)),
                    type=int)
    ap.add_argument('--listen',
                    default=parsed_configs.get('listen', ':9230'),
                    type=str,
                    help='Expose Prometheus metrics on this address')
    ap.add_argument('--datacenter',
                    required=False,
                    default=cluster,
                    dest='datacenter',
                    help='Current datacenter (eg, eqiad)')
    ap.add_argument('--etcd-domain',
                    required=False,
                    default=parsed_configs.get('etcd_domain'),
                    dest='etcd_domain',
                    help='Domain to use for etcd srv lookup')
    ap.add_argument('--etcd-path',
                    required=False,
                    default=parsed_configs.get('etcd_path'),
                    dest='etcd_path',
                    help='Where to find the etcd MasterDatacenter value')
    ap.add_argument('--etcd-refresh',
                    required=False,
                    default=float(parsed_configs.get('etcd_refresh', 10)),
                    dest='etcd_refresh',
                    help='Seconds to wait before refreshing etcd')
    ap.add_argument('-v',
                    '--verbose',
                    required=False,
                    default=False,
                    help='Verbose logging',
                    action='store_true')
    ap.add_argument('-n',
                    '--dry-run',
                    dest='dry_run',
                    action='store_true',
                    required=False,
                    default=False,
                    help='Dry-run (don\'t actually submit to statsd)')
    args = ap.parse_args()

    nt = NavTiming(kafka_brokers=args.brokers.split(','),
                   kafka_security_protocol=args.security_protocol,
                   kafka_ssl_cafile=args.ssl_cafile,
                   kafka_consumer_group=args.consumer_group,
                   kafka_fixture=args.kafka_fixture,
                   statsd_host=args.statsd_host,
                   statsd_port=args.statsd_port,
                   datacenter=args.datacenter,
                   etcd_domain=args.etcd_domain,
                   etcd_path=args.etcd_path,
                   etcd_refresh=args.etcd_refresh,
                   verbose=args.verbose,
                   dry_run=args.dry_run)

    metrics_address, metrics_port = args.listen.split(':', 1)
    start_http_server(int(metrics_port), addr=metrics_address)

    nt.run()


if __name__ == '__main__':
    main()
