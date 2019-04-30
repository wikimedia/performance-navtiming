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


class NavTiming(object):
    def __init__(self, brokers=['127.0.0.1:9092'], consumer_group='navtiming',
                 statsd_host='localhost', statsd_port=8125, datacenter=None,
                 etcd_domain=None, etcd_path=None, etcd_refresh=10, verbose=False,
                 dry_run=False):
        self.brokers = brokers
        self.consumer_group = consumer_group
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

        self.addr = self.statsd_host, self.statsd_port
        self.sock = None

        self.handlers = {
            'NavigationTiming': self.handle_navigation_timing,
            'SaveTiming': self.handle_save_timing,
            'QuickSurveysResponses': self.handle_quick_surveys_responses,
            'PaintTiming': self.handle_paint_timing
        }

        # Mapping of continent names to ISO 3166 country codes.
        # From https://dev.maxmind.com/geoip/legacy/codes/country_continent/.
        # Antarctica excluded on account of its low population.
        self.iso_3166_by_continent = {
            'Africa': [
                'AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ',
                'DZ', 'EG', 'EH', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE',
                'KM', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA',
                'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SH', 'SL', 'SN', 'SO', 'ST', 'SZ',
                'TD', 'TG', 'TN', 'TZ', 'UG', 'YT', 'ZA', 'ZM', 'ZW'
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
                'SJ', 'SK', 'SM', 'TR', 'UA', 'VA'
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

        # Shortlist of ISO 3166-1 country codes to create dedicated Graphite metrics
        # for from Navigation Timing. This list is based on the top 40 most populous
        # countries as of 1 January 2016 (83% of the world's population living in
        # these countries) reduced to only those countries from which at least 5
        # Navigation Timing events are received on average every minute.
        #
        # <https://grafana.wikimedia.org/dashboard/db/navtiming-count-by-country>
        #
        # This is for publicly aggregated time series published via Graphite.
        #
        # The original unfiltered data from EventLogging is privately available
        # via the Analytics infrastructure (EventLogging DB).
        # - <https://wikitech.wikimedia.org/wiki/EventLogging>
        # - <https://wikitech.wikimedia.org/wiki/EventLogging#Accessing_data>
        # - <https://wikitech.wikimedia.org/wiki/Analytics/Data_access#Access_Groups>
        self.iso_3166_whitelist = {
            'DE': 'Germany',         # ~9/min
            'FR': 'France',          # ~6/min
            'GB': 'United Kingdom',  # ~8/min
            'IN': 'India',           # ~5/min
            'IT': 'Italy',           # ~6/min
            'JP': 'Japan',           # ~12/min
            'RU': 'Russia',          # ~6/min
            'US': 'United States',   # ~22/min
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

    # Only return the small subset of browsers we whitelisted, this to avoid arbitrary growth
    # in Graphite with low-sampled properties that are not useful
    # (which has lots of other negative side effects too).
    # Anything not in the whitelist should go into "Other" instead.
    def parse_ua(self, ua):
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
            return self.parse_ua_obj(ua)

        # Else it's a json-string of the digested user agent.
        return self.parse_ua_obj(json.loads(ua))

    def parse_ua_obj(self, ua_obj):
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
        if self.sock and self.addr and not self.dry_run:
            self.sock.sendto(stat, self.addr)
        else:
            self.log.info(stat)

    def make_stat(self, *args):
        """
        Create a statsd packet for adding a measure to a Timing metric
        """
        args = list(args)
        value = args.pop()
        name = '.'.join(arg.replace(' ', '_') for arg in args)
        stat = '%s:%s|ms' % (name, value)
        return stat.encode('utf-8')

    def make_count(self, *args):
        """
        Create a statsd packet for incrementing a Counter metric
        """
        args = list(args)
        value = 1
        name = '.'.join(arg.replace(' ', '_') for arg in args)
        stat = '%s:%s|c' % (name, value)
        return stat.encode('utf-8')

    def is_sane_navtiming2(self, value):
        return isinstance(value, int) and value >= 0

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
        if duration is not None:
            yield self.make_stat('mw.performance.save', duration)
            if wiki in self.group_mapping:
                group = self.group_mapping[wiki]
                yield self.make_stat('mw.performance.save_by_group', group, duration)
            else:
                yield self.make_stat('mw.performance.save_by_group.other', duration)

    def handle_quick_surveys_responses(self, meta):
        event = meta['event']
        wiki = meta['wiki']
        surveyCodeName = event.get('surveyCodeName')
        surveyResponseValue = event.get('surveyResponseValue')

        if surveyCodeName != 'perceived-performance-survey' or not wiki or not surveyResponseValue:
            return

        # Example: ext-quicksurveys-example-internal-survey-answer-neutral
        response = surveyResponseValue[48:]

        yield self.make_count('performance.survey', wiki, response)

    def handle_paint_timing(self, meta):
        event = meta['event']
        wiki = meta['wiki']

        try:
            site, auth, ua, continent, country_name, is_oversample = self.get_navigation_timing_context(meta)
        except Exception:
            return

        if event['name'] == 'first-paint':
            metric = 'firstPaint'
        elif event['name'] == 'first-contentful-paint':
            metric = 'firstContentfulPaint'
        else:
            yield self.make_count('eventlogging.client_errors.PaintTiming', 'isValidName')
            return

        value = event['startTime']

        if not self.is_sane_navtiming2(value):
            yield self.make_count('frontend.painttiming_discard', 'isSane')
            return

        # PaintTiming is funneled to navtiming2 for backwards compatibility
        for stat in self.make_navigation_timing_stats(
                site,
                auth,
                ua,
                continent,
                country_name,
                is_oversample,
                metric,
                value):
            yield stat

        if wiki in self.group_mapping:
            group = self.group_mapping[wiki]
            yield self.make_count('frontend.painttiming_group', group)

    def get_navigation_timing_context(self, meta):
        event = meta['event']

        if 'mobileMode' in event:
            if event['mobileMode'] == 'stable':
                site = 'mobile'
            else:
                site = 'mobile-beta'
        else:
            site = 'desktop'
        auth = 'anonymous' if event.get('isAnon') else 'authenticated'

        country_code = event.get('originCountry')
        continent = self.iso_3166_to_continent.get(country_code)
        country_name = self.iso_3166_whitelist.get(country_code)

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

        ua = self.parse_ua(meta['userAgent']) or ('Other', '_')

        return site, auth, ua, continent, country_name, is_oversample

    def make_navigation_timing_stats(self, site, auth, ua, continent, country_name, is_oversample, metric, value):
        if is_oversample:
            prefix = 'frontend.navtiming2_oversample'
        else:
            prefix = 'frontend.navtiming2'

        yield self.make_stat(prefix, metric, site, auth, value)
        yield self.make_stat(prefix, metric, site, 'overall', value)
        yield self.make_stat(prefix, metric, 'overall', value)

        yield self.make_stat(prefix, metric, 'by_browser', ua[0], ua[1], value)
        yield self.make_stat(prefix, metric, 'by_browser', ua[0], 'all', value)

        if continent is not None:
            yield self.make_stat(prefix, metric, 'by_continent', continent, value)

        if country_name is not None:
            yield self.make_stat(prefix, metric, 'by_country', country_name, value)

    def handle_navigation_timing(self, meta):
        event = meta['event']
        wiki = meta['wiki']

        if not self.is_compliant(event, meta['userAgent']):
            yield self.make_count('eventlogging.client_errors.NavigationTiming', 'nonCompliant')
            return

        try:
            site, auth, ua, continent, country_name, is_oversample = self.get_navigation_timing_context(meta)
        except Exception:
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

        # If we got gaps in the Navigation Timing metrics, collect them
        if 'gaps' in event:
            metrics_nav2['gaps'] = event['gaps']

        # If one of the metrics are wrong, don't send them at all
        for metric, value in metrics_nav2.items():
            isSane = self.is_sane_navtiming2(value)
            if not isSane:
                break

        # If one of the metrics are under the min then skip it entirely
        if not isSane:
            yield self.make_count('frontend.navtiming_discard', 'isSane')
        else:
            for metric, value in metrics_nav2.items():
                for stat in self.make_navigation_timing_stats(
                    site,
                    auth,
                    ua,
                    continent,
                    country_name,
                    is_oversample,
                    metric,
                    value
                ):
                    yield stat

            if wiki in self.group_mapping:
                group = self.group_mapping[wiki]
                yield self.make_count('frontend.navtiming_group', group)

    def return_commit_callback(self):
        # Closure so that log config carries over
        def commit_callback(offsets, response):
            if isinstance(response, Exception):
                self.log.error('Exception trying to commit offsets {}: {}'.format(offsets, response))
            else:
                self.log.debug('Committed offsets [{}]'.format(offsets))
        return commit_callback

    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        kafka_bootstrap_servers = tuple(self.brokers)
        kafka_topics = ['eventlogging_' + key for key in self.handlers.keys()]

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
                    group_id=self.consumer_group,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    default_offset_commit_callback=self.return_commit_callback()
                )

                self.log.info('Subscribing to topics: {}'.format(kafka_topics))
                # Force the consumer to go to the most recent message.  Need to
                # assign partitions manually, see https://github.com/dpkp/kafka-python/issues/601
                assignments = []
                for topic in kafka_topics:
                    self.log.info('Fetching partitions for topic: {}'.format(topic))
                    partitions = consumer.partitions_for_topic(topic)

                    if partitions is None:
                        self.log.info('No partitions found for topic: {}, defaulting to partition 0'.format(topic))
                        assignments.append(TopicPartition(topic, 0))
                    else:
                        for p in partitions:
                            assignments.append(TopicPartition(topic, p))

                self.log.info('Assigning partitions: {}'.format(assignments))
                consumer.assign(assignments)
                consumer.seek_to_end()

                self.log.info('Starting NavTiming Kafka consumer.')

                for message in consumer:
                    # Check whether we should be running
                    if not self.is_master():
                        self.log.info('No longer running in the master datacenter')
                        self.log.info('Stopping consuming')
                        if consumer is not None:
                            consumer.close()
                        break
                    meta = json.loads(message.value)
                    if 'schema' in meta:
                        f = self.handlers.get(meta['schema'])
                        if f is not None:
                            for stat in f(meta):
                                self.dispatch_stat(stat)
            except KeyboardInterrupt:
                self.log.info('Stopping the Kafka consumer and shutting down')
                consumer.close()
                break
            except Exception:
                self.log.exception('Unhandled exception in main loop, restarting consumer')


def main(cluster=None, config=None):
    parsed_configs = {}
    if cluster and config and cluster in config.sections():
        parsed_configs = config[cluster]

    ap = argparse.ArgumentParser(description='NavigationTiming subscriber')
    ap.add_argument('--brokers',
                    required=False if parsed_configs.get('brokers') else True,
                    default=parsed_configs.get('brokers'),
                    help='Comma-separated list of kafka brokers')
    ap.add_argument('--consumer-group',
                    required=False if parsed_configs.get('consumer_group') else True,
                    default=parsed_configs.get('consumer_group'),
                    help='Consumer group to register with Kafka')
    ap.add_argument('--statsd-host',
                    default=parsed_configs.get('statsd_host', 'localhost'),
                    type=socket.gethostbyname)
    ap.add_argument('--statsd-port',
                    default=int(parsed_configs.get('statsd_port', 8125)),
                    type=int)
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

    nt = NavTiming(brokers=args.brokers.split(','),
                   consumer_group=args.consumer_group,
                   statsd_host=args.statsd_host,
                   statsd_port=args.statsd_port,
                   datacenter=args.datacenter,
                   etcd_domain=args.etcd_domain,
                   etcd_path=args.etcd_path,
                   etcd_refresh=args.etcd_refresh,
                   verbose=args.verbose,
                   dry_run=args.dry_run)
    nt.run()


if __name__ == '__main__':
    main()
