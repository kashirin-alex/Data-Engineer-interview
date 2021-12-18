# -- coding: utf-8 --
# __author__ = 'Kashirin Alex'

import csv
import os
import time


alerts = [
    {
        'id': 0,
        'name':  'State of Many Errors',
        'duration': 60,
        'count': 10,
        'distinct': [],
        'by': {'error_code': ['0']}  # 0: fatal
    },
    {
        'id': 1,
        'name': 'Bundle-ID Errors',
        'duration': 3600,
        'count': 10,
        'distinct': ['bundle_id'],
        'by': {'error_code': ['0', '1']}  # 0: fatal or 1: error
    },
]

csv_map = {
    'error_code': 0,
    'error_message': 1,
    'severity': 2,
    'log_location': 3,
    'mode': 4,
    'model': 5,
    'graphics': 6,
    'session_id': 7,
    'sdkv': 8,
    'test_mode': 9,
    'flow_id': 10,
    'flow_type': 11,
    'sdk_date': 12,
    'publisher_id': 13,
    'game_id': 14,
    'bundle_id': 15,
    'appv': 16,
    'language': 17,
    'os': 18,
    'adv_id': 19,
    'gdpr': 20,
    'ccpa': 21,
    'country_code': 22,
    'date': 23
}


class LogProcessor(object):
    ___slots___ = ('logfile', 'tracker')

    def __init__(self, logfile):
        self.logfile = logfile
        self.tracker = {alert['id']: [] for alert in alerts}
        #

    def has_new_log(self):
        # file-check or fd-event
        while not os.path.isfile(self.logfile):
            print("Quiting no-new log")
            return False
        return True
        #

    def run(self):
        process_log = self.logfile + '.log'
        while self.has_new_log():
            os.rename(self.logfile, process_log)
            try:
                csv_file = open(process_log, 'r', encoding='utf-8')
                csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                i = 0
                try:
                    for row in csv_reader:
                        self.process_log(row)
                        i += 1
                except Exception as e:
                    print('row:', i, 'exception:', e)

                csv_file.close()
                os.rename(process_log, process_log + '-' + str(time.time()) + '.old')
            except Exception as e:
                os.rename(process_log, self.logfile)
                print(e)

            found_alerts = []
            for alert in alerts:
                for track in self.tracker[alert['id']]:
                    if track['count'] >= alert['count']:
                        found_alerts.append((alert['id'], track))
            self.send_alerts(found_alerts)
            self.tracker.clear()
        #

    def process_log(self, row):
        for alert in alerts:
            found = False
            for by, opts in alert['by'].items():
                for v in opts:
                    found = row[csv_map[by]] == v
                    if found:
                        break
                if not found:
                    break
            if not found:
                continue
            ts = float(row[csv_map['date']])

            track = None
            for idx, state in enumerate(self.tracker[alert['id']]):
                found = True
                for kind, value in state['props'].items():
                    if row[csv_map[kind]] != value:
                        found = False
                        break
                if found:
                    track = state
                    break
            if track is None:
                self.tracker[alert['id']].append({
                    'count': 1,
                    'ts': ts,
                    'props': {kind: row[csv_map[kind]] for kind in alert['distinct']}
                })
                continue

            if track['count'] >= alert['count']:
                continue

            if ts - track['ts'] <= alert['duration']:
                track['count'] += 1
            else:
                track['ts'] = ts
        #

    def send_alerts(self, found_alerts):
        for alert_id, matched in found_alerts:
            print(alert_id, alerts[alert_id]['name'], matched)
        #


if __name__ == '__main__':

    #
    # It is minimal non-persistent analyzer. (analyzing of a single LogFile - no cross logfile data remains)
    #  It better be with database support and logs(csv-data) is a received from a request to queue-service
    # Database support: (for the case with SWC-DB)
    #   1) define alerts
    #   2) insert/index by key=[rounded(ts/duration), distinct/s,,] value=+1 (column with duration TTL)
    #      (clears expired logs for the alert duration)
    #      * there won't be the need for the self.tracker object;
    #        only iter csv and update the cells over the alerts-duration and distinct kinds
    #   3) select the cells with COUNTER >= alert-count
    #   * Size and Tracked Durations won't be a subject of Worker-Host consumes more resources
    #

    LogProcessor('output.csv').run()
