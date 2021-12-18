# Data-Engineer-interview

It is minimal non-persistent analyzer. (analyzing a single LogFile - no cross logfile data remains) \
It better be with database support and logs(csv-data) is a received from a request to queue-service
    
1) The `log_alerts.py` expects the logfile `output.csv` on the path
2) The logfile is renamed while processsed and after processed renamed with timestamp
3) Alerts can be defined in the dict `alerts`
   * distinct - define the key for the count  
   * by - define the either matching field to value
 
 ---
 
 #### Database support: (for the case with [SWC-DB](https://www.swcdb.org))
 1) define alerts
 2) insert/index by key=[rounded(ts/duration), distinct/s,,] value=+1 (column with duration TTL)\
    (clears expired logs for the alert duration)
    * there won't be the need for the `self.tracker` object;
      only iter csv and update the cells over the alerts-duration and distinct kinds 
 3) select the cells with COUNTER >= alert-count
 * Size and Tracked Durations won't be a subject of Worker-Host consumes more resources
  
