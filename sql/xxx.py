from datetime import tzinfo, timedelta, datetime
import time
import calendar

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

# A UTC class.

class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

date_str = "2016-07-25 09:18:38.497"
dt_obj = datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S.%f')
print "object w/o timezone = " + repr(dt_obj)

qqqq = dt_obj.replace(tzinfo=UTC())
print "adding timezone = " + repr(qqqq)
print "time_t with hopefully proper GMT offset: " + repr(calendar.timegm(qqqq.utctimetuple()))
