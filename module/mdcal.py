#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from calendar import Calendar
import calendar
import datetime
import sys

class MarkdownCalendar(Calendar):
    def formatday(self, day, weekday):
        if day == 0:
            return '     ' # day outside month
        elif day < 10:
            return '   %d ' % (day)
        else:
            return '  %d ' % (day)

    def formatweek(self, theweek):
        s = '|'.join(self.formatday(d, wd) for (d, wd) in theweek)
        return '|%s|' % s

    def formatweekday(self, day):
        return ' %s ' % (calendar.day_abbr[day])

    def formatweekheader(self):
        s = '|'
        s += '|'.join(self.formatweekday(i) for i in self.iterweekdays())
        s += '|\n' + ('|-----' * 7) + '|'
        return s

    def formatmonthname(self, theyear, themonth, withyear=True):
        if withyear:
            s = '%s %s' % (calendar.month_name[themonth], theyear)
        else:
            s = '%s' % calendar.month_name[themonth]
        return '**%s**' % s

    def formatmonth(self, theyear, themonth, withyear=True):
        v = []
        a = v.append
        a(self.formatmonthname(theyear, themonth, withyear=withyear))
        a('\n')
        a(self.formatweekheader())
        a('\n')
        for week in self.monthdays2calendar(theyear, themonth):
            a(self.formatweek(week))
            a('\n')
        a('\n')
        return ''.join(v)

    def formatyear(self, theyear):
        v = []
        a = v.append
        a('# %s' % (theyear))
        a('\n')
        for m in range(calendar.January, calendar.January+12):
          a(self.formatmonth(theyear, m, withyear=False))
        return ''.join(v)

    def formatyearpage(self, theyear):
        return self.formatyear(theyear)

def main(args):
    import optparse
    parser = optparse.OptionParser(usage="usage: %prog [options] [year [month]]")
    (options, args) = parser.parse_args(args)

    cal = MarkdownCalendar()
    if len(args) == 1:
        print(cal.formatyearpage(datetime.date.today().year))
    elif len(args) == 2:
        print(cal.formatyearpage(int(args[1])))
    elif len(args) == 3:
        print(cal.formatmonth(int(args[1]), int(args[2])))
    else:
        parser.error("incorrect number of arguments: %s" % (len(args)))
        sys.exit(1)
        
if __name__ == "__main__":
    main(sys.argv)
