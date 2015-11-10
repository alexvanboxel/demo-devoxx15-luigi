import time
import datetime

import luigi

import dateutils

__author__ = 'alexvanboxel'


def shout(line):
    print "**************************************************************"
    print "*"
    print "* " + line
    print "*"
    print "**************************************************************"


def random_job(output):
    time.sleep(10)
    out = output.open("w")
    print >> out, "Hello Devoxx"
    out.close()


class NothingTask(luigi.Task):
    def run(self):
        shout("Hello Devoxx")


class SimpleTask(luigi.Task):
    def run(self):
        random_job(self.output())

    def output(self):
        return luigi.LocalTarget("tmp/trs/test.txt")


class DayTask(luigi.Task):
    day = luigi.DateParameter(dateutils.yester_day())

    def run(self):
        random_job(self.output())

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/%Y/%m/%d/test.txt"))


class HistoryTask(luigi.WrapperTask):
    day = luigi.DateParameter(dateutils.yester_day())
    limit = luigi.IntParameter(default=60)

    def requires(self):
        buckets = []
        till = self.day - datetime.timedelta(days=self.limit)
        for d in dateutils.day_range(till, self.day):
            buckets.append(DayTask(d))
        return buckets


if __name__ == "__main__":
    luigi.run()
