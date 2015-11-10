import random
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
    time.sleep(random.randint(1, 10))
    out = output.open("w")
    print >> out, "Hello Devoxx"
    out.close()


def isPrime(n):
    if n == 2 or n == 3: return True
    if n % 2 == 0 or n < 2: return False
    for i in range(3, int(n ** 0.5) + 1, 2):  # only odd numbers
        if n % i == 0:
            return False
    return True


class DumpCartAndInvoices(luigi.Task):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/dump_cart/%Y/%m/%d/out.txt"))

    def run(self):
        if isPrime(self.day.day):
            raise Exception('I don''t like Prime days')
        random_job(self.output())


class DumpWebLogs(luigi.Task):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/dump_log/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class HistoryTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        self.day = kwargs.get("day")
        super(HistoryTask, self).__init__(*args, **kwargs)

    def required_bucket(self, bucket):
        NotImplemented

    def requires(self):
        buckets = []
        for d in dateutils.day_range(self.day - datetime.timedelta(days=60), self.day):
            buckets.append(self.required_bucket(d))
        return buckets


class VisitBucket(luigi.Task):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def requires(self):
        return DumpWebLogs(self.day)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/visit/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class VisitHistoryAggregate(HistoryTask):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def required_bucket(self, bucket):
        return VisitBucket(bucket)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/history_visit/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class OrderBucket(luigi.Task):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def requires(self):
        return DumpCartAndInvoices(self.day)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/order/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class OrderHistoryAggregate(HistoryTask):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def required_bucket(self, bucket):
        return OrderBucket(bucket)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/history_order/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class CartBucket(luigi.Task):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def requires(self):
        return DumpCartAndInvoices(self.day)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/cart/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class CartHistoryAggregate(HistoryTask):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def required_bucket(self, bucket):
        return CartBucket(bucket)

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/history_cart/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class CustomerSegment(luigi.Task):
    day = luigi.DateParameter(dateutils.yester_day())

    def requires(self):
        return [
            CartHistoryAggregate(self.day),
            OrderHistoryAggregate(self.day),
            VisitHistoryAggregate(self.day)
        ]

    def output(self):
        return luigi.LocalTarget(self.day.strftime("tmp/segment/%Y/%m/%d/out.txt"))

    def run(self):
        random_job(self.output())


class MonthlyReport(luigi.Task):
    month = luigi.DateParameter(default=dateutils.month_first_day(dateutils.yester_day()))

    def requires(self):
        buckets = []
        for d in dateutils.day_range(
                dateutils.month_first_day(self.month),
                dateutils.month_last_day(self.month)):
            buckets.append(OrderBucket(d))
        return buckets

    def output(self):
        return luigi.LocalTarget(self.month.strftime("tmp/segment/%Y/%m/%d/out.txt"))


class NightlyTask(luigi.WrapperTask):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def requires(self):
        return [CustomerSegment(self.day),
                MonthlyReport(month=dateutils.month_first_day(dateutils.yester_day()))]

if __name__ == "__main__":
    luigi.run()
