import unittest

from google.protobuf.internal import encoder

from prometheus_client import Gauge, Counter, Summary, CollectorRegistry, Histogram
from prometheus_client.serialization.formats import ProtobufFormat, ProtobufTextFormat
from prometheus.client.model import metrics_pb2


class TestProtobufExposition(unittest.TestCase):

    def test_protobuf_gauge(self):
        myreg = CollectorRegistry()
        g = Gauge('gg', 'A gauge', registry=myreg)
        g.set(17)
        pbf = ProtobufFormat()
        # = pbf.marshall()
        measurement = metrics_pb2.Gauge(value=17)
        metric = metrics_pb2.Metric(label=[], gauge=measurement)
        pb2_family = metrics_pb2.MetricFamily(name="gg",
                                                 help="A gauge",
                                                 type=metrics_pb2.GAUGE,
                                                 metric=[metric])
        body = pb2_family.SerializeToString()
        expected_result = bytes(encoder._VarintBytes(len(body)) + body)

        self.assertEqual(expected_result, pbf.marshall(myreg))

    def test_protobuf_counter(self):
        myreg = CollectorRegistry()
        g = Counter('gg', 'A counter', registry=myreg)
        g.inc(17)
        pbf = ProtobufFormat()
        measurement = metrics_pb2.Counter(value=17)
        metric = metrics_pb2.Metric(label=[], counter=measurement)
        pb2_family = metrics_pb2.MetricFamily(name="gg",
                                              help="A counter",
                                              type=metrics_pb2.COUNTER,
                                              metric=[metric])
        body = pb2_family.SerializeToString()
        expected_result = bytes(encoder._VarintBytes(len(body)) + body)

        self.assertEqual(expected_result, pbf.marshall(myreg))

    def test_protobuf_histogram(self):
        myreg = CollectorRegistry()
        g = Histogram('hh', 'A histogram', registry=myreg)
        g.time()
        #for more complicated data structures i'll test wtih textformat
        expected = """name: "hh"
help: "A histogram"
type: HISTOGRAM
metric {
  histogram {
    sample_count: 0
    sample_sum: 0
    bucket {
      cumulative_count: 0
      upper_bound: 0.005
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.01
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.025
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.05
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.075
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.1
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.25
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.5
    }
    bucket {
      cumulative_count: 0
      upper_bound: 0.75
    }
    bucket {
      cumulative_count: 0
      upper_bound: 1.0
    }
    bucket {
      cumulative_count: 0
      upper_bound: 2.5
    }
    bucket {
      cumulative_count: 0
      upper_bound: 5.0
    }
    bucket {
      cumulative_count: 0
      upper_bound: 7.5
    }
    bucket {
      cumulative_count: 0
      upper_bound: 10.0
    }
    bucket {
      cumulative_count: 0
      upper_bound: inf
    }
  }
}
"""
        pbf = ProtobufTextFormat()

        self.assertEqual(expected, pbf.marshall(myreg))


    def test_protobuf_summary(self):
        myreg = CollectorRegistry()
        s = Summary('hh', 'A summary', registry=myreg)
        s.observe(123)
        s.observe(23)
        expected_result = """name: "hh"
help: "A summary"
type: SUMMARY
metric {
  summary {
    sample_count: 2
    sample_sum: 146
  }
}
"""
        pbft = ProtobufTextFormat()
        print(pbft.marshall(myreg))
        self.assertEqual(expected_result, pbft.marshall(myreg))