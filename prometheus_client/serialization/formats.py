from abc import ABCMeta, abstractmethod
import collections

from google.protobuf.internal import encoder

from prometheus_client.core import Metric, Gauge, Counter, Summary, REGISTRY, _floatToGoString
import utils
from prometheus.client.model import metrics_pb2

class PrometheusFormat(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_headers(self):
        """ Returns the headers of the communication format"""
        pass

    @abstractmethod
    def marshall(self, registry=REGISTRY):
        """ Marshalls a registry and returns the storage/transfer format """
        pass


class TextFormat(PrometheusFormat):
    # Header information
    CONTENT = 'text/plain'
    VERSION = '0.0.4'

    # Formats for values
    HELP_FMT = "# HELP {name} {help_text}"
    TYPE_FMT = "# TYPE {name} {value_type}"
    COMMENT_FMT = "# {comment}"
    LABEL_FMT = "{key}=\"{value}\""
    LABEL_SEPARATOR_FMT = ","
    LINE_SEPARATOR_FMT = "\n"
    COUNTER_FMT = "{name}{labels} {value}"


    def get_headers(self):
        headers = {
            'Content-Type': "{0}; version={1}; charset=utf-8".format(
                TextFormat.CONTENT,
                TextFormat.VERSION),
        }

        return headers

    def _format_line(self, name, labels, value):
        labels_str = ""
        # Unify the const_labels and labels
        # Consta labels have lower priority than labels
        labels = utils.format_labels(labels)

        # Create the label string
        if labels:
            labels_str = [TextFormat.LABEL_FMT.format(key=k, value=v)
                          for k, v in labels]
            labels_str = TextFormat.LABEL_SEPARATOR_FMT.join(labels_str)
            labels_str = "{{{labels}}}".format(labels=labels_str)


        result = TextFormat.COUNTER_FMT.format(name=name, labels=labels_str,
                                               value=_floatToGoString(value))

        return result.strip()


    def marshall_lines(self, collector):
        """ Marshalls a collector and returns the storage/transfer format in
            a tuple, this tuple has reprensentation format per element.
        """


        # create headers
        help_header = TextFormat.HELP_FMT.format(name=collector.name,
                                                 help_text=collector.documentation.replace('\\', r"\\")
                                                 .replace('\n', r"\n").encode('utf-8'))

        type_header = TextFormat.TYPE_FMT.format(name=collector.name,
                                                 value_type=collector.type)

        # Prepare start headers
        lines = [help_header, type_header]

        for name, labels, value in collector.samples:
            r = self._format_line(name, labels, value)
            # Check if it returns one or multiple lines
            if not isinstance(r, str) and isinstance(r, collections.Iterable):
                lines.extend(r)
            else:
                lines.append(r)

        return lines

    def marshall_collector(self, collector):
        result = self.marshall_lines(collector)
        return self.__class__.LINE_SEPARATOR_FMT.join(result)

    def marshall(self, registry=REGISTRY):
        blocks = []
        for metric in registry.collect():
            blocks.append(self.marshall_collector(metric))

        # Needs EOF
        blocks.append("")

        return self.__class__.LINE_SEPARATOR_FMT.join(blocks)


class ProtobufFormat(PrometheusFormat):
    # Header information
    CONTENT = 'application/vnd.google.protobuf'
    PROTO = 'io.prometheus.client.MetricFamily'
    ENCODING = 'delimited'
    VERSION = '0.0.4'
    LINE_SEPARATOR_FMT = "\n"

    def get_headers(self):
        headers = {
            'Content-Type': "{0}; proto={1}; encoding={2}".format(
                self.__class__.CONTENT,
                self.__class__.PROTO,
                self.__class__.ENCODING,
                ),
        }

        return headers

    def _create_pb2_labels(self, labels):
        return [metrics_pb2.LabelPair(name=k, value=str(v)) for k,v in labels]

    def _format_regular(self, collector, metric_type, pb_class):
        metrics = []
        for name, labels, value in collector.samples:
            labels = utils.format_labels(labels)
            pb2_labels = self._create_pb2_labels(labels)
            measurement = pb_class(value=value)
            metric = metrics_pb2.Metric(label=pb2_labels, **{collector.type: measurement})
            metrics.append(metric)

        return metrics_pb2.MetricFamily(name=collector.name,
                                                 help=collector.documentation,
                                                 type=metric_type,
                                                 metric=metrics)

    def _format_histogram(self, collector, metric_type, pb_class):
        buckets = []
        sample_count = 0
        sample_sum = 0
        pb2_labels = []
        for name, labels, value in collector.samples:
            if name.endswith("_bucket"):
                bucket_limit = labels.get('le')
                del labels['le']
                labels = utils.format_labels(labels)
                buck = metrics_pb2.Bucket(upper_bound=float(bucket_limit), cumulative_count=long(value))
                buckets.append(buck)
            if name.endswith('_count'):
                sample_count = long(value)
            if name.endswith('_sum'):
                sample_sum = long(value)
                pb2_labels = self._create_pb2_labels(labels)
        measurement = metrics_pb2.Histogram(bucket=buckets, sample_count=sample_count, sample_sum=sample_sum)

        metric = metrics_pb2.Metric(label=pb2_labels, **{collector.type: measurement})


        return metrics_pb2.MetricFamily(name=collector.name,
                                    help=collector.documentation,
                                    type=metric_type,
                                    metric=[metric])

    def _format_summary(self, collector, metric_type, pb_class):
        for name, labels, value in collector.samples:
            if name.endswith('_count'):
                sample_count = long(value)
            if name.endswith('_sum'):
                sample_sum = long(value)
                pb2_labels = self._create_pb2_labels(labels)
        measurement = metrics_pb2.Summary(sample_count=sample_count, sample_sum=sample_sum)
        metric = metrics_pb2.Metric(label=pb2_labels, **{collector.type: measurement})

        return metrics_pb2.MetricFamily(name=collector.name,
                                            help=collector.documentation,
                                            type=metric_type,
                                            metric=[metric])

    def marshall_collector(self, collector):
        meth = self._format_regular
        if collector.type == "counter":
            metric_type = metrics_pb2.COUNTER
            pb_class = metrics_pb2.Counter
        elif collector.type == "gauge":
            metric_type = metrics_pb2.GAUGE
            pb_class = metrics_pb2.Gauge
        elif collector.type == "summary":
            meth = self._format_summary
            metric_type = metrics_pb2.SUMMARY
            pb_class = metrics_pb2.Summary
        elif collector.type == "histogram":
            meth = self._format_histogram
            metric_type = metrics_pb2.HISTOGRAM
            pb_class = metrics_pb2.Histogram
        else:
            raise TypeError("Not a valid object format")

        return meth(collector, metric_type, pb_class)


    def marshall(self, registry=REGISTRY):
        """Returns bytes"""
        result = b""

        for i in registry.collect():
            # Each message needs to be prefixed with a varint with the size of
            # the message (MetricType)
            # https://github.com/matttproud/golang_protobuf_extensions/blob/master/ext/encode.go
            # http://zombietetris.de/blog/building-your-own-writedelimitedto-for-python-protobuf/
            body = self.marshall_collector(i).SerializeToString()
            msg = encoder._VarintBytes(len(body)) + body
            result += msg

        return result


class ProtobufTextFormat(ProtobufFormat):
    """Return protobuf data as text, only for debugging"""

    ENCODING = 'test'

    def marshall(self, registry=REGISTRY):
        blocks = []

        for i in registry.collect():
            blocks.append(str(self.marshall_collector(i)))

        return self.__class__.LINE_SEPARATOR_FMT.join(blocks)