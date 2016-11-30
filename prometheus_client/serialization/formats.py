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
    def _format_counter(self, counter, name):
        """ Returns a representation of a counter value in the implemented
            format. Receives a tuple with the labels (a dict) as first element
            and the value as a second element
        """
        pass

    @abstractmethod
    def _format_gauge(self, gauge, name):
        """ Returns a representation of a gauge value in the implemented
            format. Receives a tuple with the labels (a dict) as first element
            and the value as a second element
        """
        pass

    @abstractmethod
    def _format_summary(self, summary, name):
        """ Returns a representation of a summary value in the implemented
            format. Receives a tuple with the labels (a dict) as first element
            and the value as a second element
        """
        pass

    @abstractmethod
    def marshall(self, registry):
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
    GAUGE_FMT = COUNTER_FMT
    SUMMARY_FMTS = {
        'quantile': "{name}{labels} {value}",
        'sum': "{name}_sum{labels} {value}",
        'count': "{name}_count{labels} {value}",
    }


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


    def _format_counter(self, counter, name, const_labels):
        pass

    def _format_gauge(self, gauge, name, const_labels):
        pass

    def _format_summary(self, summary, name, const_labels):
        pass

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
        result = []
        for k, v in labels.items():
            l = metrics_pb2.LabelPair(name=k, value=str(v))
            result.append(l)
        return result

    def _format_counter(self, counter, name, const_labels):
        labels = utils.format_labels(counter[0])

        # With a counter and labelpairs we do a Metric
        pb2_labels = self._create_pb2_labels(labels)
        counter = metrics_pb2.Counter(value=counter[1])

        metric = metrics_pb2.Metric(label=pb2_labels, counter=counter)

        return metric

    def _format_gauge(self, gauge, name, const_labels):
        labels = utils.format_labels(gauge[0])

        pb2_labels = self._create_pb2_labels(labels)
        gauge = metrics_pb2.Gauge(value=gauge[1])

        metric = metrics_pb2.Metric(label=pb2_labels, gauge=gauge)
        return metric

    def _format_summary(self, summary, name, const_labels):
        labels = utils.format_labels(summary[0])

        pb2_labels = self._create_pb2_labels(labels)

        # Create the quantiles
        quantiles = []

        for k, v in summary[1].items():
            if not isinstance(k, str):
                q = metrics_pb2.Quantile(quantile=k, value=v)
                quantiles.append(q)

        summary = metrics_pb2.Summary(sample_count=summary[1]['count'],
                                      sample_sum=summary[1]['sum'],
                                      quantile=quantiles)

        metric = metrics_pb2.Metric(label=pb2_labels, summary=summary)

        return metric

    def marshall_collector(self, collector):

        if isinstance(collector, Counter):
            metric_type = metrics_pb2.COUNTER
            exec_method = self._format_counter
        elif isinstance(collector, Gauge):
            metric_type = metrics_pb2.GAUGE
            exec_method = self._format_gauge
        elif isinstance(collector, Summary):
            metric_type = metrics_pb2.SUMMARY
            exec_method = self._format_summary
        else:
            raise TypeError("Not a valid object format")

        metrics = []

        for i in collector.get_all():
            r = exec_method(i, collector.name, collector.const_labels)
            metrics.append(r)

        pb2_collector = metrics_pb2.MetricFamily(name=collector.name,
                                                 help=collector.help_text,
                                                 type=metric_type,
                                                 metric=metrics)
        return pb2_collector

    def marshall(self, registry):
        """Returns bytes"""
        result = b""

        for i in registry.get_all():
            # Each message needs to be prefixed with a varint with the size of
            # the message (MetrycType)
            # https://github.com/matttproud/golang_protobuf_extensions/blob/master/ext/encode.go
            # http://zombietetris.de/blog/building-your-own-writedelimitedto-for-python-protobuf/
            body = self.marshall_collector(i).SerializeToString()
            msg = encoder._VarintBytes(len(body)) + body
            result += msg

        return result


class ProtobufTextFormat(ProtobufFormat):
    """Return protobuf data as text, only for debugging"""

    ENCODING = 'test'

    def marshall(self, registry):
        blocks = []

        for i in registry.get_all():
            blocks.append(str(self.marshall_collector(i)))

        return self.__class__.LINE_SEPARATOR_FMT.join(blocks)