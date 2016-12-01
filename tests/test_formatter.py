import unittest

from prometheus_client.serialization.formats import TextFormat, ProtobufFormat, ProtobufTextFormat
from prometheus_client.serialization.formatter import Formatter


class TestFormatter(unittest.TestCase):

    def test_protobuffer(self):
        headers = ({
            'accept': "proto=io.prometheus.client.MetricFamily;application/vnd.google.protobuf;encoding=delimited",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'Accept': "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'ACCEPT': "encoding=delimited;application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily",
            'accept-encoding': "gzip, deflate, sdch",
        })

        for i in headers:
            self.assertEqual(ProtobufFormat, Formatter.getformatter(i))

    def test_protobuffer_debug(self):
        headers = ({
            'accept': "proto=io.prometheus.client.MetricFamily;application/vnd.google.protobuf;encoding=text",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'Accept': "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=text",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'ACCEPT': "encoding=text;application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily",
            'accept-encoding': "gzip, deflate, sdch",
        })

        for i in headers:
            self.assertEqual(ProtobufTextFormat, Formatter.getformatter(i))

    def test_text_004(self):
        headers = ({
            'accept': "text/plain; version=0.0.4",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'Accept': "text/plain;version=0.0.4",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'ACCEPT': " version=0.0.4; text/plain",
            'accept-encoding': "gzip, deflate, sdch",
        })

        for i in headers:
            self.assertEqual(TextFormat, Formatter.getformatter(i))

    def test_text_default(self):
        headers = ({
            'Accept': "text/plain;",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'accept': "text/plain",
            'accept-encoding': "gzip, deflate, sdch",
        })

        for i in headers:
            self.assertEqual(TextFormat, Formatter.getformatter(i))

    def test_default(self):
        headers = ({
            'accept': "application/json",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'Accept': "*/*",
            'accept-encoding': "gzip, deflate, sdch",
        }, {
            'ACCEPT': "application/nothing",
            'accept-encoding': "gzip, deflate, sdch",
        })

        for i in headers:
            self.assertEqual(TextFormat, Formatter.getformatter(i))

    def test_getdefaultformatter(self):
        self.assertEqual(TextFormat, Formatter.getdefaultFormatter())