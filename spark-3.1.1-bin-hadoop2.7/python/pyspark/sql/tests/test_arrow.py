#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import os
import threading
import time
import unittest
import warnings
from distutils.version import LooseVersion

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, \
    FloatType, DoubleType, DecimalType, DateType, TimestampType, BinaryType, StructField, ArrayType
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow, \
    pandas_requirement_message, pyarrow_requirement_message
from pyspark.testing.utils import QuietTest

if have_pandas:
    import pandas as pd
    from pandas.testing import assert_frame_equal

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)  # type: ignore
class ArrowTests(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        from datetime import date, datetime
        from decimal import Decimal
        super(ArrowTests, cls).setUpClass()
        cls.warnings_lock = threading.Lock()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.spark.conf.set("spark.sql.session.timeZone", tz)

        # Test fallback
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "false")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "false"
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "true"

        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled") == "true"
        cls.spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
        assert cls.spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled") == "false"

        # Enable Arrow optimization in this tests.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Disable fallback by default to easily detect the failures.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

        cls.schema = StructType([
            StructField("1_str_t", StringType(), True),
            StructField("2_int_t", IntegerType(), True),
            StructField("3_long_t", LongType(), True),
            StructField("4_float_t", FloatType(), True),
            StructField("5_double_t", DoubleType(), True),
            StructField("6_decimal_t", DecimalType(38, 18), True),
            StructField("7_date_t", DateType(), True),
            StructField("8_timestamp_t", TimestampType(), True),
            StructField("9_binary_t", BinaryType(), True)])
        cls.data = [(u"a", 1, 10, 0.2, 2.0, Decimal("2.0"),
                     date(1969, 1, 1), datetime(1969, 1, 1, 1, 1, 1), bytearray(b"a")),
                    (u"b", 2, 20, 0.4, 4.0, Decimal("4.0"),
                     date(2012, 2, 2), datetime(2012, 2, 2, 2, 2, 2), bytearray(b"bb")),
                    (u"c", 3, 30, 0.8, 6.0, Decimal("6.0"),
                     date(2100, 3, 3), datetime(2100, 3, 3, 3, 3, 3), bytearray(b"ccc")),
                    (u"d", 4, 40, 1.0, 8.0, Decimal("8.0"),
                     date(2262, 4, 12), datetime(2262, 3, 3, 3, 3, 3), bytearray(b"dddd"))]

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        super(ArrowTests, cls).tearDownClass()

    def create_pandas_data_frame(self):
        import numpy as np
        data_dict = {}
        for j, name in enumerate(self.schema.names):
            data_dict[name] = [self.data[i][j] for i in range(len(self.data))]
        # need to convert these to numpy types first
        data_dict["2_int_t"] = np.int32(data_dict["2_int_t"])
        data_dict["4_float_t"] = np.float32(data_dict["4_float_t"])
        return pd.DataFrame(data=data_dict)

    def test_toPandas_fallback_enabled(self):
        ts = datetime.datetime(2015, 11, 1, 0, 30)
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.fallback.enabled": True}):
            schema = StructType([StructField("a", ArrayType(TimestampType()), True)])
            df = self.spark.createDataFrame([([ts],)], schema=schema)
            with QuietTest(self.sc):
                with self.warnings_lock:
                    with warnings.catch_warnings(record=True) as warns:
                        # we want the warnings to appear even if this test is run from a subclass
                        warnings.simplefilter("always")
                        pdf = df.toPandas()
                        # Catch and check the last UserWarning.
                        user_warns = [
                            warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                        self.assertTrue(len(user_warns) > 0)
                        self.assertTrue(
                            "Attempting non-optimization" in str(user_warns[-1]))
                        assert_frame_equal(pdf, pd.DataFrame({"a": [[ts]]}))

    def test_toPandas_fallback_disabled(self):
        schema = StructType([StructField("a", ArrayType(TimestampType()), True)])
        df = self.spark.createDataFrame([(None,)], schema=schema)
        with QuietTest(self.sc):
            with self.warnings_lock:
                with self.assertRaisesRegex(Exception, 'Unsupported type'):
                    df.toPandas()

    def test_null_conversion(self):
        df_null = self.spark.createDataFrame([tuple([None for _ in range(len(self.data[0]))])] +
                                             self.data)
        pdf = df_null.toPandas()
        null_counts = pdf.isnull().sum().tolist()
        self.assertTrue(all([c == 1 for c in null_counts]))

    def _toPandas_arrow_toggle(self, df):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            pdf = df.toPandas()

        pdf_arrow = df.toPandas()

        return pdf, pdf_arrow

    def test_toPandas_arrow_toggle(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        expected = self.create_pandas_data_frame()
        assert_frame_equal(expected, pdf)
        assert_frame_equal(expected, pdf_arrow)

    def test_toPandas_respect_session_timezone(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            pdf_la, pdf_arrow_la = self._toPandas_arrow_toggle(df)
            assert_frame_equal(pdf_arrow_la, pdf_la)

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            pdf_ny, pdf_arrow_ny = self._toPandas_arrow_toggle(df)
            assert_frame_equal(pdf_arrow_ny, pdf_ny)

            self.assertFalse(pdf_ny.equals(pdf_la))

            from pyspark.sql.pandas.types import _check_series_convert_timestamps_local_tz
            pdf_la_corrected = pdf_la.copy()
            for field in self.schema:
                if isinstance(field.dataType, TimestampType):
                    pdf_la_corrected[field.name] = _check_series_convert_timestamps_local_tz(
                        pdf_la_corrected[field.name], timezone)
            assert_frame_equal(pdf_ny, pdf_la_corrected)

    def test_pandas_round_trip(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(self.data, schema=self.schema)
        pdf_arrow = df.toPandas()
        assert_frame_equal(pdf_arrow, pdf)

    def test_filtered_frame(self):
        df = self.spark.range(3).toDF("i")
        pdf = df.filter("i < 0").toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "i")
        self.assertTrue(pdf.empty)

    def test_no_partition_frame(self):
        schema = StructType([StructField("field1", StringType(), True)])
        df = self.spark.createDataFrame(self.sc.emptyRDD(), schema)
        pdf = df.toPandas()
        self.assertEqual(len(pdf.columns), 1)
        self.assertEqual(pdf.columns[0], "field1")
        self.assertTrue(pdf.empty)

    def test_propagates_spark_exception(self):
        df = self.spark.range(3).toDF("i")

        def raise_exception():
            raise Exception("My error")
        exception_udf = udf(raise_exception, IntegerType())
        df = df.withColumn("error", exception_udf())
        with QuietTest(self.sc):
            with self.assertRaisesRegex(Exception, 'My error'):
                df.toPandas()

    def _createDataFrame_toggle(self, pdf, schema=None):
        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df_no_arrow = self.spark.createDataFrame(pdf, schema=schema)

        df_arrow = self.spark.createDataFrame(pdf, schema=schema)

        return df_no_arrow, df_arrow

    def test_createDataFrame_toggle(self):
        pdf = self.create_pandas_data_frame()
        df_no_arrow, df_arrow = self._createDataFrame_toggle(pdf, schema=self.schema)
        self.assertEqual(df_no_arrow.collect(), df_arrow.collect())

    def test_createDataFrame_respect_session_timezone(self):
        from datetime import timedelta
        pdf = self.create_pandas_data_frame()
        timezone = "America/Los_Angeles"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            df_no_arrow_la, df_arrow_la = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_la = df_no_arrow_la.collect()
            result_arrow_la = df_arrow_la.collect()
            self.assertEqual(result_la, result_arrow_la)

        timezone = "America/New_York"
        with self.sql_conf({"spark.sql.session.timeZone": timezone}):
            df_no_arrow_ny, df_arrow_ny = self._createDataFrame_toggle(pdf, schema=self.schema)
            result_ny = df_no_arrow_ny.collect()
            result_arrow_ny = df_arrow_ny.collect()
            self.assertEqual(result_ny, result_arrow_ny)

            self.assertNotEqual(result_ny, result_la)

            # Correct result_la by adjusting 3 hours difference between Los Angeles and New York
            result_la_corrected = [Row(**{k: v - timedelta(hours=3) if k == '8_timestamp_t' else v
                                          for k, v in row.asDict().items()})
                                   for row in result_la]
            self.assertEqual(result_ny, result_la_corrected)

    def test_createDataFrame_with_schema(self):
        pdf = self.create_pandas_data_frame()
        df = self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertEqual(self.schema, df.schema)
        pdf_arrow = df.toPandas()
        assert_frame_equal(pdf_arrow, pdf)

    def test_createDataFrame_with_incorrect_schema(self):
        pdf = self.create_pandas_data_frame()
        fields = list(self.schema)
        fields[5], fields[6] = fields[6], fields[5]  # swap decimal with date
        wrong_schema = StructType(fields)
        with self.sql_conf({"spark.sql.execution.pandas.convertToArrowArraySafely": False}):
            with QuietTest(self.sc):
                with self.assertRaisesRegex(Exception, "[D|d]ecimal.*got.*date"):
                    self.spark.createDataFrame(pdf, schema=wrong_schema)

    def test_createDataFrame_with_names(self):
        pdf = self.create_pandas_data_frame()
        new_names = list(map(str, range(len(self.schema.fieldNames()))))
        # Test that schema as a list of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=list(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)
        # Test that schema as tuple of column names gets applied
        df = self.spark.createDataFrame(pdf, schema=tuple(new_names))
        self.assertEqual(df.schema.fieldNames(), new_names)

    def test_createDataFrame_column_name_encoding(self):
        pdf = pd.DataFrame({u'a': [1]})
        columns = self.spark.createDataFrame(pdf).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], 'a')
        columns = self.spark.createDataFrame(pdf, [u'b']).columns
        self.assertTrue(isinstance(columns[0], str))
        self.assertEqual(columns[0], 'b')

    def test_createDataFrame_with_single_data_type(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(ValueError, ".*IntegerType.*not supported.*"):
                self.spark.createDataFrame(pd.DataFrame({"a": [1]}), schema="int")

    def test_createDataFrame_does_not_modify_input(self):
        # Some series get converted for Spark to consume, this makes sure input is unchanged
        pdf = self.create_pandas_data_frame()
        # Use a nanosecond value to make sure it is not truncated
        pdf.iloc[0, 7] = pd.Timestamp(1)
        # Integers with nulls will get NaNs filled with 0 and will be casted
        pdf.iloc[1, 1] = None
        pdf_copy = pdf.copy(deep=True)
        self.spark.createDataFrame(pdf, schema=self.schema)
        self.assertTrue(pdf.equals(pdf_copy))

    def test_schema_conversion_roundtrip(self):
        from pyspark.sql.pandas.types import from_arrow_schema, to_arrow_schema
        arrow_schema = to_arrow_schema(self.schema)
        schema_rt = from_arrow_schema(arrow_schema)
        self.assertEqual(self.schema, schema_rt)

    def test_createDataFrame_with_array_type(self):
        pdf = pd.DataFrame({"a": [[1, 2], [3, 4]], "b": [[u"x", u"y"], [u"y", u"z"]]})
        df, df_arrow = self._createDataFrame_toggle(pdf)
        result = df.collect()
        result_arrow = df_arrow.collect()
        expected = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_toPandas_with_array_type(self):
        expected = [([1, 2], [u"x", u"y"]), ([3, 4], [u"y", u"z"])]
        array_schema = StructType([StructField("a", ArrayType(IntegerType())),
                                   StructField("b", ArrayType(StringType()))])
        df = self.spark.createDataFrame(expected, schema=array_schema)
        pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
        result = [tuple(list(e) for e in rec) for rec in pdf.to_records(index=False)]
        result_arrow = [tuple(list(e) for e in rec) for rec in pdf_arrow.to_records(index=False)]
        for r in range(len(expected)):
            for e in range(len(expected[r])):
                self.assertTrue(expected[r][e] == result_arrow[r][e] and
                                result[r][e] == result_arrow[r][e])

    def test_createDataFrame_with_map_type(self):
        map_data = [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]

        pdf = pd.DataFrame({"id": [0, 1, 2, 3, 4], "m": map_data})
        schema = "id long, m map<string, long>"

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df = self.spark.createDataFrame(pdf, schema=schema)

        if LooseVersion(pa.__version__) < LooseVersion("2.0.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegex(Exception, "MapType.*only.*pyarrow 2.0.0"):
                    self.spark.createDataFrame(pdf, schema=schema)
        else:
            df_arrow = self.spark.createDataFrame(pdf, schema=schema)

            result = df.collect()
            result_arrow = df_arrow.collect()

            self.assertEqual(len(result), len(result_arrow))
            for row, row_arrow in zip(result, result_arrow):
                i, m = row
                _, m_arrow = row_arrow
                self.assertEqual(m, map_data[i])
                self.assertEqual(m_arrow, map_data[i])

    def test_toPandas_with_map_type(self):
        pdf = pd.DataFrame({"id": [0, 1, 2, 3],
                            "m": [{}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2, "c": 3}]})

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df = self.spark.createDataFrame(pdf, schema="id long, m map<string, long>")

        if LooseVersion(pa.__version__) < LooseVersion("2.0.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegex(Exception, "MapType.*only.*pyarrow 2.0.0"):
                    df.toPandas()
        else:
            pdf_non, pdf_arrow = self._toPandas_arrow_toggle(df)
            assert_frame_equal(pdf_arrow, pdf_non)

    def test_toPandas_with_map_type_nulls(self):
        pdf = pd.DataFrame({"id": [0, 1, 2, 3, 4],
                            "m": [{"a": 1}, {"b": 2, "c": 3}, {}, None, {"d": None}]})

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df = self.spark.createDataFrame(pdf, schema="id long, m map<string, long>")

        if LooseVersion(pa.__version__) < LooseVersion("2.0.0"):
            with QuietTest(self.sc):
                with self.assertRaisesRegex(Exception, "MapType.*only.*pyarrow 2.0.0"):
                    df.toPandas()
        else:
            pdf_non, pdf_arrow = self._toPandas_arrow_toggle(df)
            assert_frame_equal(pdf_arrow, pdf_non)

    def test_createDataFrame_with_int_col_names(self):
        import numpy as np
        pdf = pd.DataFrame(np.random.rand(4, 2))
        df, df_arrow = self._createDataFrame_toggle(pdf)
        pdf_col_names = [str(c) for c in pdf.columns]
        self.assertEqual(pdf_col_names, df.columns)
        self.assertEqual(pdf_col_names, df_arrow.columns)

    def test_createDataFrame_fallback_enabled(self):
        ts = datetime.datetime(2015, 11, 1, 0, 30)
        with QuietTest(self.sc):
            with self.sql_conf({"spark.sql.execution.arrow.pyspark.fallback.enabled": True}):
                with warnings.catch_warnings(record=True) as warns:
                    # we want the warnings to appear even if this test is run from a subclass
                    warnings.simplefilter("always")
                    df = self.spark.createDataFrame(
                        pd.DataFrame({"a": [[ts]]}), "a: array<timestamp>")
                    # Catch and check the last UserWarning.
                    user_warns = [
                        warn.message for warn in warns if isinstance(warn.message, UserWarning)]
                    self.assertTrue(len(user_warns) > 0)
                    self.assertTrue(
                        "Attempting non-optimization" in str(user_warns[-1]))
                    self.assertEqual(df.collect(), [Row(a=[ts])])

    def test_createDataFrame_fallback_disabled(self):
        with QuietTest(self.sc):
            with self.assertRaisesRegex(TypeError, 'Unsupported type'):
                self.spark.createDataFrame(
                    pd.DataFrame({"a": [[datetime.datetime(2015, 11, 1, 0, 30)]]}),
                    "a: array<timestamp>")

    # Regression test for SPARK-23314
    def test_timestamp_dst(self):
        # Daylight saving time for Los Angeles for 2015 is Sun, Nov 1 at 2:00 am
        dt = [datetime.datetime(2015, 11, 1, 0, 30),
              datetime.datetime(2015, 11, 1, 1, 30),
              datetime.datetime(2015, 11, 1, 2, 30)]
        pdf = pd.DataFrame({'time': dt})

        df_from_python = self.spark.createDataFrame(dt, 'timestamp').toDF('time')
        df_from_pandas = self.spark.createDataFrame(pdf)

        assert_frame_equal(pdf, df_from_python.toPandas())
        assert_frame_equal(pdf, df_from_pandas.toPandas())

    # Regression test for SPARK-28003
    def test_timestamp_nat(self):
        dt = [pd.NaT, pd.Timestamp('2019-06-11'), None] * 100
        pdf = pd.DataFrame({'time': dt})
        df_no_arrow, df_arrow = self._createDataFrame_toggle(pdf)

        assert_frame_equal(pdf, df_no_arrow.toPandas())
        assert_frame_equal(pdf, df_arrow.toPandas())

    def test_toPandas_batch_order(self):

        def delay_first_part(partition_index, iterator):
            if partition_index == 0:
                time.sleep(0.1)
            return iterator

        # Collects Arrow RecordBatches out of order in driver JVM then re-orders in Python
        def run_test(num_records, num_parts, max_records, use_delay=False):
            df = self.spark.range(num_records, numPartitions=num_parts).toDF("a")
            if use_delay:
                df = df.rdd.mapPartitionsWithIndex(delay_first_part).toDF()
            with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": max_records}):
                pdf, pdf_arrow = self._toPandas_arrow_toggle(df)
                assert_frame_equal(pdf, pdf_arrow)

        cases = [
            (1024, 512, 2),    # Use large num partitions for more likely collecting out of order
            (64, 8, 2, True),  # Use delay in first partition to force collecting out of order
            (64, 64, 1),       # Test single batch per partition
            (64, 1, 64),       # Test single partition, single batch
            (64, 1, 8),        # Test single partition, multiple batches
            (30, 7, 2),        # Test different sized partitions
        ]

        for case in cases:
            run_test(*case)

    def test_createDateFrame_with_category_type(self):
        pdf = pd.DataFrame({"A": [u"a", u"b", u"c", u"a"]})
        pdf["B"] = pdf["A"].astype('category')
        category_first_element = dict(enumerate(pdf['B'].cat.categories))[0]

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": True}):
            arrow_df = self.spark.createDataFrame(pdf)
            arrow_type = arrow_df.dtypes[1][1]
            result_arrow = arrow_df.toPandas()
            arrow_first_category_element = result_arrow["B"][0]

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.enabled": False}):
            df = self.spark.createDataFrame(pdf)
            spark_type = df.dtypes[1][1]
            result_spark = df.toPandas()
            spark_first_category_element = result_spark["B"][0]

        assert_frame_equal(result_spark, result_arrow)

        # ensure original category elements are string
        self.assertIsInstance(category_first_element, str)
        # spark data frame and arrow execution mode enabled data frame type must match pandas
        self.assertEqual(spark_type, 'string')
        self.assertEqual(arrow_type, 'string')
        self.assertIsInstance(arrow_first_category_element, str)
        self.assertIsInstance(spark_first_category_element, str)

    def test_createDataFrame_with_float_index(self):
        # SPARK-32098: float index should not produce duplicated or truncated Spark DataFrame
        self.assertEqual(
            self.spark.createDataFrame(
                pd.DataFrame({'a': [1, 2, 3]}, index=[2., 3., 4.])).distinct().count(), 3)

    def test_no_partition_toPandas(self):
        # SPARK-32301: toPandas should work from a Spark DataFrame with no partitions
        # Forward-ported from SPARK-32300.
        pdf = self.spark.sparkContext.emptyRDD().toDF("col1 int").toPandas()
        self.assertEqual(len(pdf), 0)
        self.assertEqual(list(pdf.columns), ["col1"])

    def test_createDataFrame_empty_partition(self):
        pdf = pd.DataFrame({"c1": [1], "c2": ["string"]})
        df = self.spark.createDataFrame(pdf)
        self.assertEqual([Row(c1=1, c2='string')], df.collect())
        self.assertGreater(self.spark.sparkContext.defaultParallelism, len(pdf))


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message)  # type: ignore
class MaxResultArrowTests(unittest.TestCase):
    # These tests are separate as 'spark.driver.maxResultSize' configuration
    # is a static configuration to Spark context.

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession(SparkContext(
            'local[4]', cls.__name__, conf=SparkConf().set("spark.driver.maxResultSize", "10k")))

        # Explicitly enable Arrow and disable fallback.
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        cls.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_exception_by_max_results(self):
        with self.assertRaisesRegex(Exception, "is bigger than"):
            self.spark.range(0, 10000, 1, 100).toPandas()


class EncryptionArrowTests(ArrowTests):

    @classmethod
    def conf(cls):
        return super(EncryptionArrowTests, cls).conf().set("spark.io.encryption.enabled", "true")


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
