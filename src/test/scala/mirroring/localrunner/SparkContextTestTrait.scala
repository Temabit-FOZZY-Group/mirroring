/*
 * Copyright (2021) The Delta Flow Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mirroring.localrunner

import mirroring.services.SparkContextTrait
import org.apache.spark.sql.SparkSession

trait SparkContextTestTrait extends SparkContextTrait {

  override def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
//      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalogImplementation", "hive")
//      .config("spark.sql.warehouse.dir", "s3a://minio-v2.palefat.fozzy.lan/palefat-internal-data/vl_orlov")
//      .config(
//        "spark.hadoop.hive.metastore.uris",
//        "thrift://hive-metastore-0.hive-metastore-hs.hive-metastore.svc:9083,thrift://hive-metastore-1.hive-metastore-hs.hive-metastore.svc:9083"
//      )
//      .config("spark.hadoop.hive.metastore.uri.selection", "RANDOM")
//      .config("spark.hadoop.hive.input.format", "io.delta.hive.HiveInputFormat")
//      .config("spark.hadoop.hive.tez.input.format", "io.delta.hive.HiveInputFormat")
//      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//      .config("spark.hadoop.fs.s3a.path.style.access", "true")
//      .config("spark.hadoop.fs.s3a.access.key", "spark-jobs")
//      .config("spark.hadoop.fs.s3a.secret.key", "1xnNUDM44rWk0V0PmuLYhgd8CCeC95FM")
//      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      .config("spark.hadoop.fs.s3a.fast.upload", "true")
//      .config("spark.hadoop.fs.s3a.endpoint", "minio-v2.palefat.fozzy.lan")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

  }
}
