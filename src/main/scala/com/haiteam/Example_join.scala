package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_join {

  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").
    getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"


  // 상대경로 입력
  var mainDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + mainData)
  var subDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + subData)

  mainDataDf.createOrReplaceTempView("mainData")
  subDataDf.createOrReplaceTempView("subData")

  // Left join : a.productgroup, b.productname
  var leftJoinData = spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
    "from mainData a " +
    "left join subData b " +
    "on a.productgroup = b.productid")


///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
// 접속정보 설정 (KOPO_CHANNEL_SEASONALITY_NEW)
var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl" // poly server1
var staticUser = "kopo"
var staticPw = "kopo"
var selloutDb1 = "KOPO_CHANNEL_SEASONALITY_NEW"

// jdbc (java database connectivity) 연결
var selloutDataFromOracle1= spark.read.format("jdbc").
options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

// 메모리 테이블 생성
selloutDataFromOracle1.createOrReplaceTempView("selloutTable1")
selloutDataFromOracle1.show()

// 접속정보 설정 (kopo_region_mst)
var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl" // poly server1
var staticUser = "kopo"
var staticPw = "kopo"
var selloutDb2 = "kopo_region_mst"

// jdbc (java database connectivity) 연결
var selloutDataFromOracle2= spark.read.format("jdbc").
options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

// 메모리 테이블 생성
selloutDataFromOracle2.createOrReplaceTempView("selloutTable2")
selloutDataFromOracle2.show()

  // Left join : a.regionid, b.regionid
var leftJoinData1 = spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname " +
"from selloutTable1 a " +
"left join selloutTable2 b " +
"on a.regionid = b.regionid")

  // Inner join : b.regionid -> a.regionid (작은 항을 기준으로 큰 항을 조인하는게 응답시간을 줄임
var innerJoinData1 = spark.sql("select b.regionid, b.regionname, a.product, a.yearweek, a.qty " +
"from selloutTable2 b " +
"inner join selloutTable1 a " +
"on a.regionid = b.regionid")
