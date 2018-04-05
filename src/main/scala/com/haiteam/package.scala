package com

package object haiteam {

  ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
  // 접속정보 설정
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl" // poly server1
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "KOPO_CHANNEL_SEASONALITY_EX"

  // jdbc (java database connectivity) 연결
  var selloutDataFromOracle2= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  selloutDataFromOracle2.createOrReplaceTempView("selloutTable")
  selloutDataFromOracle2.show()

  // 임시 테이블 drio&생성
  spark.catalog.dropTempView("maindata")
  selloutDataFromOracle2.createTempView("maindata")

  // 데이터 타입 확인
  selloutDataFromOracle2.schema

  // 데이터 변환
  // cast([컬럼명] as [바꿀타입명])
  var data1 = spark.sql("select regionid, productgroup," + " yearweek, cast(qty as double)  from maindata ")
  data1.schema

}
