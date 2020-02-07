package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerLocationTest extends SqlizerTest {

  test("location latitude") {
    val soql = "select location_latitude(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_Y(t1.location_geom)::numeric) FROM t1")
    setParams.length should be (0)
  }

  test("location longitude") {
    val soql = "select location_longitude(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_X(t1.location_geom)::numeric) FROM t1")
    setParams.length should be (0)
  }

  test("location human_address") {
    val soql = "select location_human_address(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (t1.location_address) FROM t1")
    setParams.length should be (0)
  }

  test("location point") {
    val soql = "select location::point"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((t1.location_geom)) FROM t1")
    setParams.length should be (0)
  }

  test("location within_circle") {
    val soql = "select case_number where within_circle(location, 1.0, 2.0, 30)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be (
      """SELECT t1.case_number FROM t1
        | WHERE ((ST_within((t1.location_geom), ST_Buffer(ST_MakePoint(?, ?)::geography, ?)::geometry)))"""
        .stripMargin.replaceAll("\n", ""))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(2.0, 1.0, 30d).map(BigDecimal.valueOf(_)))
  }

  test("location within_box") {
    val soql = "select case_number where within_box(location, 1.0, 2.0, 3.0, 4.0)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be (
      """SELECT t1.case_number FROM t1
        | WHERE ((ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ (t1.location_geom)))"""
        .stripMargin.replaceAll("\n", ""))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(2.0, 3.0, 4.0, 1.0).map(BigDecimal.valueOf(_)))
  }

  test("subcolumn subscript converted") {
    val soql = """SELECT location.longitude as longitude WHERE location.latitude = 1.1 order by longitude"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_X(t1.location_geom)::numeric) FROM t1 WHERE ((ST_Y(t1.location_geom)::numeric) = ?) ORDER BY (ST_X(t1.location_geom)::numeric) nulls last")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(BigDecimal.valueOf(1.1)))
  }

  test("location ctor") {
    val soql = """SELECT location('point (2.2 1.1)'::point, '101 Main St', 'Seattle', 'WA', '98104')"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary(((ST_GeomFromText(?, 4326)))),(case when coalesce(?,?,?,?) is null then null else '{"address": ' || coalesce(to_json(?::text)::text, '""') || ', "city": ' || coalesce(to_json(?::text)::text, '""') || ', "state": ' || coalesce(to_json(?::text)::text, '""') || ', "zip": ' || coalesce(to_json(?::text)::text, '""') || '}' end) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("point (2.2 1.1)", "101 Main St", "Seattle", "WA", "98104", "101 Main St", "Seattle", "WA", "98104"))
  }

  /**
    * There is a postgis regression in slowness in group by geometry.
    * Converting it to binary gets around the perf problem.
    */
  test("group by geometry generates ST_AsBinary on geometry column in group by") {
    val soql = """SELECT snap_to_grid(polygon, 2) as snapped, count(*) GROUP BY snapped"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_SnapToGrid(t1.polygon, 2))),(count(*)) FROM t1 GROUP BY ST_AsBinary((ST_SnapToGrid(t1.polygon, 2)))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    setParams.length should be (0)
  }

  test("Group by using ST_AsBinary with having clause") {
    val soql = """SELECT polygon WHERE within_polygon(snap_to_grid(`polygon`, 0.9), 'POLYGON((-85 35, -85 35, -85 35, -85 35, -85 35))')"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    // if the group by clause uses st_asbinary(polygon) then the having should also use the
    // st_asbinary(polygon) so avoid the ploygon should be a part of group by
    sql should be ("""SELECT polygon within_polygon(snap_to_grid(st_asbinary(polygon), 0.9))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    setParams.length should be (0)
  }
}
