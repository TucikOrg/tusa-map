package com.tusa.map

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.queryForObject
import org.springframework.stereotype.Service

@Service
class LayersService(
    private val jbcTemplate: JdbcTemplate
) {

    // это слой для всей большой воды (Полигоны)
    // океаны, моря
    fun oceanPolygons(z: Int, x: Int, y: Int): ByteArray {
        val waterTables = mapOf(
            0 to "ocean_small_zoom_preset",
            1 to "ocean_small_zoom_preset",
            2 to "ocean_small_zoom_preset",
            3 to "ocean_small_zoom_preset",
            4 to "ocean_small_zoom_preset",
            5 to "water_big_zoom_preset_simplify",
            6 to "water_big_zoom_preset_simplify",
            7 to "water_big_zoom_preset_simplify",
            8 to "water_big_zoom_preset_simplify",
            9 to "water_big_zoom_preset_simplify",
            10 to "water_big_zoom_preset",
            11 to "water_big_zoom_preset",
            12 to "water_big_zoom_preset",
            13 to "water_big_zoom_preset",
            14 to "water_big_zoom_preset",
            15 to "water_big_zoom_preset",
            16 to "water_big_zoom_preset",
        )

        val geomTables = mapOf(
            0 to "ST_Simplify(geom, 20000)",
            1 to "ST_Simplify(geom, 17000)",
            2 to "ST_Simplify(geom, 15000)",
            3 to "ST_Simplify(geom, 5000)",
            4 to "geom",
            5 to "geom",
            6 to "geom",
            7 to "geom",
            8 to "geom",
            9 to "geom",
            10 to "geom",
            11 to "geom",
            12 to "geom",
            13 to "geom",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val simplifyTable = mapOf(
            0 to true,
            1 to true,
            2 to true,
            3 to true,
            4 to true,
            5 to true,
            6 to true,
            7 to true,
            8 to true,
            9 to true,
            10 to true,
            11 to true,
            12 to true,
            13 to true,
            14 to true,
            15 to false,
            16 to false,
        )

        val geom = geomTables[z]?: return ByteArray(0)
        val waterTable = waterTables[z]?: return ByteArray(0)
        val oceanQuery = makeQuery(geom, waterTable, z, x, y, "water", ", ogc_fid as id",
            simplify = simplifyTable[z]?: true
        )
        val oceanPolygons = jbcTemplate.queryForObject<ByteArray>(oceanQuery)
        return oceanPolygons
    }

    fun innerWater(z: Int, x: Int, y: Int): ByteArray {
        val waterTables = mapOf(
            9 to "water",
            10 to "water",
            11 to "water",
            12 to "water",
            13 to "water",
            14 to "water",
            15 to "water",
            16 to "water",
        )

        val geomTables = mapOf(
            9 to "ST_Simplify(geom, 100)",
            10 to "ST_Simplify(geom, 50)",
            11 to "geom",
            12 to "geom",
            13 to "geom",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val additionalFilterTable = mapOf(
            7 to "and ST_Area(geom) > 1000",
        )

        val geom = geomTables[z]?: return ByteArray(0)
        val waterTable = waterTables[z]?: return ByteArray(0)
        val additionalFilter = additionalFilterTable[z]?: ""
        val waterQuery = makeQuery(geom, waterTable, z, x, y, "water_inner",
            additionalFilter = additionalFilter
        )
        val water = jbcTemplate.queryForObject<ByteArray>(waterQuery)
        return water
    }

    fun rivers(z: Int, x: Int, y: Int): ByteArray {
        return ByteArray(0)
        val waterTables = mapOf(
            9 to "rivers",
            10 to "rivers",
            11 to "rivers",
            12 to "rivers",
            13 to "rivers",
            14 to "rivers",
            15 to "rivers",
            16 to "rivers",
        )

        val geomTables = mapOf(
            8 to "ST_Simplify(geom, 10000)",
            9 to "geom",
            10 to "geom",
            11 to "geom",
            12 to "geom",
            13 to "geom",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val geom = geomTables[z]?: return ByteArray(0)
        val waterTable = waterTables[z]?: return ByteArray(0)
        val waterQuery = makeQuery(geom, waterTable, z, x, y, "river")
        val water = jbcTemplate.queryForObject<ByteArray>(waterQuery)
        return water
    }

    // Это покрытие лесное планеты (Полигоны)
    fun landCover(z: Int, x: Int, y: Int): ByteArray {

        val geomTable = mapOf(
            0 to "ST_Simplify(geom, 40000)",
            1 to "ST_Simplify(geom, 17000)",
            2 to "ST_Simplify(geom, 10000)",
            3 to "geom",
        )

        val tables = mapOf(
            0 to "landcover_preset",
            1 to "landcover_preset",
            2 to "landcover_preset",
        )

        val additionalFields = ""
        val geom = geomTable[z]?: return ByteArray(0)
        val table = tables[z]?: return ByteArray(0)

        val landCoverQuery = makeQuery(geom, table, z, x, y, "landcover",
            additionalFields = additionalFields
        )
        val landCover = jbcTemplate.queryForObject<ByteArray>(landCoverQuery)
        return landCover
    }

    // Это дороги (Линии)
    fun road(z: Int, x: Int, y: Int): ByteArray {
        val additionalFieldsTables = mapOf(
            3 to ", type as class",
            4 to ", type as class",
            5 to ", type as class",
            6 to ", type as class",
            7 to ", type as class",
            8 to ", type as class",
            9 to ", type as class",
            10 to ", type as class",
            11 to ", type as class",
            12 to ", type as class",
            13 to ", type as class, name",
            14 to ", type as class, name",
            15 to ", type as class, name",
            16 to ", type as class, name",
        )

        val zoomTables = mapOf(
            3 to "roads_small_zoom_3",
            4 to "roads_small_zoom_3",
            5 to "roads_small_zoom",
            6 to "roads_small_zoom",
            7 to "roads_small_zoom",
            8 to "roads_small_zoom",
            9 to "roads_small_zoom",
            10 to "roads_simplify",
            11 to "roads_simplify",
            12 to "roads_simplify",
            13 to "roads",
            14 to "roads",
            15 to "roads",
            16 to "roads",
        )

        val geomTables = mapOf(
            3 to "ST_Simplify(geom, 50000)",
            4 to "geom",
            5 to "ST_Simplify(geom, 40000)",
            6 to "ST_Simplify(geom, 30000)",
            7 to "ST_Simplify(geom, 13000)",
            8 to "ST_Simplify(geom, 3000)",
            9 to "geom",
            10 to "ST_Simplify(geom, 20000)",
            11 to "ST_Simplify(geom, 10000)",
            12 to "geom",
            13 to "ST_Simplify(geom, 5000)",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val additionalFilterTable = mapOf(
            5 to "and type in ('motorway', 'trunk', 'primary')",
            6 to "and type in ('motorway', 'trunk', 'primary')",
        )

        val geom = geomTables[z]?: return ByteArray(0)
        val table = zoomTables[z]?: return ByteArray(0)
        val additionalFields = additionalFieldsTables[z]?: ""
        val additionalFilter = additionalFilterTable[z]?: ""

        val roadsQuery = makeQuery(geom, table, z, x, y, "road",
            additionalFields = additionalFields,
            additionalFilter = additionalFilter
        )
        val roads = jbcTemplate.queryForObject<ByteArray>(roadsQuery)
        return roads
    }

    fun buildings(z: Int, x: Int, y: Int): ByteArray {
        val geomTable = mapOf(
            13 to "geom",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val tables = mapOf(
            13 to "buildings_simplify25x25",
            14 to "buildings_simplify25x25",
            15 to "buildings",
            16 to "buildings",
        )
        val table = tables[z]?: return ByteArray(0)

        val geom = geomTable[z]?: return ByteArray(0)
        val buildingsQuery = makeQuery(geom, table, z, x, y, "buildings")
        val buildings = jbcTemplate.queryForObject<ByteArray>(buildingsQuery)
        return buildings
    }

    fun placeLabels(z: Int, x: Int, y: Int): ByteArray {
        val geomTable = mapOf(
            0 to "geom",
            1 to "geom",
            2 to "geom",
            3 to "geom",
            4 to "geom",
            5 to "geom",
            6 to "geom",
            7 to "geom",
            8 to "geom",
            9 to "geom",
            10 to "geom",
            11 to "geom",
            12 to "geom",
            13 to "geom",
        )

        val tables = mapOf(
            0 to "place_labels",
            1 to "place_labels",
            2 to "place_labels",
            3 to "place_labels",
            4 to "place_labels",
            5 to "place_labels",
            6 to "place_labels",
            7 to "place_labels",
            8 to "place_labels",
            9 to "place_labels",
            10 to "place_labels",
            11 to "place_labels",
            12 to "place_labels",
            13 to "place_labels",
        )

        val filterTable = mapOf(
            1  to "and name is not null and type in ('ocean', 'continent')",
            2  to "and name is not null and type in ('country') and (rank <= 0 or rank is null)",
            3  to "and name is not null and ( type in ('country', 'sea') or (rank <= 0 and type in ('city') ))",
            4  to "and name is not null and ( type in ('sea', 'country', 'city', 'state') and population > 1000000)",
            5  to "and name is not null and ( type in ('sea', 'city', 'town', 'province', 'state', 'island') and population > 300000)",
            6  to "and name is not null and ( type in ('sea', 'city', 'town', 'island', 'province', 'quarter') and population > 100000)",
            7  to "and name is not null and ( type in ('sea', 'city', 'town', 'island', 'province', 'quarter', 'village', 'neighbourhood', 'quarter', 'suburb', 'municipality') and population > 20000 and population < 800000)",
            8  to "and name is not null and ( population >= 10000 and population < 400000) or type in ('quarter')",
            9  to "and name is not null and ( type in ('village', 'neighbourhood', 'district') and population >= 0 and population < 300000)",
            10  to "and name is not null and ( type in ('village', 'neighbourhood', 'district', 'suburb') and population >= 0 and population < 300000)",
            11  to "and name is not null and ( type in ('village', 'hamlet', 'neighbourhood', 'district', 'suburb', 'subdistrict') and population >= 0 and population < 200000)",
            12  to "and name is not null and population >= 0 and population < 300000",
            13  to "and name is not null and population >= 0 and population < 300000",
        )

        val table = tables[z]?: return ByteArray(0)
        val geom = geomTable[z]?: return ByteArray(0)
        val additionalFilter = filterTable[z]?: return ByteArray(0)
        val placeLabelsQuery = makeQuery(geom, table, z, x, y, "place_label",
            additionalFields = ", name, type, name_en, name_ru, population",
            additionalFilter = additionalFilter)
        val placeLabels = jbcTemplate.queryForObject<ByteArray>(placeLabelsQuery)
        return placeLabels
    }

    fun admin(z: Int, x: Int, y: Int): ByteArray {
        val geomTable = mapOf(
            1 to "ST_Simplify(geom, 10000)",
            2 to "ST_Simplify(geom, 10000)",
            3 to "ST_Simplify(geom, 5000)",
            4 to "ST_Simplify(geom, 5000)",
            5 to "ST_Simplify(geom, 3000)",
            6 to "ST_Simplify(geom, 1000)",
            7 to "geom",
            8 to "geom",
            9 to "geom",
            10 to "geom",
            11 to "geom",
            12 to "geom",
        )

        val tables = mapOf(
            1 to "administrative",
            2 to "administrative",
            3 to "administrative",
            4 to "administrative",
            5 to "administrative",
            6 to "administrative",
            7 to "administrative",
            8 to "administrative",
            9 to "administrative",
            10 to "administrative",
            11 to "administrative",
            12 to "administrative",
        )

        val additionalFilterTable = mapOf(
            1 to "and admin_level in ('0', '1', '2')",
            2 to "and admin_level in ('0', '1', '2')",
            3 to "and admin_level in ('0', '1', '2', '3', '4')",
            4 to "and admin_level in ('0', '1', '2', '3', '4')",
            5 to "and admin_level in ('0', '1', '2', '3', '4')",
            6 to "and admin_level in ('0', '1', '2', '3', '4')",
            7 to "and admin_level in ('0', '1', '2', '3', '4')",
            8 to "and admin_level in ('0', '1', '2', '3', '4')",
            9 to "and admin_level in ('0', '1', '2', '3', '4')",
            10 to "and admin_level in ('0', '1', '2', '3', '4')",
            11 to "and admin_level in ('0', '1', '2', '3', '4')",
            12 to "and admin_level in ('0', '1', '2', '3', '4')",
        )

        val table = tables[z]?: return ByteArray(0)
        val geom = geomTable[z]?: return ByteArray(0)
        val additionalFilter = additionalFilterTable[z]?: return ByteArray(0)

        val adminQuery = makeQuery(geom, table, z, x, y, "admin",
            additionalFields = ", type as class, admin_level, name",
            additionalFilter = additionalFilter
        )
        return jbcTemplate.queryForObject<ByteArray>(adminQuery)
    }

    fun landuse(z: Int, x: Int, y: Int): ByteArray {
        val geomTable = mapOf(
            13 to "geom",
            14 to "geom",
            15 to "geom",
            16 to "geom",
        )

        val tables = mapOf(
            13 to "landuse",
            14 to "landuse",
            15 to "landuse",
            16 to "landuse",
        )
        val table = tables[z]?: return ByteArray(0)

        val geom = geomTable[z]?: return ByteArray(0)
        val landuseQuery = makeQuery(geom, table, z, x, y, "landuse",
            additionalFields = ", type as class",
        )
        return jbcTemplate.queryForObject<ByteArray>(landuseQuery)
    }

    private fun makeQuery(
        geomStr: String,
        table: String,
        z: Int, x: Int, y: Int,
        layoutName: String,
        additionalFields: String = "",
        additionalFilter: String = "",
        simplify: Boolean = true
    ): String {
        val query: String = """
            WITH tile_bbox AS (
              SELECT ST_TileEnvelope($z, $x, $y) AS bbox
            )
            SELECT
              ST_AsMVT(q, '$layoutName', 4096, 'geom')
            FROM (
              SELECT
                ST_AsMVTGeom($geomStr, bbox, 4096, 256, $simplify) AS geom
                $additionalFields
              FROM $table, tile_bbox
              WHERE geom && bbox and ST_Intersects(
                geom,
                bbox
              ) $additionalFilter
            ) AS q;
        """.trimIndent()
        return query
    }
}