package com.tusa.map

import java.util.concurrent.atomic.AtomicInteger
import org.springframework.http.ContentDisposition
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class MapController(
    private val layersService: LayersService
) {
    private val logger = org.slf4j.LoggerFactory.getLogger(javaClass)
    private val cache: MutableMap<String, ByteArray> = mutableMapOf()
    private var atomicInteger: AtomicInteger = AtomicInteger(0)

    @GetMapping("api/v1/tile/{z}/{x}/{y}.mvt", produces = ["application/vnd.mapbox-vector-tile"])
    fun getTile(
        @PathVariable z: Int,
        @PathVariable x: Int,
        @PathVariable y: Int
    ): ResponseEntity<ByteArray> {
        var requestsCount = atomicInteger.incrementAndGet()
        val tileStr = "($z, $x, $y)"
        //logger.info("Request for tile $tileStr. req: $requestsCount")
        // Set headers for the .mvt response
        val headers = org.springframework.http.HttpHeaders()
        headers.contentType = MediaType("application", "vnd.mapbox-vector-tile")
        headers.contentDisposition = ContentDisposition.attachment().build()
        headers.setContentDispositionFormData("inline", "$z-$x-$y.mvt")

        val startTime = System.currentTimeMillis()

        val key = "$z-$x-$y"
        cache[key]?.let {
            val tileSizeStr = "${String.format("%.1f", it.size / 1024.0)}KB "
            requestsCount = atomicInteger.decrementAndGet()
            logger.info("$tileStr from cache ($tileSizeStr). req: $requestsCount")
            return ResponseEntity.ok()
                .headers(headers)
                .body(it)
        }

        val water                   =     layersService.oceanPolygons(z, x, y)
        val landCover               =     layersService.landCover(z, x, y)
        val roads                   =     layersService.road(z, x, y)
        val buildings               =     layersService.buildings(z, x, y)
        val landuse                 =     layersService.landuse(z, x, y)
        val innerWater              =     layersService.innerWater(z, x, y)
        val rivers                  =     layersService.rivers(z, x, y)
        val placeLabel              =     layersService.placeLabels(z, x, y)
        val admin                   =     layersService.admin(z, x, y)

        // Собираем слои в один ответ
        val layers = water + landCover + roads + buildings + landuse + innerWater + rivers +
                placeLabel + admin
        cache[key] = layers

        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime

        val tileKBSize = layers.size / 1024.0
        val tileSizeStr = "Tile size: ${String.format("%.1f", tileKBSize)}KB "
        val timeStr = "Time: $elapsedTime ms"
        requestsCount = atomicInteger.decrementAndGet()

        logger.info("$tileSizeStr $tileStr  $timeStr req: $requestsCount")
        return ResponseEntity.ok()
            .headers(headers)
            .body(layers)
    }
}