package com.tusa.map

data class LayerSqlData(
    val geom: String,
    val table: String,
    val additionalFilter: String,
    val additionalFields: String,
    val layerName: String,
    val simplifyEnabled: Boolean
)