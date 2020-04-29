package fof.daq.hub.model

import io.vertx.core.json.JsonObject

abstract class AbstractModel{
    fun toJson() = JsonObject.mapFrom(this)
}