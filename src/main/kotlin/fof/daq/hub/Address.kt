package fof.daq.hub

import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject

object Address{
    const val PREFIX = "FOF.DAQ." // 默认全局前缀
    /**
     * Web 用地址
     * */
    object WEB {
        const val PATH = PREFIX + "HUB.SERVER."
        // 代理中心
        const val PROXY = PATH + "PROXY"
        // 服务初始化地址
        const val INIT = PATH + "INIT"
        // 客户监听地址
        const val LISTEN = PATH + "LISTEN."
    }
    /**
     * 采集服务器地址
     * */
    object CW {
        const val PATH = PREFIX + "CW.SERVER."
        const val LISTEN =  PATH + "LISTEN."

        fun listen(uuid: String) = LISTEN + uuid
        fun action(action: Action, body: JsonObject): JsonObject {
            return JsonObject().put("ACTION", action.name).put("data", body)
        }
    }

    enum class Action{
        CHECK, // 检查是否活跃
        STOP,  // 强制停止
        CLOSE  // H5关闭通知
    }
}

