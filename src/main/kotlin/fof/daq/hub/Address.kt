package fof.daq.hub

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
        const val LISTEN = PREFIX + "HUB.CLIENT.LISTEN."
        // 使用MID作通信标识
        fun listen(mid: String) = LISTEN + mid
        fun event(event: Event, body: JsonObject? = null): JsonObject {
            return JsonObject().put("EVENT", event.name).apply { body?.let { this.mergeIn(it) } }
        }
    }
    /**
     * 采集服务器地址
     * */
    object CW {
        const val PATH = PREFIX + "CW.SERVER."
        const val LISTEN =  PATH + "LISTEN."

        fun listen(uuid: String) = LISTEN + uuid
        fun action(action: Action, body: JsonObject? = null): JsonObject {
            return JsonObject().put("ACTION", action.name).apply { body?.let { this.mergeIn(it) } }
        }
    }

    /**
     * JAVA通知给Crawler服务的行为（内容存放在body传递）
     * */
    enum class Action{
        CHECK, // 检查是否活跃
        STOP,  // 强制停止
        CLOSE, // H5关闭通知
    }

    /**
     * Crawler消息通知JAVA（内容存放在body）
     * */
    enum class Event{
        CLOSE,  // 关闭服务
        MESSAGE // 消息服务
    }
}

