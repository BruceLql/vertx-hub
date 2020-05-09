package fof.daq.hub.common.enums

/**
 * 服务状态
 */
enum class ServerState(var code: Int, var content: String) {
    UP(1, "可用"),
    UNAVAILABLE(2, "不可用"),
    DOWN(3, "下线")
}
