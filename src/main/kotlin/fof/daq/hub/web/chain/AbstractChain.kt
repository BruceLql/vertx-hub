package fof.daq.hub.web.chain

import fof.daq.hub.Address
import fof.daq.hub.common.logger
import fof.daq.hub.common.utils.MD5
import fof.daq.hub.common.value
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.sockjs.BridgeEvent
import io.vertx.rxjava.core.Vertx
import rx.Observable

abstract class AbstractChain(val vertx: Vertx, val crawlerServer: CrawlerServer){
    val SESSION_CUSTOMER = "SESSION_CUSTOMER"
    val log = logger(this::class)
    val eb = vertx.eventBus()
    val listTimeClose = mutableMapOf<String, Long>()
    abstract fun bridge(event: BridgeEvent): Observable<BridgeEvent>

    fun sockCreate(customer: JsonObject) {
        customer.value<String>("uuid")?.also {  uuid ->
            println("cancel time")
            listTimeClose[uuid]?.let(vertx::cancelTimer) // 取消原有定时器，如果有
        }
    }

    /**
     * 连接端口等待1分钟
     * */
    fun sockClose(customer: JsonObject){
        customer.value<String>("uuid")?.also {  uuid ->
            val delay: Long = 1000 * 60
            log.info("[CLOSE] UUID[$uuid] waiting: $delay")
            listTimeClose[uuid]?.let(vertx::cancelTimer) // 取消原有定时器，如果有
            // 添加定时器
            listTimeClose[uuid] = vertx.setTimer(delay){
                // 通知采集服务器用户已关闭
                println("ffffffffffffffff")
                crawlerServer.destory(uuid) // 销毁集群记录
                .flatMap {
                    eb.rxSend<JsonObject>(Address.CW.listen(uuid), Address.CW.action(Address.Action.CLOSE, customer))
                }.doAfterTerminate {
                    listTimeClose.remove(uuid) // 删除列表记录
                }.subscribe{
                        // todo
                }
            }
        }
    }

    /**
     * 重载头部信息， 将用户信息注入头部，将原有headers放在临时headers里
     * */
    protected fun reHeaders(event: BridgeEvent): Observable<BridgeEvent> {
        log.info("[SOCK-JS] start register")
        // 读取session中的记录
        val oldCustomer = try { event.socket()?.webSession()?.get<JsonObject>(SESSION_CUSTOMER) } catch (e: Exception) { null }
        if (oldCustomer != null) {
            oldCustomer.put("headers", event.rawMessage.value<JsonObject>("headers"))
            event.rawMessage.put("headers", oldCustomer)
            log.info("[SOCK-JS] Old customer: $oldCustomer")
            return Observable.just(event)
        }

        val authId = event.socket()?.headers()?.get("Authorization")
            authId ?: return Observable.error(NullPointerException("Authorization is null"))

        val uuid = MD5.digest(authId)
            uuid ?: return Observable.error(NullPointerException("UUID generation error"))

        val sessionId = event.socket()?.webSession()?.oldId()
            sessionId ?: return Observable.error(NullPointerException("Session id is null"))

        val principal = event.socket()?.webUser()?.principal()
        val body = event.rawMessage?.value<JsonObject>("body")
        // 从请求参数中获取(mobile/isp)，没有就从token中获取
        val mobile:String? = body?.value<String>("mobile") ?: principal?.value<String>("mobile")
            mobile ?: return Observable.error(NullPointerException("Mobile is null"))
        val isp:String? = body?.value<String>("isp") ?: principal?.value<String>("isp")
            isp ?: return Observable.error(NullPointerException("ISP is null"))

        val customer = Customer(uuid = uuid, sessionId = sessionId, mobile= mobile, isp= isp)
        // 获取token的登入信息
        principal?.apply { customer.oid = this.value<String>("oid") } // 保存订单ID,如果存在
        principal?.apply { customer.sid = this.value<String>("sid") } // 保存商户ID,如果存在
        principal?.apply { customer.pid = this.value<String>("pid") } // 保存平台ID,如果存在
        // 临时存储headers信息
        customer.headers = event.rawMessage.value<JsonObject>("headers")
        customer.createdAt = System.currentTimeMillis()
        val newCustomer = customer.toJson()
        log.info("[SOCK-JS] New customer: $newCustomer")
        event.rawMessage.put("headers", newCustomer)
        // 保存新用户至session
        event.socket()?.webSession()?.put(SESSION_CUSTOMER, newCustomer)
        return Observable.just(event)
    }

    /**
     * 匹配验证地址
     * */
    fun checkAddress(event: BridgeEvent, address: String): Boolean {
        return try {
            event.rawMessage.value<String>("address") == address
        } catch (e: Exception) {
            false
        }
    }

}