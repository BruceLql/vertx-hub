package fof.daq.hub.web.chain

import fof.daq.hub.Address
import fof.daq.hub.common.customer
import fof.daq.hub.common.logger
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.utils.MD5
import fof.daq.hub.common.value
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import fof.daq.hub.service.CacheService
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.sockjs.BridgeEvent
import io.vertx.rxjava.core.Vertx
import org.springframework.beans.factory.annotation.Autowired
import rx.Observable
import rx.Single

abstract class AbstractChain(val vertx: Vertx, val crawlerServer: CrawlerServer){
    val SESSION_CUSTOMER = "SESSION_CUSTOMER"
    val log = logger(this::class)
    private val logUtils = LogUtils(this::class.java)
    private val eb = vertx.eventBus()
    private val listTimeClose = mutableMapOf<String, Long>()
    abstract fun bridge(event: BridgeEvent): Observable<BridgeEvent>

    @Autowired
    private lateinit var cacheService: CacheService


    /**
     * SOCK连接事件
     * */
    fun socketCreate(sessionId: String, customer: JsonObject) {
        listTimeClose[sessionId]?.let(vertx::cancelTimer).also {
            customer.value<String>("mobile")?.also { mobile ->
                logUtils.trace(mobile, "[SOCKET重连接] 清除销毁定时器成功", customer)
            }
        }
    }

    /**
     * 连接端口关闭事件
     * */
    fun socketClose(sessionId: String, customer: JsonObject){
        listTimeClose[sessionId]?.let(vertx::cancelTimer) // 取消原有定时器，如果有
        val uuid = customer.value<String>("uuid")
        val mobile = customer.value<String>("mobile")
        if (uuid != null && mobile != null) {
            val delay: Long = 1000 * 60 // 默认一分钟
            logUtils.info(mobile, "[SOCKET关闭] 设定:${delay}ms后触发销毁定时器事件", customer)
            // 添加定时器
            listTimeClose[sessionId] = vertx.setTimer(delay){
                logUtils.trace(mobile, "[SOCKET关闭] 触发销毁事件", customer)
                crawlerServer.server(uuid){ am , _customer ->
                    if (_customer?.sessionId == sessionId) { // 判断是否同一个会话服务
                        am.rxRemove(uuid).map { _customer }
                                .doOnSuccess { logUtils.trace(mobile, "[SOCKET关闭] 成功执行删除集群UUID:$uuid", _customer) }
                                .doAfterTerminate { this.closeNotice(_customer) }
                    } else {
                        Single.just(_customer)
                                .also { logUtils.trace(mobile, "[SOCKET关闭] 未删除集群UUID[$uuid]不一致的SessionId:$sessionId", _customer) }
                    }
                }
                .doOnError { logUtils.error(mobile, "[SOCKET关闭] 异常错误", it) }
                .subscribe()
            }
        }
    }

    /**
     * 关闭通知
     * */
    private fun closeNotice(customer: Customer) {
        val address = Address.CW.listen(customer.uuid)
        val message = Address.CW.action(Address.Action.CLOSE)
        logUtils.trace(customer.mobile, "[SOCKET关闭] 通知请求 地址:$address", message)
        eb.send<JsonObject>(address, message,  DeliveryOptions().customer(customer.toJson())){ ar ->
            if(ar.succeeded()) {
                logUtils.trace(customer.mobile, "[SOCKET关闭] 通知成功 地址:$address", message)
            } else {
                logUtils.failed(customer.mobile, "[SOCKET关闭] 通知失败 地址:$address", ar.cause(), message)
            }
        }
    }

    /**
     * 重载头部信息， 将用户信息注入头部，将原有headers放在临时headers里
     * */
    protected fun reHeaders(event: BridgeEvent): Observable<BridgeEvent> {
        event.rawMessage.value<String>("address")?.let { address ->
            // 如果是回复数据跳过合并整理
            if (address.startsWith("__vertx.reply.")) {
                return Observable.just(event)
            }
        }
        val principal = event.socket()?.webUser()?.principal()
        val body = event.rawMessage?.value<JsonObject>("body")
        // 从请求参数中获取(mobile/isp)，没有就从token中获取
        val mobile:String? = body?.value<String>("mobile") ?: principal?.value<String>("mobile")
            mobile ?: return Observable.error(NullPointerException("Mobile is null"))
        val isp:String? = body?.value<String>("isp") ?: principal?.value<String>("isp")
            isp ?: return Observable.error(NullPointerException("ISP is null"))
        // 读取session中的记录
        val oldCustomer = try { event.socket()?.webSession()?.get<JsonObject>(SESSION_CUSTOMER) } catch (e: Exception) { null }
        if (oldCustomer != null) {
            oldCustomer.put("mobile", mobile)
            oldCustomer.put("isp", isp)
            oldCustomer.put("sessionId", event.socket()?.webSession()?.oldId())
            oldCustomer.put("headers", event.rawMessage.value<JsonObject>("headers")?.toString())
            event.rawMessage.put("headers", oldCustomer)
            log.info("[SOCK BY SEND] read from session: $oldCustomer")
            return Observable.just(event)
        }
        return try {
            val authId = event.socket()?.headers()?.get("Authorization")
                authId ?: throw NullPointerException("Authorization is null")

            val uuid = MD5.digest(authId)
                uuid ?: throw NullPointerException("UUID generation error")

            val sessionId = event.socket()?.webSession()?.oldId()
                sessionId ?: throw NullPointerException("Session id is null")

            val customer = Customer(uuid = uuid, mobile= mobile, isp= isp, sessionId = sessionId)
            // 获取token的登入信息
            principal?.apply { customer.oid = this.value<String>("oid") } // 保存订单ID,如果存在
            principal?.apply { customer.sid = this.value<String>("sid") } // 保存商户ID,如果存在
            principal?.apply { customer.pid = this.value<String>("pid") } // 保存平台ID,如果存在

            //保存新增参数
            val mobile = principal?.value<String>("mobile") ?: throw NullPointerException("Mobile id is null")
            val userId = principal?.value<String>("userId") ?: throw NullPointerException("UserId id is null")
            val callBack = principal?.value<String>("callBack") ?: throw NullPointerException("CallBack id is null")
            val name = principal?.value<String>("name") ?: throw NullPointerException("Name id is null")
            val cid = principal?.value<String>("cid") ?: throw NullPointerException("Cid id is null")
            val notifyUrl = principal?.value<String>("notifyUrl") ?: throw NullPointerException("NotifyUrl id is null")
            val nonce = principal?.value<Number>("nonce") ?: throw NullPointerException("Nonce id is null")
            val reqData = JsonObject()
                    .put("mobile",mobile)
                    .put("userId", userId)
                    .put("backUrl", callBack)
                    .put("name", name)
                    .put("cid", cid)
                    .put("notifyUrl", notifyUrl)
                    .put("nonce", nonce)
            cacheService.putH5ReqParams(uuid,reqData).subscribe()

            // 临时存储headers信息
            customer.headers = event.rawMessage.value<JsonObject>("headers")?.toString()
            customer.createdAt = System.currentTimeMillis()
            val newCustomer = customer.toJson()
            logUtils.info(mobile, "[SOCKET访问] 客户信息验证通过", newCustomer)
            event.rawMessage.put("headers", newCustomer)
            // 保存新用户至session
            event.socket()?.webSession()?.put(SESSION_CUSTOMER, newCustomer)
            Observable.just(event)
        } catch (e: Exception) {
            log.error("[SOCK BY SEND] failed", e)
            Observable.error(e)
        }
    }
}
