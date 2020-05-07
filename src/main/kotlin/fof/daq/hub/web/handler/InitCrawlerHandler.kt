package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.customer
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.value
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.MultiMap
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.core.shareddata.AsyncMap
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Controller
import rx.Single
import java.util.concurrent.TimeUnit

/**
 * 初始化注册采集服务器
 * */
@Controller
class InitCrawlerHandler @Autowired constructor(
        private val crawlerServer: CrawlerServer,
        private val eb: EventBus,
        @Qualifier("config") val config: JsonObject
) : AbstractConsumerHandler() {

    // 验证Crawler服务重试次数
    private val CHECK_RETRY_TIME: Long = 3
    // 关闭通知超时
    private val NOTICE_CLOSE_TIMEOUT: Long = 5
    // 关闭通知重试次数
    private val NOTICE_CLOSE_RETRY: Long = 1

    private val logUtils = LogUtils(this::class.java)

    /**
     * 消息处理
     * */
    override fun consumer(customer: Customer? , message: Message<JsonObject>) {
        // 获取经过封装的客户信息
        if (customer == null){
            log.error(NullPointerException("Customer is null"))
            message.fail(0, "Customer is null")
            return
        }
        val body =  message.body()
        body.value<String>("mobile")?.also { customer.mobile = it }
        body.value<String>("isp")?.also { customer.isp = it }

        logUtils.trace(customer.mobile, "[初始化采集服务] 请求 Body:$body", customer)
        // 先取消原有加载程序
        listInitObservable[customer.uuid]?.unsubscribe()
        // 注册或获取采集服务器记录
        listInitObservable[customer.uuid] = crawlerServer.server(customer.uuid){ am, oldCustomer ->
            this.customerController(am, oldCustomer, customer)
        }
        .doAfterTerminate { listInitObservable.remove(customer.uuid) }
        .subscribe({ _customer ->
            when(_customer){
                null -> {
                    logUtils.error(customer.mobile, "[初始化采集服务] 处理失败", NullPointerException("Customer is null"))
                    message.fail(1, "[分配失败] Customer is null")
                }
                else -> {
                    logUtils.info(_customer.mobile, "[初始化采集服务] 执行成功", _customer)
                    message.reply(_customer.reply())
                }
            }
        },{
            logUtils.error(customer.mobile, "[初始化采集服务] 分配错误", it)
            message.fail(1, it.message)
        })
    }

    /**
     * 客户条件处理
     * */
    private fun customerController(am: AsyncMap<String, JsonObject>, oldCustomer: Customer?, customer: Customer): Single<Customer?> {
        if (oldCustomer == null) {
            logUtils.info(customer.mobile, "[初始化采集服务] 集群无客户,执行搜寻采集服务事件", customer)
            return buildCrawler(customer)
                   .flatMap { _customer -> // 保存至集群服务器
                       am.rxPut(_customer.uuid, _customer.toJson()).map { _customer }
                   }
        }
        val oldMid = oldCustomer.mid
        val oldSessionId = oldCustomer.sessionId
        // 判断集群存储的sessionId是否和当前新的sessionId一致
        if (oldSessionId != customer.sessionId) {
            oldCustomer.sessionId = customer.sessionId
        }
        return if (oldCustomer.mobile != customer.mobile || oldCustomer.isp != customer.isp) {
            logUtils.info(customer.mobile, "[初始化采集服务] 集群已存在客户,信息变更执行重分配服务(mobile:${customer.mobile}，isp:${customer.isp})", oldCustomer)
            updateCrawlerServer(oldCustomer, customer)
            .flatMap { _customer ->
                logUtils.trace(_customer.mobile, "[初始化采集服务] 更新数据", _customer)
                am.rxReplace(oldCustomer.uuid, _customer.toJson()).map { _customer }
            }
        } else {
            logUtils.info(oldCustomer.mobile, "[初始化采集服务] 集群已存在客户,检查采集服务是否运行", oldCustomer)
            checkCrawlerServer(oldCustomer)
            .flatMap { _customer ->
                if(oldMid != _customer.mid || oldSessionId != customer.sessionId) {
                    logUtils.trace(_customer.mobile, "[初始化采集服务] 更新数据", _customer)
                    am.rxReplace(_customer.uuid, _customer.toJson()).map { _customer }
                } else {
                    Single.just(_customer)
                }
            }
        }.doOnSuccess {
            oldSessionId?.also { id ->
                // 会话ID不一致通知关闭旧会话ID
                if (id != it.sessionId) this.closeOldSocket(it, id)
            }
        }
    }

    /**
     * 通知原有SOCKET关闭,原有socket根据session判断是否断开
     * */
    private fun closeOldSocket(customer: Customer, sessionId: String) {
        customer.mid?.also { mid ->
            val address = Address.WEB.LISTEN + mid
            logUtils.trace(customer.mobile, "[通知SOCKET关闭服务] SessionID:$sessionId 地址:$address")
            eb.publish(address, JsonObject().put("method", "stop").put("session_id", sessionId))
        }
    }

    /**
     * 通知原有采集服务终止关闭
     * 重选服务
     * */
    private fun updateCrawlerServer(oldCustomer: Customer, newCustomer: Customer): Single<Customer> {
        val address = Address.CW.listen(oldCustomer.uuid)
        val message = Address.CW.action(Address.Action.STOP)
        val options = DeliveryOptions().customer(oldCustomer.toJson())
        return this.eb.rxSend<JsonObject>(address, message, options) // 发送通知关闭服务
                .timeout(NOTICE_CLOSE_TIMEOUT, TimeUnit.SECONDS) // 限定5秒
                .retry(NOTICE_CLOSE_RETRY) // 重试1次
                .doOnSuccess { logUtils.trace(oldCustomer.mobile, "[采集服务关闭通知] 成功", it.body()) }
                .doOnError { logUtils.failed(oldCustomer.mobile, "[采集服务关闭通知] 失败", it) }
                .map { null }
                .onErrorResumeNext { Single.just(null) } // 无论通知失败与否必须重分配
                .flatMap {
                    logUtils.info(newCustomer.mobile, "[重分配采集服务]", newCustomer)
                    this.buildCrawler(newCustomer)
                }
    }


    /**
     * 验证服务是否正在运行
     * */
    private fun checkCrawlerServer(oldCustomer: Customer): Single<Customer> {
        val address = Address.CW.listen(oldCustomer.uuid)
        val message = Address.CW.action(Address.Action.CHECK)
        val options = DeliveryOptions().customer(oldCustomer.toJson(), 5)
        logUtils.trace(oldCustomer.mobile, "[检查采集服务] 地址: $address", oldCustomer)
        return eb.rxSend<JsonObject>(address, message, options)
                .doOnError {
                  logUtils.failed(oldCustomer.mobile, "[检查采集服务] 验证失败", it)
                }
                .retry(CHECK_RETRY_TIME) // 连接重时次数
                .map {
                    logUtils.trace(oldCustomer.mobile, "[检查采集服务] 服务在线", it.body())
                    oldCustomer
                }
                .onErrorResumeNext {
                    logUtils.error(oldCustomer.mobile, "[检查采集服务] 重试${CHECK_RETRY_TIME}次失败，开始尝试重分配", it)
                    buildCrawler(oldCustomer)
                }
    }

    /**
     * 选择采集服务器
     * */
    private fun buildCrawler(customer: Customer): Single<Customer> {
        val params = customer.toCrawler()
        val headers = MultiMap.caseInsensitiveMultiMap()
            headers.add("host", config.value("TCP.HOST", "127.0.0.1"))
            headers.add("port", config.value("TCP.PORT", 8080).toString())
        logUtils.trace(customer.mobile, "[分配采集服务] 准备请求 Headers: ${headers.toList()}", customer)
        return crawlerServer.register(params, headers)
                .map {
                    logUtils.info(customer.mobile, "[分配采集服务] 成功返回MID[${it.first}]", it.second)
                    customer.apply { this.mid = it.first }
                }.toSingle()
    }
}
