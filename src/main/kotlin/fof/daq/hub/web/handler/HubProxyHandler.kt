package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.utils.RetryWithNoHandler
import fof.daq.hub.common.value
import fof.daq.hub.model.Customer
import fof.daq.hub.service.CacheService
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.core.shareddata.SharedData
import io.vertx.rxjava.ext.web.client.WebClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Controller
import rx.Observable
import rx.Single
import java.util.concurrent.TimeUnit

/**
 * 初始化注册采集服务器
 * */
@Controller
class HubProxyHandler @Autowired constructor(
        private val eb: EventBus,
        private val webClient: WebClient,
        @Qualifier("config") val config: JsonObject,
        private val initCrawlerHandler: InitCrawlerHandler,
        private val cacheService: CacheService,
        private val sd: SharedData
) : AbstractConsumerHandler() {
    private val logUtils = LogUtils(this::class.java)

    override fun consumer(customer: Customer?, message: Message<JsonObject>) {
        if (customer == null) {
            log.error(NullPointerException("[HubProxy] Customer is null"))
            return
        }

        val mid = customer.mid
        if (mid == null) {
            logUtils.error(customer.mobile, "[消息代理-错误] 参数缺失", NullPointerException("Mid is null"))
            return
        }
        val address = Address.WEB.listen(mid)
        val body = message.body()
        logUtils.info(customer.mobile, "[接收到代理请求] body:$body", customer)
        //接收Crawler 关闭通知 重分配采集服务
        when (body.value<String>("EVENT")) {

            //py服务关闭触发，重新选择一台CW服务
            Address.Event.CLOSE.name -> {
                closeMessage(body, address, customer, message)
            }
            //完成之后，调用数据清洗服务接口
            Address.Event.DONE.name -> {
                doneMessage(address, mid, customer, body, message)
            }
            //停止，重新选择CW服务器
            Address.Event.STOP.name -> {
                stopMessage(body, address, customer, message)
            }
            //如果是timeout 事件清空sd内容
            Address.Event.TIMEOUT.name -> {
                tmeOutMessage(customer)
            }
            else -> messageProxy(body, address, customer, message)
        }
    }


    /**
     * 收到py done事件处理
     */
    private fun doneMessage(address: String, mid: String, customer: Customer, body: JsonObject, message: Message<JsonObject>) {
        logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:DONE")
        //回复py已接收完成通知
        message.reply(JsonObject().put("msg", "[已接收 EVENT:DONE 事件]"))
        val uuid = customer.uuid
        cacheService.getReqParams(uuid)
                .flatMap { json ->
                    val notifyUrl = json.value<String>("notifyUrl") ?: throw NullPointerException("Notify_url is null")
                    //再将消息代理到前端 （加入notifyUrl）一并返回
                    body.put("notifyUrl", "http://www.baidu.com")
                    messageProxy(body, address, customer, message)
                    //调用数据清洗服务
                    this.collectNotice(customer)
                }
                .flatMap {
                    logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:DONE 清空缓存内用户服务分配记录")
                    cacheService.clearServer(uuid)
                }.flatMap {
                    logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:DONE 清空缓存内用户请求参数记录")
                    cacheService.clearH5ReqParams(uuid)
                }.flatMap {
                    logUtils.trace(customer.mobile,"[接收到代理请求] EVENT:DONE 爬虫执行成功，缓存这次用户执行成功的记录")
                    //存入缓存记录
                    sd.rxGetLocalLockWithTimeout(customer.mobile,1000).toObservable().flatMap { lock ->
                        val data = JsonObject()
                                .put("mobile",customer.mobile)
                                .put("isp",customer.isp)
                                .put("lastTime",System.currentTimeMillis())
                        cacheService.putSuccessfulCustomer(customer.mobile,customer.isp,data)
                                .doAfterTerminate { lock.release() }
                    }
                }
                .subscribe()

    }


    /**
     * 收到py socket 关闭触发close事件处理，更换节点，并通知前端更新mid
     */
    private fun closeMessage(body: JsonObject, address: String, customer: Customer, message: Message<JsonObject>) {
        logUtils.trace(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE", customer)
        val uuid = customer.uuid
        sd.rxGetLocalLockWithTimeout(uuid, 1000).toObservable()
                .flatMap { lock ->
                    cacheService.listServerHistory(uuid)
                            .map { listHistory ->
                                logUtils.trace(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE] 变更当前服务器为不可用状态")
                                var allowLastServer = listHistory.filter {
                                    it.value.value("status", true)
                                }.toList().last()
                                cacheService.putServer(uuid, allowLastServer.second.put("status", false))
                            }
                            .doAfterTerminate { lock.release() }
                }
                .flatMap {
                    logUtils.trace(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE] 重新分配机器")
                    initCrawlerHandler.buildCrawler(customer).toObservable()
                }
                .flatMap { customer ->
                    logUtils.trace(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE] 重新分配机器完成，并通知前端更新mid")
                    this.noticeH5(body, address, customer, message).toObservable()
                }
                .subscribe({
                    //给前端发送 update mid 事件
                    var message = Address.WEB.event(Address.Event.UPDATE).put("mid", customer.mid)
                    eb.send<JsonObject>(address, message) { ar ->
                        if (ar.succeeded()) {
                            logUtils.trace(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE 重新分配机器完成，已成功发送更新Mid事件到前端")
                        } else {
                            logUtils.failed(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE 重新分配机器完成，并通知前端发生错误", ar.cause())
                        }
                    }
                }, { err ->
                    message.fail(1, err.message ?: "Unknown exception error")
                    logUtils.error(customer.mobile, "[socket关闭触发到代理请求] EVENT:CLOSE 重新分配机器完成，并通知前端发生错误", err, customer)
                })
    }


    /**
     * 收到py stop事件处理，更换节点，并通知前端更新mid
     */
    private fun stopMessage(body: JsonObject, address: String, customer: Customer, message: Message<JsonObject>) {
        logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP", customer)
        val uuid = customer.uuid
        sd.rxGetLocalLockWithTimeout(uuid, 1000).toObservable()
                .flatMap { lock ->
                    cacheService.listServerHistory(uuid)
                            .map { listHistory ->
                                logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP] 变更当前服务器为不可用状态")
                                var allowLastServer = listHistory.filter {
                                    it.value.value("status", true)
                                }.toList().last()
                                cacheService.putServer(uuid, allowLastServer.second.put("status", false))
                            }
                            .doAfterTerminate { lock.release() }
                }
                .flatMap {
                    logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP] 重新分配机器")
                    initCrawlerHandler.buildCrawler(customer).toObservable()
                }
                .flatMap { customer ->
                    logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP] 重新分配机器完成，并通知前端更新mid")
                    this.noticeH5(body, address, customer, message).toObservable()
                }
                .subscribe({
                    //给前端发送 update mid 事件
                    var message = Address.WEB.event(Address.Event.UPDATE).put("mid", customer.mid)
                    eb.send<JsonObject>(address, message) { ar ->
                        if (ar.succeeded()) {
                            logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP 重新分配机器完成，已成功发送更新Mid事件到前端")
                        } else {
                            logUtils.failed(customer.mobile, "[接收到代理请求] EVENT:STOP 重新分配机器完成，并通知前端发生错误", ar.cause())
                        }
                    }
                }, { err ->
                    message.fail(1, err.message ?: "Unknown exception error")
                    logUtils.error(customer.mobile, "[接收到代理请求] EVENT:STOP 重新分配机器完成，并通知前端发生错误", err, customer)
                })
    }


    /**
     * 通知H5
     */
    private fun noticeH5(body: JsonObject, address: String, customer: Customer, message: Message<JsonObject>): Single<Message<JsonObject>> {
        val timeout = message.headers().get("timeout")?.toLong() ?: 20 * 1000 // 前端默认等待超时时间
        val option = DeliveryOptions().setSendTimeout(timeout).setHeaders(message.headers().delegate)
        logUtils.info(customer.mobile, "[CW代理] 请求地址:$address / Headers: ${message.headers().toList()} / Body:${message.body()}", customer)
        return eb.rxSend<JsonObject>(address, body, option)
                .retryWhen(RetryWithNoHandler(1)) // 无地址重试等待间隔1秒
                .timeout(timeout, TimeUnit.MILLISECONDS) // 请求超时时间

    }


    /**
     * 收到py timeout事件清空sd内容
     */
    private fun tmeOutMessage(customer: Customer) {

        logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:TIMEOUT 清空服务历史记录")
        //清空sd
        cacheService.clearServer(customer.uuid)
                .doOnError {
                    it.printStackTrace()
                    log.error("[接收到代理请求] EVENT:TIMEOUT] 异常清空sd内容")
                }
                .subscribe()
    }


    /**
     * 消息代理
     */
    private fun messageProxy(body: JsonObject, address: String, customer: Customer, message: Message<JsonObject>) {
        val timeout = message.headers().get("timeout")?.toLong() ?: 20 * 1000 // 前端默认等待超时时间
        val option = DeliveryOptions().setSendTimeout(timeout).setHeaders(message.headers().delegate)
        logUtils.info(customer.mobile, "[CW代理] 请求地址:$address / Headers: ${message.headers().toList()} / Body:${message.body()}", customer)
        eb.rxSend<JsonObject>(address, body, option)
                .retryWhen(RetryWithNoHandler(1)) // 无地址重试等待间隔1秒
                .timeout(timeout, TimeUnit.MILLISECONDS) // 请求超时时间
                .subscribe({ msg ->
                    val replyOption = DeliveryOptions().setSendTimeout(20 * 1000)
                    logUtils.info(customer.mobile, "[CW代理] 回复地址:${message.replyAddress()} / Result:${msg.body()}", customer)
                    message.replyAndRequest<JsonObject>(msg.body(), replyOption) { ar ->
                        if (ar.succeeded()) {
                            msg.reply(JsonObject().put("status", "SUCCESS").put("message", "消息回复成功").put("result", ar.result().body()))
                            logUtils.trace(customer.mobile, "[CW代理] 回复成功 Body:${ar.result().body()}", customer)
                        } else {
                            msg.reply(JsonObject().put("status", "FAILED").put("message", "消息已回复，无返回信息").put("reason", ar.cause().message))
                            logUtils.failed(customer.mobile, "[CW代理] 回复失败", ar.cause(), customer)
                        }
                    }
                }, { err ->
                    message.fail(1, err.message ?: "Unknown exception error")
                    logUtils.error(customer.mobile, "[CW代理] 错误", err, customer)
                })
    }

    /**
     * 调用数据清洗服务
     */
    private fun collectNotice(customer: Customer): Observable<String> {
        logUtils.info(customer.mobile, "[调用数据清洗服务]", customer)
        val url = config.value<String>("COLLECT.NOTICE")
        if (url == null) log.error(NullPointerException("url is null"))
        val mid = customer.mid
        if (mid == null) {
            logUtils.error(customer.mobile, "[参数校验-错误] 参数缺失", NullPointerException("Mid is null"))
            return Observable.error(NullPointerException("Mid is null"))
        }
        return cacheService.getReqParams(customer.uuid).flatMap { json ->
            json.put("taskId", customer.mid)
            json.put("isCache",false)
            this.webClient.postAbs(url).rxSendJsonObject(json)
                    .toObservable()
                    .doOnNext { res ->
                        when (res.statusCode()) {
                            200 -> logUtils.info(customer.mobile, "[调用数据清洗服务已完成] ${res.bodyAsString()}")
                            else -> logUtils.error(customer.mobile, "[调用数据清洗服务失败！]", Exception("调用请求返回非200"))
                        }
                    }.map { it.bodyAsString() }
                    .doOnError { it.printStackTrace() }
                    .retryWhen(RetryWithNoHandler(1))  //如果失败就重试一次
        }
    }
}
