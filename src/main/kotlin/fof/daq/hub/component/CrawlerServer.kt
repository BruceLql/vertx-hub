package fof.daq.hub.component

import fof.daq.hub.common.exception.NoSuchServerException
import fof.daq.hub.common.logger
import fof.daq.hub.common.toEntity
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.utils.RetryWithTimeOut
import fof.daq.hub.common.value
import fof.daq.hub.model.Customer
import fof.daq.hub.model.Server
import fof.daq.hub.model.Server.Companion.PY_SERVER_KEY
import fof.daq.hub.service.HeartBeatService
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.MultiMap
import io.vertx.rxjava.core.buffer.Buffer
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.shareddata.AsyncMap
import io.vertx.rxjava.core.shareddata.SharedData
import io.vertx.rxjava.ext.web.client.HttpResponse
import io.vertx.rxjava.ext.web.client.WebClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.util.Base64Utils
import rx.Observable
import rx.Single
import rx.schedulers.Schedulers
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Controller
class CrawlerServer @Autowired constructor(
        private val client: WebClient,
        private val sd: SharedData,
        private val heartBeatService: HeartBeatService

) {

    // 连接超时时间
    private val CONNECT_TIMEOUT: Long = 10

    // 重连接次数
    private val CONNECT_RETRY = 1

    // 总超时时间
    private val CONNECT_TOTAL_TIMEOUT: Long = 20

    val CRAWLER_KEY = "_CRAWLER_UUID_SERVER_"

    private val log = logger(this::class)

    private val logUtils = LogUtils(this::class.java)


    /**
     * 读取集群中的客户信息
     * */
    fun server(uuid: String, body: ((AsyncMap<String, JsonObject>, Customer?) -> Single<Customer?>)? = null): Single<Customer?> {
        log.info("Start trying to read the records of the cluster by UUID[$uuid]")
        return sd.rxGetAsyncMap<String, JsonObject>(CRAWLER_KEY)
                .flatMap { am ->
                    am.rxGet(uuid)
                            .map { it?.toEntity<Customer>() }
                            .flatMap { customer ->
                                if (customer != null) {
                                    logUtils.trace(customer.mobile, "[集群SD存在记录] UUID[$uuid]", customer)
                                } else {
                                    log.info("No cluster record by UUID[$uuid]")
                                }
                                body?.invoke(am, customer) ?: Single.just(customer)
                            }
                }
    }

    /**
     * 从集群中销毁
     * */
    fun destroy(uuid: String): Single<JsonObject> {
        return sd.rxGetAsyncMap<String, JsonObject>(CRAWLER_KEY).flatMap { it.rxRemove(uuid) }
    }

    /**
     * 服务器注册
     * @return Pair(mongodbID , 源JSON数据)
     * */
    fun register(params: JsonObject, headers: MultiMap): Observable<Pair<String, JsonObject>> {
        log.info("[Start server assignment] set total connect Timeout:${CONNECT_TOTAL_TIMEOUT}s / Params:$params / Headers: ${headers.toList()}")

        val isp = params.value<String>("isp") ?: return Observable.error(NullPointerException("isp is null"))
        val uuid = params.value<String>("uuid") ?: return Observable.error(NullPointerException("uuid is null"))
        val mobile = params.value<String>("mobile") ?: return Observable.error(NullPointerException("mobile is null"))
        //计算tags值
        val calcTags = Server.calcTags(listOf(isp))
        return heartBeatService.listHeartBeat(calcTags).flatMap { listHeartBeat ->
            logUtils.trace(mobile,"[服务器分配] 数据库可用列表:${listHeartBeat.toList()}")
            Observable.defer {
                //获取可用服务器
                this.server(uuid, mobile, listHeartBeat)
                        .observeOn(Schedulers.io())
                        .flatMap { serverJson ->
                            println(Thread.currentThread().name)
                            logUtils.trace(mobile,"[服务器分配] 开始尝试连接服务器",serverJson)
                            sd.rxGetLocalLockWithTimeout(uuid,1000).toObservable().flatMap { lock ->
                                //尝试链接服务器
                                this.tryConnect(serverJson, params, headers, mobile, uuid)
                                        .doAfterTerminate { lock.release() }
                            }
                        }
            }.retryWhen { obs ->
                obs.flatMap { throwable ->
                    when (throwable) {
                        is NoSuchServerException -> {
                            log.error("[NoSuchServerException] End assignment. Params:$params / Headers: ${headers.toList()}", throwable)
                            Observable.error(throwable)
                        }
                        else -> {
                            log.warn("---- Try next server ----")
                            Observable.just(null)
                        }
                    }
                }
            }
            .timeout(CONNECT_TOTAL_TIMEOUT, TimeUnit.SECONDS) // 限定20秒超时
            .onErrorResumeNext {
                log.error("[Failed server assignment]", it)
                when (it) {
                    is TimeoutException -> Observable.error(TimeoutException("服务分配超时失败,限定连接总时:${CONNECT_TOTAL_TIMEOUT}秒"))
                    else -> Observable.error(it)
                }
            }
        }


    }

    /**
     * 尝试链接服务器
     */
    private fun tryConnect(serverJson: JsonObject,params: JsonObject,headers: MultiMap,mobile: String,uuid: String): Observable<Pair<String, JsonObject>> {
        val url = serverJson.value<String>("url") ?: return Observable.error(NullPointerException("Url is null"))
        //开始链接服务器
        return this.connect(url, params, headers)
                .flatMap(this::filter) // 过滤结果
                .flatMap { pair ->
                    //将成功的服务器存入历史记录
                    serverJson.put("status", true).put("last_time", System.currentTimeMillis()).put("mid",pair.first)
                    logUtils.trace(mobile,"[服务器分配成功]",serverJson)
                    this.putCache(uuid, serverJson).map { pair }
                }.onErrorResumeNext{ error ->
                    //将失败的服务器存入历史记录
                    serverJson.put("status", false).put("last_time", System.currentTimeMillis())
                    logUtils.failed(mobile,"[服务器分配失败]",error,serverJson)
                    this.putCache(uuid, serverJson).flatMap {
                        Observable.error<Pair<String,JsonObject>>(error)
                    }
                }
    }

    /**
     * 过滤返回的结果
     * */
    private fun filter(response: HttpResponse<Buffer>): Observable<Pair<String, JsonObject>> {
        log.info("[Filter result] response: ${response.bodyAsString()}")
        return try {
            val body = response.bodyAsJsonObject()
            // 限定返回必须带有MID
            val mid = body.value<String>("mid") ?: throw NullPointerException("Mid is null")
            log.info("[Filter success] Mid[$mid] Body:$body")
            Observable.just(Pair(mid, body))
        } catch (e: Exception) {
            log.error("[Filter failed] ${e.message}")
            Observable.error<Pair<String, JsonObject>>(e)
        }
    }

    /**
     * 尝试连接服务器，获取返回结果
     * */
    private fun connect(url: String, params: JsonObject, headers: MultiMap): Observable<HttpResponse<Buffer>> {
        log.info("Connect URL:$url / Timeout:${CONNECT_TIMEOUT}s / Body:$params / Headers:${headers.toList()}")
        // todo py 要求 将host、port加密传入headers中
        val host = headers.get("host")
        val port = headers.get("port")
        headers.add("token", Base64Utils.encodeToString("$host:$port".toByteArray()))
        return this.client
                .postAbs(url)
                .putHeaders(headers)
                .rxSendJsonObject(params)
                .doOnError { err -> log.error("Connect error URL[$url]", err) }
                .toObservable()
                .timeout(CONNECT_TIMEOUT, TimeUnit.SECONDS) // 限制访问超时时间默认 5秒
                .doOnError { err -> log.error("Timeout error URL[$url]", err) }
                .retryWhen(RetryWithTimeOut(CONNECT_RETRY)) // 重时次数，默认一次
    }

    /**
     * 返回单个可用服务器地址
     * */
    private fun server(uuid: String, mobile: String,listHeartBeat: MutableList<JsonObject>): Observable<JsonObject> {
        //通过当前用户isp绑定查询可用服务
        return this.listHistory(uuid).flatMap { listHistory ->
            logUtils.trace(mobile,"[服务器分配] 已有缓存列表 ${listHistory.toList()}")
            this.choose(listHeartBeat, listHistory).doOnError {
                logUtils.failed(mobile,"[服务器分配结果失败]",it)
            }
        }
    }

    /**
     * 获取py_server 到sharedData中
     */
    private fun listHistory(uuid: String): Observable<MutableMap<String, JsonObject>> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(PY_SERVER_KEY)
                .flatMap { it -> it.rxGet(uuid).map { it ?: mutableMapOf() } }
                .toObservable()
                .doOnError { log.error(it) }
    }

    /**
     * 存入py_server 到sharedData中
     */
    private fun putCache(uuid: String, server: JsonObject): Observable<Void> {
        val url = server.value<String>("url")?: return Observable.error(NullPointerException("Url is null"))
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(PY_SERVER_KEY).flatMap { am ->
            am.rxGet(uuid).flatMap { item ->
                val map = if (item.isNullOrEmpty()) {
                    mutableMapOf()
                } else {
                    item
                }
                map[url] = server
                am.rxPut(uuid, map)
            }
        }.toObservable()
    }

    /**
     * 清空sharedData 中的 py_server记录
     */
    fun clearPyServerByUuid(uuid: String): Single<MutableMap<String, JsonObject>> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(PY_SERVER_KEY)
                .flatMap { am ->
                    am.rxRemove(uuid)
                }
    }




    /**
     * 选择服务器
     */
    private fun choose(records: List<JsonObject>, historys: MutableMap<String, JsonObject>): Observable<JsonObject> {
        if(records.isNullOrEmpty()){
            return Observable.error(NoSuchServerException("无可用服务器记录"))
        }
        if(historys.isNullOrEmpty()){
            return Observable.just(records.first())
        }
        //可用历史记录
        var allowHistory = historys.filter {
            it.value.value("status",false)
        }.toList()
        //返回最后一个可用历史记录
        if(allowHistory.isNotEmpty()){
            return Observable.just(allowHistory.last().second)
        }
        //筛选出不在历史记录中的可用服务器
        val allowRecord = records.filter {
            var url = it.value<String>("url")
            when(url){
                null -> false
                else -> historys[url]?.value("status", false) ?: true
            }
        }
        if(allowRecord.isNullOrEmpty()){
            return Observable.error(NoSuchServerException("无可用服务器过滤结果"))
        }
        return Observable.just(allowRecord.first())
    }
}
