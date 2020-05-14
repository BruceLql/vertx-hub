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
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Controller
class CrawlerServer @Autowired constructor(
        private val client: WebClient,
        private val sd: SharedData,
        private val eb: EventBus,
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
        return Observable.defer {
            this.server(params)
                    .flatMap { url -> this.connect(url, params, headers) }  // 连接服务器
                    .flatMap(this::filter) // 过滤结果
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
    private fun server(params: JsonObject): Observable<String>{
        log.info("[开始分配服务器] >>>>>>>>")
        //通过当前用户isp绑定查询可用服务
        val uuid = params.value<String>("uuid") ?: return Observable.error(NullPointerException("uuid is null"))
        val isp = params.value<String>("isp") ?: return Observable.error(NullPointerException("isp is null"))
        val mobile = params.value<String>("mobile") ?: return Observable.error(NullPointerException("mobile is null"))
        //计算tags值
        val calcTags = Server.calcTags(JsonArray().add(isp))

        return Observable.zip(heartBeatService.listHeartBeatByStatusOrIsp(calcTags),this.listHistoryBySd(uuid)){ listHeartBeat, listHistoryBySd ->
            var host = ""
            this.choose(isp,mobile,uuid,listHeartBeat,listHistoryBySd)
                    .doOnError { it.printStackTrace() }
                    .subscribe { host = it.first }
            "http://$host/operator"
        }
    }

    /**
     * 获取py_server 到sharedData中
     */
    private fun listHistoryBySd(uuid: String): Observable<MutableMap<String, JsonObject>> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(PY_SERVER_KEY)
                .flatMap { it -> it.rxGet(uuid).map { it?: mutableMapOf() } }
                .toObservable()
                .doOnError { it.printStackTrace() }
    }

    /**
     * 存入py_server 到sharedData中
     */
    private fun putToSd(uuid: String, server: JsonObject): Observable<Void> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(PY_SERVER_KEY).flatMap { am ->
            am.rxGet(uuid).flatMap { item ->
                when(item){
                    null -> {
                        val itemMap =  mutableMapOf<String,JsonObject>()
                        itemMap[uuid] = server
                        am.rxPut(uuid, itemMap)
                    }
                    else -> {
                        server.map{ se ->
                            item[uuid]!!.put(se.key,se.value)
                        }
                        am.rxPut(uuid, item)
                    }
                }
            }
        }.toObservable()
    }

    /**
     * 清空sharedData 中的 py_server记录
     */
    fun clearPyServer(uuid: String){
        sd.rxGetAsyncMap<String,MutableMap<String, JsonObject>>(PY_SERVER_KEY)
                .flatMap { am -> am.rxRemove(uuid)}
    }

    /**
     * 选择服务器
     */
    private fun choose(isp: String, mobile: String, uuid: String, dbList: List<JsonObject>, sdMap: MutableMap<String, JsonObject>): Observable<Pair<String, JsonObject>> {
        if (!dbList.isNullOrEmpty()) {
            //若sharedData中没有则直接取出db中的第一个
            var server = if (sdMap.isNullOrEmpty()) { dbList.first() }
            else {
                dbList.first {
                    val host = it.value<String>("host") ?: false
                    val ip = "$host:" + it.value<Number>("port")
                    !sdMap[uuid]!!.containsKey(ip)
                }
            }
            val ip = server.value<String>("host") + ":" + server.value<Number>("port")
            var json = JsonObject()
                    .put("mobile", mobile)
                    .put("isp", isp)
                    .put("lastTime", System.currentTimeMillis())
            //更新到sharedData中
            this.putToSd(uuid, JsonObject().put(ip,json))
            return Observable.just(Pair(ip, server))
        } else {
            // todo 无服务器可选, mysql ， sharedData
            return Observable.error(NoSuchServerException("未找到服务器"))
        }
    }
}
