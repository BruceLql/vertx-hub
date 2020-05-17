package fof.daq.hub.service

import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.model.Server
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.shareddata.SharedData
import org.springframework.stereotype.Service
import rx.Observable

/**
 * 缓存服务类
 */
@Service
class CacheService (
        private val sd: SharedData
){

    private val log = logger(this::class)

    /**
     * 记录每次用户请求的参数
     */
    private val H5_PARAMS_KEY = "_H5_PARAMS_KEY_"


    /**
     * 获取py_server 到sharedData中
     */
    fun listServerHistory(uuid: String): Observable<MutableMap<String, JsonObject>> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(Server.PY_SERVER_KEY)
                .flatMap { it -> it.rxGet(uuid).map { it ?: mutableMapOf() } }
                .toObservable()
                .doOnError { log.error(it) }
    }

    /**
     * 存入py_server 到sharedData中
     */
    fun putServer(uuid: String, server: JsonObject): Observable<Void> {
        val url = server.value<String>("url")?: return Observable.error(NullPointerException("Url is null"))
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(Server.PY_SERVER_KEY).flatMap { am ->
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
    fun clearServer(uuid: String): Observable<MutableMap<String, JsonObject>> {
        return sd.rxGetAsyncMap<String, MutableMap<String, JsonObject>>(Server.PY_SERVER_KEY)
                .flatMap { am ->
                    am.rxRemove(uuid)
                }.toObservable()
    }

    /**
     * 存储用户每次请求进来携带的参数
     */
    fun putH5(uuid: String,data: JsonObject): Observable<Void> {
        return sd.rxGetAsyncMap<String,JsonObject>(H5_PARAMS_KEY)
                .flatMap { am ->
                    am.rxPut(uuid,data)
                }.toObservable()
    }

    /**
     * 存储用户每次请求进来携带的参数
     */
    fun putH5ReqParams(uuid: String,data: JsonObject): Observable<Void> {
        return sd.rxGetLocalLockWithTimeout(uuid,1000).toObservable().flatMap { lock ->
            this.putH5(uuid,data)
                    .doAfterTerminate { lock.release() }
        }
    }


    /**
     * 存储用户每次请求进来携带的参数
     */
    fun clearH5ReqParams(uuid: String): Observable<JsonObject> {
        return sd.rxGetAsyncMap<String,JsonObject>(H5_PARAMS_KEY).flatMap { am ->
            am.rxRemove(uuid)
        }.toObservable()
    }

    /**
     * 获取用户每次请求携带参数的记录
     */
    fun getReqParams(uuid: String): Observable<JsonObject>{
        return sd.rxGetAsyncMap<String,JsonObject>(H5_PARAMS_KEY).flatMap { am ->
            am.rxGet(uuid)
        }.toObservable()
    }
}
