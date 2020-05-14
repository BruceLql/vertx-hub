package fof.daq.hub.service

import fof.daq.hub.common.enums.ServerState
import fof.daq.hub.model.Server
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import org.springframework.stereotype.Repository
import rx.Observable


/**
 * 心跳服务处理类
 */
@Repository
class HeartBeatService(
        private val client: AsyncSQLClient
) {


    /**
     * 查询正常状态的服务列表
     */
    fun listHeartBeatByStatus(): Observable<MutableList<JsonObject>> {
        val sql = "select * from py_server p where p.status=${ServerState.UP.code}"
        return client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate { conn.delegate.close() }.map { it.rows }
        }.toObservable().doOnError { it.printStackTrace() }
    }

    /**
     * 更新服务的状态和时间
     */
    fun updateByStatusOrTime(host: String, port: Number,serverState: ServerState,timestamp: Long,tags:Int): Observable<UpdateResult> {
        val sql = "update py_server set status=${serverState.code},timestamp=$timestamp,tags=$tags where host='$host' and port=$port"
        return client.rxGetConnection().flatMap { conn->
            conn.rxUpdate(sql).doAfterTerminate { conn.delegate.close()}
        }.toObservable().doOnError { it.printStackTrace() }
    }

    /**
     * 根据isp查找正常状态的服务列表
     */
    fun listHeartBeatByStatusOrIsp(calcTags : Int): Observable<MutableList<JsonObject>> {
        var sql = "select * from py_server where $calcTags <= tags and status=${ServerState.UP.code}"
        return client.rxGetConnection().flatMap { conn ->
            conn.rxQuery(sql).doAfterTerminate { conn.delegate.close() }.map { it.rows }
        }.toObservable().doOnError { it.printStackTrace() }
    }

    /**
     * 根据host,port返回单个
     */
    fun findOneByHostOrPort(host:String,port: Number): Observable<List<JsonObject>> {
        val sql = "select * from py_server where host='$host' and port=$port"
        return client.rxGetConnection().flatMap {conn ->
            conn.rxQuery(sql).doAfterTerminate { conn.delegate.close() }.map { it.rows }
        }.toObservable().doOnError { it.printStackTrace() }
    }

    /**
     * 根据host,port更新状态
     */
    fun updateByStatus(host: String, port: Number, serverState: ServerState): Observable<UpdateResult> {
        val sql = "update py_server set status=${serverState.code} where host='$host' and port=$port"
        return client.rxGetConnection().flatMap { conn->
            conn.rxUpdate(sql).doAfterTerminate { conn.delegate.close() }
        }.toObservable().doOnError { it.printStackTrace() }
    }

    /**
     * 保存server 注册的信息
     */
    fun save(server: Server) {
        val sql = "INSERT INTO `py_server`(`host`, `port`, `server_name`, `timestamp`, `tags`, `version`, `status`, `switch`, `create_at`) " +
                "VALUES ('${server.host}', ${server.port}, '${server.server_name}', ${server.timestamp}, '${server.tags}', '${server.version}',${server.status}, ${server.switch}, ${server.created_at});"
        client.rxGetConnection().flatMap { conn ->
            conn.rxExecute(sql).doAfterTerminate { conn.delegate.close() }
        }.toObservable().doOnError { it.printStackTrace() }.subscribe()
    }
}
