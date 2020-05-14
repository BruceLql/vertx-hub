package fof.daq.hub.service

import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import org.springframework.stereotype.Service
import rx.Single

/**
 * 数据清洗服务
 */
@Service
class CollectNoticeService(
        private val client: AsyncSQLClient
) {


    /**
     * 保存到数据库
     */
    fun save(uuid:String,userId: String, name: String, cid: String, mobile: String, callBack: String, notifyUrl: String, nonce: String,oparateType:String): Single<Void> {
        val sql = "insert into h5_request_param (uuid,user_id,name,cid,mobile,call_back,notify_url,nonce,oparate_type) " +
                "values('$uuid','$userId','$name','$cid','$mobile','$callBack','$notifyUrl','$nonce','$oparateType')"
        return client.rxGetConnection().flatMap {conn ->
            conn.rxExecute(sql).doAfterTerminate { conn.delegate.close() }
        }.doOnError { it.printStackTrace() }
    }

    /**
     * 更新mid
     */
    fun update(uuid: String,mid:String): Single<UpdateResult> {
        val sql = "update h5_request_param set mid='$mid' where uuid='$uuid'"
        return client.rxGetConnection().flatMap {conn ->
            conn.rxUpdate(sql).doAfterTerminate { conn.delegate.close() }
        }.doOnError { it.printStackTrace() }
    }

    /**
     * 根据uuid查询单个记录
     */
    fun findOneByUuid(uuid:String): Single<List<JsonObject>> {
        val sql = "select * from h5_request_param where uuid='$uuid'"
        return client.rxGetConnection().flatMap {conn ->
            conn.rxQuery(sql).doAfterTerminate { conn.delegate.close() }.map { it.rows }
        }.doOnError { it.printStackTrace() }
    }
}
