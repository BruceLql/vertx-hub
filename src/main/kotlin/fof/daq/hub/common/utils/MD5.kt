package fof.daq.hub.common.utils

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException

object MD5{
    fun digest(value: String): String? {
        try {
            //初始化MessageDigest信息摘要对象,并指定为MD5不分大小写都可以
            val md = MessageDigest.getInstance("MD5")
                md.update(value.toByteArray())
            //计算信息摘要digest()方法,返回值为字节数组
            val b = md.digest()
            var i: Int
            val buf = StringBuffer("")
            for (offset in b.indices) {
                //将首个元素赋值给i
                i = b[offset].toInt()
                if (i < 0) { i += 256 }
                if (i < 16) { buf.append("0") }
                //转换成16进制编码
                buf.append(Integer.toHexString(i))
            }
            return buf.toString().substring(8, 24)
        } catch (e: NoSuchAlgorithmException) {
            return null
        }
    }

    /**
     * 返回整段
     */
    fun encryption(value: String): String? {
        try {
            //初始化MessageDigest信息摘要对象,并指定为MD5不分大小写都可以
            val md = MessageDigest.getInstance("MD5")
            md.update(value.toByteArray())
            //计算信息摘要digest()方法,返回值为字节数组
            val b = md.digest()
            var i: Int
            val buf = StringBuffer("")
            for (offset in b.indices) {
                //将首个元素赋值给i
                i = b[offset].toInt()
                if (i < 0) { i += 256 }
                if (i < 16) { buf.append("0") }
                //转换成16进制编码
                buf.append(Integer.toHexString(i))
            }
            return buf.toString()
        } catch (e: NoSuchAlgorithmException) {
            return null
        }
    }
}
