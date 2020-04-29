var logger = {
    error: function(err) {
        console.error(err);
    },
    info: function (info) {
        console.log(info)
    }
};
function Hub(url, option) {
    this.eb = null;
    this.url = url;
    this.headers = {};
    this.reconnectEnabled = true;// 是否重连
    this.option = this.reconnectEnabled ? Object.assign({
        vertxbus_reconnect_attempts_max: 3 // 最大连接次数
    }, option) : option;
    this.close = null;  // 外部close监听函数
    this.open = null;   // 外部open监听函数
    this.message = null;// 外部message监听函数
    this.listens = [];
}
Hub.prototype.connect = function() {
    return new Promise(function(resolve, reject){
        if (this.eb === null) {
            this.eb = new EventBus(this.url, this.option);
            this.eb.reconnectEnabled = this.reconnectEnabled;
            this.eb.onclose = function (e) {
                this.onclose(e);
                reject(e);
            }.bind(this);
            this.eb.onopen = function () {
                this.onopen();
                resolve(this.eb);
            }.bind(this);
        } else {
            var tryReConnect = function () {
                this.connect()
                    .then(function (result) { resolve(result) })
                    .catch(function (err) { reject(err) });
            }.bind(this);
            switch (this.eb.state) {
                case EventBus.OPEN:
                     resolve(this.eb);
                     break;
                case EventBus.CLOSED:
                     this.eb = null;
                    tryReConnect();
                    break;
                default:
                    setTimeout(tryReConnect, 1000)
            }
        }
    }.bind(this));
};
Hub.prototype.onclose = function(e) {
    logger.error("连接断开");
    this.close && this.close(e);
};
Hub.prototype.onopen = function() {
    if (Object.keys(this.headers).length > 0) {
        logger.info("[尝试重注册地址]");
        this.init(this.headers); // 重新注册用户信息
    }
    this.open && this.open();
};
Hub.prototype.onmessage = function(err, result) {
    if(!err && result && result.address) {
        this.emit(result.address, result);
    }
    if (!err && result.reply) {
        result.reply({"success": 1}) // todo 增加header
    }
    this.message && this.message(err, result);
};
Hub.prototype.send = function(address, message, headers) {
    return new Promise(function(resolve, reject){
        this.connect().then(function (eb) {
            eb.send(address, message, headers, function(err, res) {
                if (err) {
                    reject(err);
                    logger.error(err);
                } else {
                    resolve(res);
                }
            });
        }).catch(function (err) {
            reject(err);
            logger.error(err);
        }) ;
    }.bind(this));
};
Hub.prototype.register = function(mid) {
    var address = "FOF.DAQ.HUB.CLIENT.MID." + mid;
    logger.info("-开始注册监听地址："+ address);
    return new Promise(function(resolve, reject){
        this.connect()
            .then(function (eb) {
                try {
                    eb.registerHandler(address, this.headers, this.onmessage.bind(this));
                    logger.info("-成功注册监听地址："+ address);
                    resolve(address);
                } catch (e) {
                    reject(e);
                }
            }.bind(this))
            .catch(function (err) { reject(err) })
    }.bind(this));
};
Hub.prototype.once = function(key, func, timeOutCallBack, timeOut) {
    var uid = Math.random();
    var value = {key: key, func: func, uid: uid};
    if (timeOutCallBack) {
        value["timeout"] = setTimeout(function () {
            var index = this.listens.findIndex(function (it) { return it.uid === uid });
            if (index > -1) {
                this.listens.splice(index, 1);
            }
            timeOutCallBack(new Error("Time out"));
        }.bind(this), timeOut || 3000)  // 限制3秒超时监听
    }
    this.listens.push(value);
};
Hub.prototype.emit = function(address, result) {
    var index = this.listens.findIndex(function (it) { return it.key === address });
    if (index > -1 && this.listens[index]) {
        this.listens[index].func(result);
        var timeout = this.listens[index]["timeout"];
        if (timeout) clearTimeout(timeout);
        this.listens.splice(index, 1);
        this.emit(address, result);
    }
};
/**
 * 初始化服务，后台选举出PY机器并返回MID
 * @param mobile_object 手机号码
 * @param password 服务密码
 * @param isp 服务提供商
 * @param code 短信码
 * */
Hub.prototype.init = function(mobile_object, password, isp, code) {
    return new Promise(function(resolve, reject){
        var message = {};
        if (typeof mobile_object === "object") {
            message = mobile_object;
        } else {
            message = {"mobile": mobile_object || null, "password": password || null, "isp": "CTCC", "code": code || null};
        }
        logger.info("-初始化服务商：" + JSON.stringify(message));
        this.send('FOF.DAQ.HUB.SERVER.INIT', message, {"sb":1111, "asdf": [2,3]})
            .then(function (result) {
                try {
                    var mid = result.body.mid;
                    this.headers = Object.assign(message, { mid: mid }); // 初始化成功后保存所选择的信息
                    logger.info("-初始化服务商分配MID：" + mid);
                    // 注册监听该MID
                    this.register(mid)
                        .then(function (address) {
                            if(result.reply) {
                                // 验证地址是否注册成功
                                this.once(address, function(){
                                    logger.info("-验证成功监听地址：" + address);
                                    resolve(mid);
                                }, function (err) {
                                    logger.info("-验证失败超时监听地址：" + address);
                                    reject(err);
                                });
                                result.reply({"code": 1, "address": address});
                            } else {
                                resolve(mid);
                            }
                        }.bind(this))
                        .catch(function (err) { reject(err); });
                } catch (e) {
                    reject(e);
                }
            }.bind(this)).catch(function (err) {
                reject(err);
            });
    }.bind(this));
};
Hub.prototype.login = function(message) {
    return new Promise(function(resolve, reject){
        this.connect()
            .then(function (eb) {
                console.log(2222222)
                eb.registerHandler('FOF.DAQ.HUB.SERVER.LOGIN', function (err, body) {
                    console.log(err)
                    console.log(body);
                });
            });
/*        */
    }.bind(this));
};
Hub.prototype.verification = function () {

};