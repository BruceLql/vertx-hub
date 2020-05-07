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
            this.eb.onerror = function (e) {
                this.onerror(e);
                reject(e);
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
Hub.prototype.onerror = function(e) {
    logger.error(e);
};
Hub.prototype.onmessage = function(err, result) {
    if (result && result.body && result.body["method"]) {
        if (result.body["method"] === 'stop' && result.body["session_id"]) {
            if (this.headers["sessionId"] === result.body.session_id) {
                logger.info("[服务在其它地方打开]关闭现有socket:" + result.body.session_id)
                this.eb.close();
            }
        }
    }
    if(!err && result && result.address) {
        this.emit(result.address, result);
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
    return new Promise(function(resolve, reject){
        this.connect()
            .then(function (eb) {
                try {
                    var address = "FOF.DAQ.HUB.CLIENT.LISTEN." + mid;
                    logger.info("-尝试注册监听地址："+ address);
                    eb.registerHandler(address, this.headers, this.onmessage.bind(this));
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
Hub.prototype.init = function(data) {
    return new Promise(function(resolve, reject){
        var message = Object.assign({"mobile": null, "isp": null, "password": null, "code": null}, data);
        if (message.mobile == null) {
            reject(new Error("手机号码不能为空"));
            return
        }
        if (message.isp == null) {
            reject(new Error("运营商不能为空"));
            return
        }
        logger.info("-初始化服务商：" + JSON.stringify(message));
        this.send('FOF.DAQ.HUB.SERVER.INIT', message)
            .then(function (result) {
                if (typeof result.body == "object" && result.body.mid != null) {
                    var body = result.body;
                    var mid = body.mid;
                    this.headers = body;
                    logger.info("-初始化服务商分配[MID]：" + mid);
                    // 注册监听该MID
                    this.register(mid)
                        .then(function (address) {
                            logger.info("-注册监听地址完成："+ address);
                            resolve(body);
                        }.bind(this))
                        .catch(function (err) { reject(err); });
                } else {
                    reject(new Error("初始化失败，缺少注册信息"));
                }
            }.bind(this)).catch(function (err) {
                reject(err);
            });
    }.bind(this));
};
/**
 * 初始化服务，后台选举出PY机器并返回MID
 * @param mobile 手机号码
 * @param isp 服务提供商
 * @param password 服务密码
 * @param code 短信码
 * */
Hub.prototype.start = function(mobile, isp, password, code) {
    return this.init({"mobile": mobile || null, "isp": isp, "password": password || null, "code": code || null})
};