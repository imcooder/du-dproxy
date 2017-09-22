/**
 * @file 文件介绍
 * @author imcooder@gmail.com
 */
/* eslint-disable fecs-camelcase */
/* jshint esversion: 6 */
/* jshint node:true */
"use strict";
var _ = require('underscore');
var fs = require('fs');
var assert = require('assert');
var commonUtil = require('du-node-utils');
var RAL = require('node-ral').RAL;
var ralP = require('node-ral').RALPromise;

var Service = module.exports = {
    opt: {},
    init: function (opt) {
        console.log('[redis]init:%j', opt);
        var self = this;
        self.opt = {
            prefix: '',
        };
        if (opt) {
            self.opt = _.extend(self.opt, opt);
        }
        self.initDB();
    },
    initRal: function(opt) {
        console.log('initRal:%j', opt);
        RAL.init(opt);
    },
    initDB: function () {
        console.log('[redis]initDB');
        var self = this;
        return self;
    },
    closeDB: function () {
        console.log('[redis]closeDB');
    },
    makeKey: function (value) {
        return this.opt.prefix + commonUtil.toString(value);
    },
    ralRequest: function(serviceName, opt) {
        let start = commonUtil.now();
        if (_.has(opt, 'headers') && !(_.isObject(opt.headers) && !_.isArray(opt.headers))) {
            delete opt.headers;
        }
        if (_.has(opt, 'query') && !(_.isObject(opt.query) && !_.isArray(opt.query))) {
            delete opt.query;
        }
        return ralP(serviceName, opt).then(function(data) {
            console.log('ral response:', data);
            if (data && _.isString(data)) {
                try {
                    data = JSON.parse(data);
                } catch (error) {
                    console.error('parse json failed:str[%s] error:%s', error.stack);
                    return Promise.reject(new Error('bad json'));
                }
            }
            console.log('[rpc]using:%d', commonUtil.now() - start);
            return data;
        }).catch(function(error) {
            console.error('[ral]call failed:error:%s', error.stack);
            if (error.code === 'ETIMEDOUT' || error.code === 'ESOCKETTIMEDOUT') {
                console.error('[rpc] timeout opt:%j', opt);
                return Promise.reject(new Error('timeout'));
            }
            console.log('[rpc]using:%d', commonUtil.now() - start);
            return Promise.reject(error);
        });
    },
    mget: function (keys, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]mget:%s', logid, keys);
        var self = this;
        if (!_.isArray(keys)) {
            keys = [keys];
        }
        var args = [];
        _.each(keys, item => {
            args.push(self.makeKey(item));
        });
        if (!args) {
            console.error('logid:%s invalid arguments', logid);
            return Promise.reject(new Error('invalid arguments'));
        }
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'MGET',
                'args': args
            }
        }).then(response => {
            //console.log('logid:%s [redis]mget response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.error('logid:%s db error', logid);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s bad status', logid);
                return Promise.reject(new Error('bad status'));
            }
            var ret = {};
            if (_.isArray(response.res)) {
                if (response.res.length == keys.length) {
                    for (var idx = 0; idx < keys.length; idx++) {
                        ret[keys[idx]] = response.res[idx];
                    }
                } else {
                    console.error('logid:%s response error', logid);
                }
            }
            return ret;
        });
    },
    setex: function (key, value, time, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]setex key:%s value:%j time:%s', logid, key, value, time);
        let self = this;
        var args = [];
        args.push(self.makeKey(key));
        args.push(commonUtil.toString(time));
        args.push(commonUtil.toString(value));
        if (!args) {
            console.error('logid:%s invalid arguments', logid);
            return Promise.reject(new Error('invalid arguments'));
        }
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'SETEX',
                'args': args
            }
        }).then(response => {
            console.log('logid:%s [redis]setex response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.error('logid:%s db error', logid);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s bad status', logid);
                return Promise.reject(new Error('bad status'));
            }
            if (response.res !== 'OK') {
                return Promise.reject(new Error(response.res || 'faild'));
            }
            return;
        });
    },
    mset: function (data, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        //console.log('logid:%s [redis]mset:', logid, data);
        let self = this;
        var args = [];
        _.each(data, item => {
            if (_.has(item, 'key') && _.has(item, 'value')) {
                args.push(self.makeKey(item.key));
                args.push(commonUtil.toString(item.value));
            } else {
                console.error('logid:%s bad format', logid);
            }
        });
        if (!args) {
            console.error('logid:%s invalid arguments', logid);
            return Promise.reject(new Error('invalid arguments'));
        }
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'MSET',
                'args': args
            }
        }).then(response => {
            console.log('logid:%s [redis]mset response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.error('logid:%s db error', logid);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s bad status', logid);
                return Promise.reject(new Error('bad status'));
            }
            return;
        });
    },
    del: function (keys, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]del:%j', logid, keys);
        var self = Service;
        if (!_.isArray(keys)) {
            keys = [keys];
        }
        var args = [];
        _.each(keys, item => {
            args.push(self.makeKey(item));
        });
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'DEL',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s del response:', logid, response);
            if (!_.has(response, 'status')) {
                console.eror('del failed:%j', response);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s del failed:%j', logid, response);
                return Promise.reject(new Error('bad status'));
            }
            return;
        });
    },
    hmset: function (key, values, opt) {
        var self = Service;
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        var args = [];
        args.push(self.makeKey(key));
        _.each(values, (itemValue, itemKey) => {
            args.push(commonUtil.toString(itemKey));
            args.push(commonUtil.toString(itemValue));
        });
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'HMSET',
                'args': args,
            }
        }).then(response => {
            if (!_.has(response, 'status')) {
                console.error('logid:%s hmset failed:%j', logid, response);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s hmset failed:%j', logid, response);
                return Promise.reject(new Error('bad status'));
            }
            return;
        });
    },
    hgetall: function (key, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]hgetall:%j', logid, key);
        var self = Service;
        var args = [];
        args.push(this.makeKey(key));
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'HGETALL',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s hgetall response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.error('logid:%s hgetall failed:%j', logid, response);
                return Promise.reject(new Error('db error'));
            }
            if (response.status !== 0) {
                console.error('logid:%s hgetall failed:%j', logid, response);
                return Promise.reject(new Error('bad status'));
            }
            var ret = {};
            if (response.res && _.isArray(response.res)) {
                for (var i = 0; i < response.res.length; i += 2) {
                    ret[response.res[i]] = response.res[i + 1];
                }
            }
            return ret;
        });
    },
    hdel: function (key, fields, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]hdel:%j', logid, key);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        if (!_.isArray(fields)) {
            fields = [fields];
        }
        _.each(fields, item => {
            args.push(commonUtil.toString(item));
        });
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'HDEL',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s hdel response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.eror('logid:%s hdel failed:%j', logid, response);
                return;
            }
            if (response.status !== 0) {
                console.error('logid:%s hdel failed:%j', logid, response);
                return;
            }
            return;
        });
    },
    lrange: function (key, start, stop, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]lrange:%s %s-%s', logid, key, start, stop);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        args.push(commonUtil.toString(start));
        args.push(commonUtil.toString(stop));
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'LRANGE',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s lrange response:%j', logid, response);
            if (!_.has(response, 'status') || response.status !== 0) {
                console.error('logid:%s lrange failed:%j', logid, response);
                return Promise.reject(new Error(response.msg || 'bad response'));
            }
            let res = [];
            if (_.has(response, 'res')) {
                res = response.res;
            }
            if (!_.isArray(res)) {
                res = [];
            }
            return res;
        }).catch(error => {
            console.error('logid:%s lrange failed:%s', logid, error.stack);
            return Promise.reject(error);
        });
    },
    lpush: function (key, values, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]lpush:%s %j', logid, key, values);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        if (!_.isArray(values)) {
            values = [values];
        }
        for (let value of values) {
            args.push(commonUtil.toString(value));
        }
        if (_.isEmpty(args)) {
            return Promise.reject(new Error('invalid args'));
        }
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'LPUSH',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s lpush response:%j', logid, response);
            if (!_.has(response, 'status')) {
                return Promise.reject(new Error(response.msg || 'need status'));
            }
            if (response.status !== 0) {
                return Promise.reject(new Error(response.msg || 'bad status'));
            }
            if (!_.has(response, 'res') || !_.isNumber(response.res)) {
                return Promise.reject(new Error('bad res'));
            }
            return response.res;
        }).catch(error => {
            console.error('logid:%s lpush error:%s', logid, error.stack);
            return Promise.reject(error);
        });
    },
    rpush: function (key, values, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]rpush:%s %j', logid, key, values);
        console.log(values);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        if (!_.isArray(values)) {
            values = [values];
        }
        for (let value of values) {
            args.push(commonUtil.toString(value));
        }
        if (_.isEmpty(args)) {
            return Promise.reject(new Error('invalid args'));
        }
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'RPUSH',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s rpush response:%j', logid, response);
            if (!_.has(response, 'status')) {
                return Promise.reject(new Error(response.msg || 'need status'));
            }
            if (response.status !== 0) {
                return Promise.reject(new Error(response.msg || 'bad status'));
            }
            if (!_.has(response, 'res') || !_.isNumber(response.res)) {
                return Promise.reject(new Error('bad res'));
            }
            return response.res;
        }).catch(error => {
            console.error('logid:%s rpush error:%s', logid, error.stack);
            return Promise.reject(error);
        });
    },
    lrem: function (key, count, value, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]lrem:%s %s-%s', logid, key, count, value);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        args.push(commonUtil.toString(count));
        args.push(commonUtil.toString(value));
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'LREM',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s lrem response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.log('logid:%s lrem failed:%j', logid, response);
                return;
            }
            if (response.status !== 0) {
                console.log('logid:%s lrem failed:%j', logid, response);
                return;
            }
            return response.res;
        }).catch(error => {
            console.log('logid:%s lrem failed:%s', logid, error.stack);
            return Promise.reject(error);
        });
    },
    ltrim: function (key, start, stop, opt) {
        if (!opt) {
            opt = {};
        }
        let logid = opt.logid || 'null';
        console.log('logid:%s [redis]ltrim:%s %s-%s', logid, key, start, stop);
        var self = Service;
        var args = [];
        args.push(self.makeKey(key));
        args.push(commonUtil.toString(start));
        args.push(commonUtil.toString(stop));
        return self.ralRequest('redis_cmd', {
            data: {
                'cmd': 'LTRIM',
                'args': args,
            }
        }).then(response => {
            console.log('logid:%s ltrim response:%j', logid, response);
            if (!_.has(response, 'status')) {
                console.error('logid:%s ltrim failed:%j', logid, response);
                return;
            }
            if (response.status !== 0) {
                console.error('logid:%s ltrim failed:%j', logid, response);
                return;
            }
            return;
        }).catch(error => {
            console.error('logid:%s ltrim error:%s', logid, error.stack);
            return Promise.reject(error);
        });
    },
};
