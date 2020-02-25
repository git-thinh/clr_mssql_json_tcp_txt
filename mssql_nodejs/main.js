const ___HTTP_PORT = 8080;
const ___LOG_PORT = 1515;
const ___LOG_ERROR_PORT = 1510;

const ___SCOPE = 'DB_QUEUE';
const ___TOKEN_API = 'eb976d531188435ea006fce8769c53d5';
const ___DB_CONFIG = {
    server: '192.168.10.54',
    authentication: {
        type: 'default',
        options: {
            userName: 'sa',
            password: 'dev@123'
        }
    },
    options: {
        database: 'Test'
    }
};

//#region [ VARIABLES ]

const _JOB = require('cron').CronJob;

let ___CONNECTED = false;
let ___ADDRESS_PORT;


const _PATH = require('path');
const _FS = require('fs');
const _FETCH = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

//#endregion

//#region [ LOG UDP ]

const ___log = (...agrs) => { LOG_MSG_BUFFER.push(___SCOPE + ' ' + new Date().toLocaleString() + ': \t' + JSON.stringify(agrs)); }
const ___log_err_throw = (func_name, err_throw, para1, para2, para3) => {
    const s = ___SCOPE + '_ERR_THROW ' + new Date().toLocaleString() + ' [ ' + func_name + ' ]';
    LOG_ERROR_MSG.push([___SCOPE, s, err_throw, para1, para2, para3]);
}

const LOG_ERROR_MSG = [];
let LOG_ERROR_MSG_WRITING = false;
new _JOB('* * * * * *', function () {
    try {
        if (LOG_ERROR_MSG_WRITING) return;
        if (LOG_ERROR_MSG.length > 0) {
            LOG_ERROR_MSG_WRITING = true;
            const text = LOG_ERROR_MSG.shift();
            var buf = Buffer.from(text);
            const udp = _DGRAM.createSocket('udp4');
            udp.send(buf, 0, buf.length, ___LOG_ERROR_PORT, '127.0.0.1', (err) => {
                LOG_ERROR_MSG_WRITING = false;
                udp.close();
            });
        }
    } catch (e1) { }
}).start();

const LOG_MSG_BUFFER = [];
let LOG_MSG_WRITING = false;
new _JOB('* * * * * *', function () {
    try {
        if (LOG_MSG_WRITING) return;
        if (LOG_MSG_BUFFER.length > 0) {
            LOG_MSG_WRITING = true;
            const text = LOG_MSG_BUFFER.shift();
            var buf = Buffer.from(text);
            const udp = _DGRAM.createSocket('udp4');
            udp.send(buf, 0, buf.length, ___LOG_PORT, '127.0.0.1', (err) => {
                LOG_MSG_WRITING = false;
                udp.close();
            });
        }
    } catch (e1) { }
}).start();

//#endregion

___log('DB_QUEUE', 'Start ... ', new Date().toLocaleString());

//#region [ API LOAD FILE ]

const ___API = {};

const file___loadScript = () => {
    _FS.readdir('./api/', (err, arr_apis_) => {
        //console.log(arr_apis_);
        arr_apis_.forEach(api_ => {
            _FS.readdir('./api/' + api_, (err, files_) => {
                //console.log(api_, files_);
                files_.forEach(fi_ => {
                    const file = './api/' + api_.toLowerCase() + '/' + fi_.toLowerCase();
                    console.log(file);
                    if (file.endsWith('.sql')) {
                        _FS.readFile(file, 'utf-8', (err, sql_script) => {
                            if (err) {
                                //console.log(err);
                            } else {
                                var text_sql = sql_script.trim();
                                if (___API[api_.toUpperCase()] == null) ___API[api_.toUpperCase()] = {};
                                const biz = fi_.substr(0, fi_.length - 4).toUpperCase();
                                ___API[api_.toUpperCase()][biz] = text_sql;
                            }
                        });
                    }
                });
            });
        });
    });
};

file___loadScript();

//#endregion

//#region [ HTTP ]

const _HTTP_EXPRESS = require('express');
const _HTTP_BODY_PARSER = require('body-parser');
const _HTTP_APP = _HTTP_EXPRESS();
const _HTTP_SERVER = require('http').createServer(_HTTP_APP);

_HTTP_APP.use(_HTTP_BODY_PARSER.json());
_HTTP_APP.use((error, req, res, next) => {
    if (___CONNECTED == false) {
        return res.json({ ok: false, mesage: 'Db disconnect ...' });
    }
    if (error !== null) {
        return res.json({ ok: false, mesage: 'Invalid json ' + error.toString() });
    }
    return next();
});
_HTTP_APP.use(_HTTP_EXPRESS.static(_PATH.join(__dirname, 'htdocs')));

_HTTP_APP.get('/apis', async (req, res) => { res.json(___API); });

_HTTP_APP.post('/' + ___SCOPE.toLowerCase() + '/:api_name/:action/:token', async (req, res) => {
    const path = '/' + ___SCOPE.toLowerCase() + '/:api_name/:action/:token';

    const data = req.body;
    const api_name = req.params.api_name.toUpperCase();
    const action = req.params.action.toUpperCase();
    const token = req.params.token.toLowerCase();

    if (data == null) {
        res.json({ ok: false, message: 'Data is null' });
        return;
    }

    if (api_name == null || api_name.length == 0) {
        res.json({ ok: false, message: path + ' invalid' });
        return;
    }

    if (action == null || action.length == 0) {
        res.json({ ok: false, message: path + ' invalid' });
        return;
    }

    f___MSG_PUSH(res, { api: api_name, action: action }, data);
});

//_HTTP_APP.post('/biz/:connect_string/:api_name/:store_action/:token', async (req, res) => {
//    const data = req.body;
//    try {
//        const connect_string = req.params.connect_string.toLowerCase();
//        const api_name = req.params.api_name.toUpperCase();
//        const store_action = req.params.store_action;
//        const token = req.params.token.toLowerCase();

//        if (connect_string.length == 0
//            || api_name.length == 0
//            || store_action.length == 0
//            || token.length == 0) {
//            res.json({ ok: false, message: 'Uri of APIs must be: api/biz/:connect_string/:api_name/:store_action/:token ' });
//            return;
//        }

//        if (___TOKEN_API != token) {
//            res.json({ ok: false, message: 'TOKEN invalid: ' + token });
//            return;
//        }

//        if (connect_string != 'amz' && connect_string != '123') {
//            res.json({ ok: false, message: 'CONNECT_STRING invalid, they are (amz|123)' });
//            return;
//        }

//    } catch (err_throw) {
//        //___log_err_throw('/biz/:connect_string/:api_name/:store_action/:token', err_throw, req.path, data);
//        res.json({ ok: false, message: 'ERR_THROW: ' + err_throw });
//    }
//});

_HTTP_SERVER.listen(___HTTP_PORT, '127.0.0.1', () => {
    ___ADDRESS_PORT = _HTTP_SERVER.address();
    console.log('HTTP_API: ', ___ADDRESS_PORT);
});

//#endregion

//#region [ DB ]

const _DB_CONNECTION = require('tedious').Connection;
const _DB_REQUEST = require('tedious').Request;
const _DB_TYPES = require('tedious').TYPES;
const _ASYNC = require('async');

const _DB_CONN = new _DB_CONNECTION(___DB_CONFIG);

_DB_CONN.on('connect', function (err) {
    if (err) {
        console.log(err);
    } else {
        console.log('DB Connected ... ');
        ___CONNECTED = true;
    }
});

const db___execute_callback = (m) => {
    const client = m.client;
    const error = m.error;
    const result = m.result;

    const setting = m.setting;
    const data = m.data;

    const api = setting.api;
    const action = setting.action;

    const store = api + '/' + action;

    if (error) {
        console.log('ERROR: ' + store, data, error);

        if (client) {
            delete m['client'];
            m.ok = false;
            client.json(m);
        }

        return;
    }

    console.log('OK: ' + store, result);

    if (client) {
        delete m['client'];
        m.ok = true;
        client.json(m);
    }
};

const db___execute = (m) => {
    if (m) {
        const client = m.client;
        const setting = m.setting;
        const data = m.data;
        if (client && setting && data) {
            const api = setting.api;
            const action = setting.action;
            if (api && action) {
                if (___API[api] == null || ___API[api][action] == null) {
                    client.json({ ok: false, message: 'Cannot find file API: api/' + api + '/' + action });
                    return;
                }

                const sql_text = ___API[api][action];

                //const sql_text = " \
                //CREATE TABLE #___CHANGED(ID BIGINT, CACHE VARCHAR(255), DB_ACTION VARCHAR(255), SQL_CMD NVARCHAR(MAX), SORT INT DEFAULT(0)); \
                //DECLARE @OK BIT = 0; \
                //DECLARE @MESSAGE NVARCHAR(MAX) = ''; \
                ///*=====================================*/ \
                //declare @Name nvarchar(50) = '23123' \
                //declare @Location nvarchar(50) = '123213' \
                ///*=====================================*/ \
                ///*ROLLBACK: START*/ \
                //\
                //		INSERT INTO TESTSCHEMA.EMPLOYEES (Name, Location) \
                //		OUTPUT INSERTED.ID, 'POL_EMPLOYEES', 'DB_INSERT', 'SELECT * FROM TESTSCHEMA.EMPLOYEES WHERE ID = ' + CAST(INSERTED.ID AS VARCHAR(36)) \
                //	INTO #___CHANGED(ID, CACHE, DB_ACTION, SQL_CMD) \
                //		VALUES (@Name, @Location); \
                //\
                //		/*--SELECT * FROM #___CHANGED;*/ \
                //		EXEC ___CHANGED_JSON @OK OUTPUT, @MESSAGE OUTPUT; \
                //		DROP TABLE #___CHANGED; \
                //		/*-- CHECK TO ROLLBACK, THEN @OK = 0 -> FAIL*/ \
                //		/*--IF @OK = 0  BEGIN  PRINT 'CALL ROLLBACK'; END*/ \
                //\
                ///*ROLLBACK: END*/ \
                ///*=====================================*/ \
                ///*PRINT @OK;*/ \
                ///*PRINT @MESSAGE;*/"


                const _results = [];
                const request = new _DB_REQUEST(sql_text, function (err_, count_, rows_) {
                    m.error = err_;
                    m.result = _results; 
                    //console.log('ROWS === ', _rows); 
                    db___execute_callback(m);
                });

                for (var col in data) {
                    if (typeof data[col] == 'string')
                        request.addParameter(col, _DB_TYPES.NVarChar, data[col]);
                    else
                        request.addParameter(col, _DB_TYPES.BigInt, data[col]);
                }

                //request.on('doneProc', function (rowCount, more, rows) {
                //    console.log(rowCount, more, rows);
                //});

                request.on('row', function (columns) {
                    const o = {};
                    columns.forEach(function (v_) {
                        const col = v_.metadata.colName;
                        const val = v_.value;
                        switch (col) {
                            case 'ID':
                                if (v_.value != null)
                                    o[col] = Number(val);
                                else
                                    o[col] = val;
                                break;
                            case 'VAL':
                                if (val != null && val.length > 1 && val[0] == '{')
                                    o[col] = JSON.parse(val);
                                else
                                    o[col] = val;
                                break;
                            default:
                                o[col] = val;
                                break;
                        }
                        //console.log('????????? = ' + v_.metadata.colName, v_.metadata.type.name);
                        //console.log('????????? = ' + v_.metadata.colName, v_.metadata.type.type);
                    });
                    //console.log('????????? = ', o);
                    _results.push(o);
                });

                _DB_CONN.execSql(request);
            }
        }
    }
};

//#endregion

const ___MSG = [];
const f___MSG_PUSH = (res, setting, data) => { ___MSG.push({ client: res, setting: setting, data: data }); };
const f___MSG_UPDATE = function () {
    if (___CONNECTED && ___MSG.length > 0) {
        const m = ___MSG.shift();
        db___execute(m);
    }
    setTimeout(function () { f___MSG_UPDATE(); }, 1);
};

f___MSG_UPDATE();