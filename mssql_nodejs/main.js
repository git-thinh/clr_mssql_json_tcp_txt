const ___CONNECTED = false;


const _PATH = require('path');
const _FS = require('fs');
const _FETCH = require('node-fetch');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;


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



const ___TOKEN_API = 'eb976d531188435ea006fce8769c53d5';
_HTTP_APP.post('/biz/:connect_string/:api_name/:store_action/:token', async (req, res) => {
    const data = req.body;
    try {
        const connect_string = req.params.connect_string.toLowerCase();
        const api_name = req.params.api_name.toUpperCase();
        const store_action = req.params.store_action;
        const token = req.params.token.toLowerCase();

        if (connect_string.length == 0
            || api_name.length == 0
            || store_action.length == 0
            || token.length == 0) {
            res.json({ ok: false, message: 'Uri of APIs must be: api/biz/:connect_string/:api_name/:store_action/:token ' });
            return;
        }

        if (___TOKEN_API != token) {
            res.json({ ok: false, message: 'TOKEN invalid: ' + token });
            return;
        }

        if (connect_string != 'amz' && connect_string != '123') {
            res.json({ ok: false, message: 'CONNECT_STRING invalid, they are (amz|123)' });
            return;
        }
         
    } catch (err_throw) {
        //___log_err_throw('/biz/:connect_string/:api_name/:store_action/:token', err_throw, req.path, data);
        res.json({ ok: false, message: 'ERR_THROW: ' + err_throw });
    }
});

