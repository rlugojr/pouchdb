'use strict';

import getArguments from 'argsarray';

import uuid from '../../deps/uuid';
import { createError, MISSING_DOC } from '../../deps/errors';

import { META_STORE, DOC_STORE, ATTACH_STORE } from './util';

// API implementations
import info from './info';
import get from './get';
import bulkDocs from './bulkDocs';
import allDocs from './allDocs';
import changes from './changes';

var IDB_VERSION = 1;

var dbPromise;

function createSchema (db) {

  var docStore = db.createObjectStore(DOC_STORE, {keyPath : 'id'});
  docStore.createIndex('deleted', 'deleted', {unique: false});
  docStore.createIndex('seq', 'seq', {unique: true});

  db.createObjectStore(ATTACH_STORE, {keyPath: 'digest'});
  db.createObjectStore(META_STORE, {keyPath: 'id'});
}

function setupDatabase (opts) {

  if (dbPromise) {
    return dbPromise;
  }

  dbPromise = new Promise(function(resolve, reject) {

    var req = opts.storage
      ? indexedDB.open(opts.name, {version: IDB_VERSION, storage: opts.storage})
      : indexedDB.open(opts.name, IDB_VERSION);

    req.onupgradeneeded = function (e) {
      var db = e.target.result;
      if (e.oldVersion < 1) {
        createSchema(db);
      }
    };

    req.onsuccess = function (e) {
      var idb = e.target.result;
      idb.onabort = function (e) {
        console.error('Database has a global failure', e.target.error);
        dbPromise = null;
        idb.close();
      };
      resolve(idb);
    };
  });

  return dbPromise;
}

function IdbPouch (opts, callback) {

  var api = this;
  var dbName = opts.name;

  var data = {
    seq: 1
  };

  // This is a wrapper function for any methods that need an
  // active database handle it will recall itself but with
  // the database handle as the first argument
  var $ = function(fun) {
    return getArguments(function (args) {
      setupDatabase(opts).then(function (db) {
        args.unshift(db);
        fun.apply(api, args);
      });
    });
  };

  api.type = function () { return 'idb-next'; };
  api._id = $(function(idb, cb) { cb(null, '123'); });

  api._info = $(function(idb, cb) {
    return info(idb, data, cb);
  });

  api._get = $(get);

  api._bulkDocs = $(function(idb, req, opts, callback) {
    return bulkDocs(idb, req, opts, data, callback);
  });

  api._allDocs = $(function (idb, opts, cb) {
    return allDocs(idb, api, opts, cb);
  });

  api._getAttachment = $(function (db, attachment, opts) {
  });


  api._changes = $(function (idb, opts) {
    return changes(idb, opts);
  });

  api._getRevisionTree = $(function (db, id, callback) {
    var txn = db.transaction([DOC_STORE], 'readonly');
    var req = txn.objectStore(DOC_STORE).get(id);
    req.onsuccess = function (e) {
      if (!e.target.result) {
        callback(createError(MISSING_DOC));
      } else {
        callback(null, e.target.result.rev_tree);
      }
    }
  });

  api._doCompaction = $(function (db, id, revs, callback) {
  });

  api._destroy = function (opts, callback) {

    function doDestroy() {
      var req = indexedDB.deleteDatabase(dbName);
      req.onsuccess = function() {
        dbPromise = null;
        callback(null, {ok: true});
      };
    }

    // If the database is open we need to close it
    if (dbPromise) {
      dbPromise.then(function(db) {
        db.close();
        doDestroy();
      });
    } else {
      doDestroy();
    }
  };

  api._close = $(function (db, cb) {
    dbPromise = null;
    db.close();
    cb();
  });

  // TODO: this setTimeout seems nasty, if its needed lets
  // figure out / explain why
  setTimeout(function() {
    callback(null, api);
  });
}

// TODO: this isnt really valid permanently, just being lazy to start
IdbPouch.valid = function () {
  return true;
};

export default IdbPouch;
