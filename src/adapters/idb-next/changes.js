'use strict';

import { DOC_STORE } from './util';
import filterChange from '../../deps/filterChange';

export default function(idb, opts) {

  var limit = 'limit' in opts ? opts.limit : -1;
  if (limit === 0) {
    limit = 1;
  }

  var returnDocs = 'return_docs' in opts ? opts.return_docs : true;

  var txn = idb.transaction([DOC_STORE], 'readonly');
  var store = txn.objectStore(DOC_STORE).index('seq');

  var filter = filterChange(opts);
  var received = 0;

  var lastSeq = 0;
  var results = [];

  function onReqSuccess(e) {
    if (!e.target.result) { return; }
    var cursor = e.target.result;
    var doc = cursor.value;
    // TODO: fix this;
    doc.data._rev = doc.rev;

    // WARNING: expecting possible old format
    var change = opts.processChange(doc.data, doc, opts);
    change.seq = doc.seq;
    lastSeq = doc.seq;
    var filtered = filter(change);

    // If its an error
    if (typeof filtered === 'object') {
      return opts.complete(filtered);
    }

    if (filtered) {
      received++;
      if (returnDocs) {
        results.push(change);
      }
      opts.onChange(change);
    }
    if (received !== limit) {
      cursor.continue();
    }
  }

  function onTxnComplete() {
    opts.complete(null, {
      results: results,
      last_seq: lastSeq
    });
  }

  var req;
  if (opts.descending) {
    req = store.openCursor(null, 'prev');
  } else {
    req = store.openCursor(IDBKeyRange.lowerBound(opts.since));
  }

  txn.oncomplete = onTxnComplete;
  req.onsuccess = onReqSuccess;
}
