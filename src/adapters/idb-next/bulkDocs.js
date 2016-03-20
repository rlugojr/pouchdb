'use strict';

import { createError, REV_CONFLICT, MISSING_DOC } from '../../deps/errors';
import { parseDoc } from '../../deps/docs/parseDoc';

import merge from '../../deps/merge/index';
import calculateWinningRev from '../../deps/merge/winningRev';

import { META_STORE, DOC_STORE, ATTACH_STORE } from './util';
import { idbError } from './util';

export default function(db, req, opts, seq, callback) {

  var stores = [DOC_STORE, ATTACH_STORE, META_STORE];
  var txn = db.transaction(stores, 'readwrite');
  var results = [];

  function rootIsMissing(doc) {
    return doc.rev_tree[0].ids[1].status === 'missing';
  }

  // Reads the original doc from the store if available
  function fetchExistingDocs(txn, docs) {
    return new Promise(function (resolve) {
      var fetched = 0;
      var oldDocs = {};

      function readDone(e) {
        if (e.target.result) {
          oldDocs[e.target.result.id] = e.target.result;
        }
        if (++fetched === docs.length) {
          resolve(oldDocs);
        }
      }

      docs.forEach(function(doc) {
        txn.objectStore(DOC_STORE).get(doc.id).onsuccess = readDone;
      });
    });
  }

  function processDocs(txn, docs, oldDocs) {

    docs.forEach(function(doc, i) {
      var newDoc;

      // The first document write cannot be a deletion
      if ('was_delete' in opts && !(doc.id in oldDocs)) {
        newDoc = createError(MISSING_DOC, 'deleted');

      // Update the existing document
      } else if (doc.id in oldDocs) {
        newDoc = update(txn, doc, oldDocs[doc.id]);

      // New document
      } else {
        newDoc = doc;
      }

      // First document write cannot have a revision
      // TODO: Pretty unclear implementation, revisit
      if (opts.new_edits && rootIsMissing(doc)) {
        newDoc = createError(REV_CONFLICT);
      }

      if (newDoc.error) {
        results[i] = newDoc;
      } else {
        write(txn, newDoc, i);
      }
    });
  }

  // Converts from the format returned by parseDoc into the new format
  // we use to store
  function convertDocFormat(doc) {

    var newDoc = {
      id: doc.metadata.id,
      rev: doc.metadata.rev,
      rev_tree: doc.metadata.rev_tree,
      revs: doc.metadata.revs || {}
    };

    newDoc.revs[newDoc.rev] = {
      data: doc.data,
      deleted: doc.metadata.deleted
    }

    return newDoc;
  }

  function update(txn, doc, oldDoc) {

    var isRoot = /^1-/.test(doc.rev);

    // Reattach first writes after a deletion to last deleted tree
    if (oldDoc.deleted && !doc.deleted && opts.new_edits && isRoot) {
      var tmp = doc.revs[doc.rev].data;
      tmp._rev = oldDoc.rev;
      tmp._id = oldDoc.id;
      doc = convertDocFormat(parseDoc(tmp, opts.new_edits));
    }

    var merged = merge(oldDoc.rev_tree, doc.rev_tree[0], 100);
    doc.rev_tree = merged.tree;

    var inConflict = opts.new_edits && (((oldDoc.deleted && doc.deleted) ||
       (!oldDoc.deleted && merged.conflicts !== 'new_leaf') ||
       (oldDoc.deleted && !doc.deleted && merged.conflicts === 'new_branch')));

    if (inConflict) {
      return createError(REV_CONFLICT);
    }

    return doc;
  }

  function write(txn, doc, i) {

    // We copy the data from the winning revision into the root
    // of the document so that it can be indexed
    var winningRev = calculateWinningRev(doc);
    doc.data = doc.revs[winningRev].data;
    doc.rev = winningRev;
    // .deleted needs to be an int for indexing
    doc.deleted = doc.revs[winningRev].deleted ? 1 : 0;

    // TODO: fix this
    doc.seq = seq.seq;
    seq.seq += 1;

    doc.winningRev = calculateWinningRev(doc);

    txn.objectStore(DOC_STORE).put(doc).onsuccess = function() {
      results[i] = {
        ok: true,
        id: doc.id,
        rev: doc.rev
      };
    };
  }

  var docs = [];
  for (var i = 0, len = req.docs.length; i < len; i++) {
    var result;
    // TODO: We should get rid of throwing for invalid docs, also not sure
    // why this is needed in idb-next and not idb
    try {
      result = parseDoc(req.docs[i], opts.new_edits);
    } catch (err) {
      result = err;
    }
    if (result.error) {
      return callback(result);
    }

    // Ideally parseDoc would return data in this format, but it is currently
    // shared
    var newDoc = {
      id: result.metadata.id,
      rev: result.metadata.rev,
      rev_tree: result.metadata.rev_tree,
      revs: result.metadata.revs || {}
    };

    newDoc.revs[newDoc.rev] = {
      data: result.data,
      deleted: result.metadata.deleted
    }

    docs.push(convertDocFormat(result));
  }

  txn.onabort = idbError(callback);
  txn.ontimeout = idbError(callback);

  txn.oncomplete = function() {
    callback(null, results);
  };

  fetchExistingDocs(txn, docs).then(function(oldDocs) {
    processDocs(txn, docs, oldDocs);
  });
};
