// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;
// HARD PARAPHRASE 
LockManager::~LockManager() {
  // Cleanup lock_table_
  auto iterator = lock_table_.begin();
  while(iterator != lock_table_.end()){
    delete iterator->second;
    iterator++;
  }
  // for (iterator = lock_table_.begin(); iterator != lock_table_.end(); iterator++) {
  //   delete iterator->second;
  // }
}

// HARD PARAPHRASE 
deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  deque<LockRequest> *lockedresource = lock_table_[key];
  if (!lockedresource) {
    lockedresource = new deque<LockRequest>();
    lock_table_[key] = lockedresource;
  }
  return lockedresource;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

// PARAPHRASE
bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  deque<LockRequest> *LockQueue = _getLockQueue(key);
  LockRequest lockRequest(EXCLUSIVE, txn);
  bool empty = true;

  empty = LockQueue->empty();
  LockQueue->push_back(lockRequest);

  if (!empty) { // Add to wait list, doesn't own lock.
    txn_waits_[txn]++;
  }
  return empty;
}


bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

// PARAPHRASE
void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);
  bool removedOwner = true; // Is the lock removed the lock owner?
  auto iterator= queue->begin();
  // Delete the txn's exclusive lock.
  while (iterator < queue->end()){
    if (iterator->txn_ == txn) { // TODO is it ok to just compare by address?
        queue->erase(iterator);
        break;
    }
    removedOwner = false;
    iterator++;
  }
  // for (iterator = queue->begin(); iterator < queue->end(); iterator++) {
  //   if (it->txn_ == txn) { // TODO is it ok to just compare by address?
  //       queue->erase(it);
  //       break;
  //   }
  //   removedOwner = false;
  // }

  if (!queue->empty() && removedOwner) {
    // Give the next transaction the lock
    LockRequest next = queue->front();

    if (--txn_waits_[next.txn_] == 0) {
        ready_txns_->push_back(next.txn_);
        txn_waits_.erase(next.txn_);
    }
  }
}

// PARAPHRASE
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *lockStatus = _getLockQueue(key);
  if (lockStatus->empty()) {
    return UNLOCKED;
  } else {
    vector<Txn*> tempowners;
    tempowners.push_back(lockStatus->front().txn_);
    *owners = tempowners;
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

// HARD PARAPHRASE 
bool LockManagerB::_addLock(LockMode lockMode, Txn* txn, const Key& key) {
  LockMode status = Status(key, nullptr);
  LockRequest request(lockMode, txn);

  deque<LockRequest> *lockStatus = _getLockQueue(key);
  lockStatus->push_back(request);

  bool grantAccess = status == UNLOCKED;
  if (lockMode == SHARED) {
    grantAccess |= _noExclusiveWaiting(key);
  } else {
    _numExclusiveWaiting[key]++;
  }

  if (!grantAccess)
    txn_waits_[txn]++;

  return grantAccess;
}

// PARAPHRASE
bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  return _addLock(EXCLUSIVE, txn, key);
}

// PARAPHRASE
bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  return _addLock(SHARED, txn, key);
}

// PARAPHRASE
void LockManagerB::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);

  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) {
      queue->erase(it);
      if (it->mode_ == EXCLUSIVE) {
        _numExclusiveWaiting[key]--;
      }

      break;
    }
  }

  // Advance the lock, by making new owners ready.
  // Some in newOwners already own the lock.  These are not in
  // txn_waits_.
  vector<Txn*> newOwners;
  Status(key, &newOwners);

  for (auto&& owner : newOwners) {
    auto waitCount = txn_waits_.find(owner);
    if (waitCount != txn_waits_.end() && --(waitCount->second) == 0) {
      ready_txns_->push_back(owner);
      txn_waits_.erase(waitCount);
    }
  }
}

// PARAPHRASE
LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  }

  LockMode mode = EXCLUSIVE;
  vector<Txn*> txn_owners;
  for (auto&& lockRequest : *dq) {
    if (lockRequest.mode_ == EXCLUSIVE && mode == SHARED)
        break;

    txn_owners.push_back(lockRequest.txn_);
    mode = lockRequest.mode_;

    if (mode == EXCLUSIVE)
      break;
  }

  if (owners)
    *owners = txn_owners;

  return mode;
}

// HARD PARAPHRASE 
inline bool LockManagerB::_noExclusiveWaiting(const Key& key) {
  return _numExclusiveWaiting[key] == 0;
}
