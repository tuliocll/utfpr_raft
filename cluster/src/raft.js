import { callRpc } from "./rpc.js";

const ELECTION_TIMEOUT_MIN = 1500;
const ELECTION_TIMEOUT_MAX = 3000;
const HEARTBEAT_INTERVAL = 500;

const STATE = {
  FOLLOWER: "follower",
  CANDIDATE: "candidate",
  LEADER: "leader",
};

export class RaftNode {
  constructor({ id, peers, storage, stateMachine }) {
    this.id = id;
    this.peers = peers;
    this.storage = storage;
    this.stateMachine = stateMachine;

    this.state = STATE.FOLLOWER;
    this.leaderId = null;

    this.commitIndex = 0;
    this.lastApplied = 0;

    this.nextIndex = {};
    this.matchIndex = {};

    this.electionTimer = null;
    this.heartbeatTimer = null;

    this.pendingClientRequests = new Map();
  }

  start() {
    console.log(
      `[${this.id}] iniciando como FOLLOWER, term=${this.storage.currentTerm}`,
    );
    this._becomeFollower(this.storage.currentTerm);
  }

  async _maybeStepDown(theirTerm) {
    if (theirTerm > this.storage.currentTerm) {
      await this.storage.setTerm(theirTerm);
      this._becomeFollower(theirTerm);
      return true;
    }
    return false;
  }

  _becomeFollower(term) {
    const wasLeader = this.state === STATE.LEADER;
    this.state = STATE.FOLLOWER;
    this._clearHeartbeatTimer();
    this._resetElectionTimer();

    if (wasLeader) {
      for (const [, { reject }] of this.pendingClientRequests) {
        reject(new Error("not_leader_anymore"));
      }
      this.pendingClientRequests.clear();
    }

    console.log(`[${this.id}] -> FOLLOWER (term=${term})`);
  }

  async _becomeCandidate() {
    const newTerm = this.storage.currentTerm + 1;
    await this.storage.setTerm(newTerm);
    await this.storage.setVotedFor(this.id);

    this.state = STATE.CANDIDATE;
    this.leaderId = null;
    this._resetElectionTimer();
    console.log(
      `[${this.id}] -> CANDIDATE (term=${newTerm}), iniciando eleição`,
    );

    this._startElection(newTerm);
  }

  _becomeLeader() {
    this.state = STATE.LEADER;
    this.leaderId = this.id;
    this._clearElectionTimer();

    const lastIdx = this.storage.lastLogIndex;
    for (const peerId of Object.keys(this.peers)) {
      this.nextIndex[peerId] = lastIdx + 1;
      this.matchIndex[peerId] = 0;
    }

    console.log(`[${this.id}] -> LEADER (term=${this.storage.currentTerm})`);

    this._replicateLog();
    this.heartbeatTimer = setInterval(
      () => this._replicateLog(),
      HEARTBEAT_INTERVAL,
    );
  }

  _resetElectionTimer() {
    this._clearElectionTimer();
    const timeout =
      ELECTION_TIMEOUT_MIN +
      Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
    this.electionTimer = setTimeout(() => this._onElectionTimeout(), timeout);
  }

  _clearElectionTimer() {
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }
  }

  _clearHeartbeatTimer() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  async _onElectionTimeout() {
    if (this.state === STATE.LEADER) return;
    await this._becomeCandidate();
  }

  async _startElection(term) {
    let votesReceived = 1;
    const totalNodes = Object.keys(this.peers).length + 1;
    const majority = Math.floor(totalNodes / 2) + 1;

    const args = {
      term,
      candidateId: this.id,
      lastLogIndex: this.storage.lastLogIndex,
      lastLogTerm: this.storage.lastLogTerm,
    };

    for (const [, peerAddress] of Object.entries(this.peers)) {
      callRpc(peerAddress, "request-vote", args).then(async (reply) => {
        if (!reply) return;
        if (this.state !== STATE.CANDIDATE || this.storage.currentTerm !== term)
          return;
        if (await this._maybeStepDown(reply.term)) return;

        if (reply.voteGranted) {
          votesReceived++;
          if (votesReceived >= majority) {
            this._becomeLeader();
          }
        }
      });
    }
  }

  async handleRequestVote(args) {
    await this._maybeStepDown(args.term);

    const reply = { term: this.storage.currentTerm, voteGranted: false };
    if (args.term < this.storage.currentTerm) return reply;

    const canVote =
      this.storage.votedFor === null ||
      this.storage.votedFor === args.candidateId;

    const myLastTerm = this.storage.lastLogTerm;
    const myLastIdx = this.storage.lastLogIndex;
    const logOk =
      args.lastLogTerm > myLastTerm ||
      (args.lastLogTerm === myLastTerm && args.lastLogIndex >= myLastIdx);

    if (canVote && logOk) {
      await this.storage.setVotedFor(args.candidateId);
      reply.voteGranted = true;
      this._resetElectionTimer();
    }

    return reply;
  }

  async submitCommand(command) {
    if (this.state !== STATE.LEADER) {
      const err = new Error("not_leader");
      err.leaderId = this.leaderId;
      err.leaderAddress = this.leaderId ? this.peers[this.leaderId] : "";
      throw err;
    }

    const newIndex = this.storage.lastLogIndex + 1;
    const entry = {
      term: this.storage.currentTerm,
      index: newIndex,
      command,
    };
    await this.storage.appendEntries([entry]);

    const promise = new Promise((resolve, reject) => {
      this.pendingClientRequests.set(newIndex, { resolve, reject });
    });

    this._replicateLog();
    return promise;
  }

  _replicateLog() {
    if (this.state !== STATE.LEADER) return;
    const term = this.storage.currentTerm;
    for (const [peerId, peerAddress] of Object.entries(this.peers)) {
      this._replicateToPeer(peerId, peerAddress, term);
    }
  }

  async _replicateToPeer(peerId, peerAddress, term) {
    const nextIdx = this.nextIndex[peerId];
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm = this.storage.log[prevLogIndex].term;
    const entries = this.storage.log.slice(nextIdx);

    const args = {
      term,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.commitIndex,
    };

    const reply = await callRpc(peerAddress, "append-entries", args, 300);
    if (!reply) return;

    if (this.state !== STATE.LEADER || this.storage.currentTerm !== term)
      return;
    if (await this._maybeStepDown(reply.term)) return;

    if (reply.success) {
      const newMatch = prevLogIndex + entries.length;
      this.matchIndex[peerId] = Math.max(this.matchIndex[peerId], newMatch);
      this.nextIndex[peerId] = this.matchIndex[peerId] + 1;
      this._maybeAdvanceCommitIndex();
    } else {
      this.nextIndex[peerId] = Math.max(1, this.nextIndex[peerId] - 1);
    }
  }

  _maybeAdvanceCommitIndex() {
    if (this.state !== STATE.LEADER) return;

    const indices = Object.values(this.matchIndex);
    indices.push(this.storage.lastLogIndex);
    indices.sort((a, b) => b - a);

    const majorityIdx = Math.floor(indices.length / 2);
    const candidateCommit = indices[majorityIdx];

    if (candidateCommit <= this.commitIndex) return;
    if (this.storage.log[candidateCommit].term !== this.storage.currentTerm)
      return;

    this.commitIndex = candidateCommit;

    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.storage.log[this.lastApplied];
      const result = this.stateMachine.apply(entry.command);

      const pending = this.pendingClientRequests.get(this.lastApplied);
      if (pending) {
        pending.resolve(result);
        this.pendingClientRequests.delete(this.lastApplied);
      }
    }
  }

  async handleAppendEntries(args) {
    await this._maybeStepDown(args.term);

    const reply = { term: this.storage.currentTerm, success: false };

    if (args.term < this.storage.currentTerm) return reply;

    this._resetElectionTimer();
    this.leaderId = args.leaderId;
    if (this.state === STATE.CANDIDATE) {
      this._becomeFollower(this.storage.currentTerm);
    }

    if (args.prevLogIndex > this.storage.lastLogIndex) return reply;
    if (this.storage.log[args.prevLogIndex].term !== args.prevLogTerm)
      return reply;

    const entries = args.entries || [];
    if (entries.length > 0) {
      let insertAt = args.prevLogIndex + 1;
      let entriesToAppend = entries;

      while (
        insertAt <= this.storage.lastLogIndex &&
        entriesToAppend.length > 0
      ) {
        const mine = this.storage.log[insertAt];
        const theirs = entriesToAppend[0];
        if (mine.term !== theirs.term) {
          await this.storage.truncateLogFrom(insertAt);
          break;
        }
        insertAt++;
        entriesToAppend = entriesToAppend.slice(1);
      }

      if (entriesToAppend.length > 0) {
        await this.storage.appendEntries(entriesToAppend);
      }
    }

    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.storage.lastLogIndex);
      while (this.lastApplied < this.commitIndex) {
        this.lastApplied++;
        this.stateMachine.apply(this.storage.log[this.lastApplied].command);
      }
    }

    reply.success = true;
    return reply;
  }

  status() {
    return {
      id: this.id,
      state: this.state,
      currentTerm: this.storage.currentTerm,
      votedFor: this.storage.votedFor,
      leaderId: this.leaderId,
      lastLogIndex: this.storage.lastLogIndex,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
    };
  }
}
