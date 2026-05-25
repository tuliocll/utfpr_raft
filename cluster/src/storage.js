import { promises as fs } from "node:fs";
import path from "node:path";

export class Storage {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.statePath = path.join(dataDir, "state.json");
    this.state = {
      currentTerm: 0,
      votedFor: null,
      log: [
        { term: 0, command: null, index: 0 },
      ],
    };
  }

  async init() {
    await fs.mkdir(this.dataDir, { recursive: true });
    try {
      const raw = await fs.readFile(this.statePath, "utf-8");
      this.state = JSON.parse(raw);
      console.log(
        `[storage] estado restaurado: term=${this.state.currentTerm}, logLen=${this.state.log.length}`,
      );
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
      await this._flush();
    }
  }

  async _flush() {
    const tmp = this.statePath + ".tmp";
    const fh = await fs.open(tmp, "w");
    try {
      await fh.writeFile(JSON.stringify(this.state));
      await fh.sync();
    } finally {
      await fh.close();
    }
    await fs.rename(tmp, this.statePath);
  }

  async setTerm(term) {
    if (term === this.state.currentTerm) return;
    this.state.currentTerm = term;
    this.state.votedFor = null;
    await this._flush();
  }

  async setVotedFor(candidateId) {
    this.state.votedFor = candidateId;
    await this._flush();
  }

  async appendEntries(entries) {
    this.state.log.push(...entries);
    await this._flush();
  }

  async truncateLogFrom(index) {
    this.state.log = this.state.log.slice(0, index);
    await this._flush();
  }

  get currentTerm() {
    return this.state.currentTerm;
  }
  get votedFor() {
    return this.state.votedFor;
  }
  get log() {
    return this.state.log;
  }
  get lastLogIndex() {
    return this.state.log.length - 1;
  }
  get lastLogTerm() {
    return this.state.log[this.state.log.length - 1].term;
  }
}
