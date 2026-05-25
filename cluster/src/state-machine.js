export class KvStateMachine {
  constructor() {
    this.data = new Map();
  }

  apply(command) {
    switch (command.op) {
      case "set":
        this.data.set(command.key, command.value);
        return { ok: true };
      case "delete":
        this.data.delete(command.key);
        return { ok: true };
      case "get":
        return { ok: true, value: this.data.get(command.key) };
      default:
        return { ok: false, error: "unknown op" };
    }
  }

  snapshot() {
    return Array.from(this.data.entries());
  }
}
