export class Mutex {
    mutex: Uint8Array = new Uint8Array(new SharedArrayBuffer(1));

    acquire(): boolean {
        return Atomics.compareExchange(this.mutex, 0, 0, 1) == 0;
    }

    release() {
        Atomics.store(this.mutex, 0, 0);
    }
}