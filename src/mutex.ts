export class Mutex {
    private readonly mutex = new Int32Array(new SharedArrayBuffer(4));

    async acquire() {
        let oldValue = Atomics.compareExchange(this.mutex, 0, 0, 1);
        while (oldValue !== 0) {
            let waitValue;
            do {
                const { async, value } = await (Atomics as any).waitAsync(this.mutex, 0, 1);
                waitValue = async ? await value : value;
            } while (waitValue !== 'not-equal');
            oldValue = Atomics.compareExchange(this.mutex, 0, 0, 1);
        }
    }

    release() {
        Atomics.store(this.mutex, 0, 0);
        Atomics.notify(this.mutex, 0, 1);
    }
}