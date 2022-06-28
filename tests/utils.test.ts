import { setTimeout } from "timers/promises";

import { ClickhouseBuffer } from '../src';
import { Mutex } from "../src/mutex";
import { hrtime } from "process";

describe('clickhouse-buffer', function () {
    it('getRowsInFiles', function () {
        const result = ClickhouseBuffer.getRowsInFiles([
            '1_1_r1000',
            '1_r100',
            '1_1_r10_2'
        ]);
        expect(result).toBe(1110);
    });
});

describe('mutex', function () {
    it('acquire', async function () {
        const mutex = new Mutex();

        const names = [];

        async function f(name: number) {
            await mutex.acquire();
            names.push(name);
            await setTimeout(100);
            mutex.release();
        }

        const sTime = hrtime.bigint();
        await Promise.all([f(1), f(2), f(3), f(4), f(5)]);
        const eTime = hrtime.bigint();

        expect(names).toEqual(expect.arrayContaining([1, 2, 3, 4, 5]));
        expect(eTime - sTime).toBeGreaterThanOrEqual(5 * 100 / 1000 * 1e9);
    });
});