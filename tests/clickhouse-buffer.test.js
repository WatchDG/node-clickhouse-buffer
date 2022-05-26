"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const src_1 = require("../src");
const promises_1 = require("timers/promises");
const promises_2 = require("fs/promises");
describe('clickhouse-buffer', function () {
    jest.setTimeout(10000);
    const ctx = {};
    const database = src_1.DEFAULT_DATABASE;
    const table = 'events';
    beforeAll(async function () {
        const mainDirectoryPath = await (0, promises_2.mkdtemp)('buffer/tmp-');
        const directoryPath = await src_1.ClickhouseBuffer.prepareDirectoryPath(mainDirectoryPath, database, table, 0o777);
        const clickhouseBuffer = new src_1.ClickhouseBuffer({
            directoryPath,
            database,
            table,
            maxRowsInMemory: 10,
            conditions: {
                maxTime: 5000
            }
        });
        await clickhouseBuffer.clickhouseClient.query(`CREATE TABLE IF NOT EXISTS ${database}.${table}
             (
                 id UInt16
             ) ENGINE = MergeTree
             (
             ) ORDER BY id`);
        await clickhouseBuffer.clickhouseClient.query(`TRUNCATE TABLE ${database}.${table}`);
        ctx.clickhouseBuffer = clickhouseBuffer;
        ctx.directoryPath = directoryPath;
    });
    afterAll(async function () {
        if (ctx.clickhouseBuffer) {
            ctx.clickhouseBuffer.release();
        }
    });
    it('insert', async function () {
        for (let i = 0; i < 9; i++) {
            ctx.clickhouseBuffer.push([i]);
        }
        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(9);
        expect(ctx.clickhouseBuffer.fileNamesInMemory()).toBe(0);
        ctx.clickhouseBuffer.push([9]);
        await (0, promises_1.setTimeout)(10);
        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.fileNamesInMemory()).toBe(1);
        await (0, promises_1.setTimeout)(6000);
        const result = await ctx.clickhouseBuffer.clickhouseClient.query(`SELECT *
             FROM ${database}.${table} FORMAT TSVWithNamesAndTypes`);
        expect(result).toBeInstanceOf(Object);
        expect(result).toHaveProperty('rows');
        expect(result.rows).toBe(10);
        expect(result).toHaveProperty('data');
        expect(result.data).toEqual(expect.arrayContaining([
            { id: 0 }, { id: 1 },
            { id: 2 }, { id: 3 },
            { id: 4 }, { id: 5 },
            { id: 6 }, { id: 7 },
            { id: 8 }, { id: 9 }
        ]));
        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.fileNamesInMemory()).toBe(0);
    });
});
