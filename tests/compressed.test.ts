import { ClickhouseBuffer, DEFAULT_DATABASE } from '../src';
import { setTimeout } from "timers/promises";
import { mkdir, mkdtemp } from 'fs/promises';

describe('compressed: gzip', function () {
    const ctx: { clickhouseBuffer?: ClickhouseBuffer, directoryPath?: string } = {};
    const database = DEFAULT_DATABASE;
    const table = 'test_compressed_gzip';

    beforeAll(async function () {
        await mkdir('buffer', { recursive: true });
        const mainDirectoryPath = await mkdtemp('buffer/tmp-');
        const directoryPath = await ClickhouseBuffer.prepareDirectoryPath(mainDirectoryPath, database, table, 0o777);

        const clickhouseBuffer = new ClickhouseBuffer({
            directoryPath,
            database,
            table,
            maxRowsInMemory: 10,
            conditions: {
                maxRows: 10
            },
            compressed: 'gzip',
            compressedFiles: "br"
        });

        await clickhouseBuffer.clickhouseClient.query(
            `CREATE TABLE IF NOT EXISTS ${database}.${table}
             (
                 id UInt16
             ) ENGINE = MergeTree
             (
             ) ORDER BY id`
        );
        await clickhouseBuffer.clickhouseClient.query(
            `TRUNCATE TABLE ${database}.${table}`
        );

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

        await setTimeout(10);

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(9);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);

        ctx.clickhouseBuffer.push([9]);

        await setTimeout(100);

        const result = await ctx.clickhouseBuffer.clickhouseClient.query(
            `SELECT *
             FROM ${database}.${table} FORMAT TSVWithNamesAndTypes`
        );
        expect(result).toBeInstanceOf(Object);
        expect(result).toHaveProperty('rows');
        expect(result.rows).toBe(10);
        expect(result).toHaveProperty('data');
        expect(result.data).toEqual(
            expect.arrayContaining([
                { id: 0 }, { id: 1 },
                { id: 2 }, { id: 3 },
                { id: 4 }, { id: 5 },
                { id: 6 }, { id: 7 },
                { id: 8 }, { id: 9 }
            ])
        );

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);
    });
});

describe('compressed: br', function () {
    const ctx: { clickhouseBuffer?: ClickhouseBuffer, directoryPath?: string } = {};
    const database = DEFAULT_DATABASE;
    const table = 'test_compressed_br';

    beforeAll(async function () {
        const mainDirectoryPath = await mkdtemp('buffer/tmp-');
        const directoryPath = await ClickhouseBuffer.prepareDirectoryPath(mainDirectoryPath, database, table, 0o777);

        const clickhouseBuffer = new ClickhouseBuffer({
            directoryPath,
            database,
            table,
            maxRowsInMemory: 10,
            conditions: {
                maxRows: 10
            },
            compressed: 'br',
            compressedFiles: "deflate"
        });

        await clickhouseBuffer.clickhouseClient.query(
            `CREATE TABLE IF NOT EXISTS ${database}.${table}
             (
                 id UInt16
             ) ENGINE = MergeTree
             (
             ) ORDER BY id`
        );
        await clickhouseBuffer.clickhouseClient.query(
            `TRUNCATE TABLE ${database}.${table}`
        );

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

        await setTimeout(10);

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(9);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);

        ctx.clickhouseBuffer.push([9]);

        await setTimeout(100);

        const result = await ctx.clickhouseBuffer.clickhouseClient.query(
            `SELECT *
             FROM ${database}.${table} FORMAT TSVWithNamesAndTypes`
        );
        expect(result).toBeInstanceOf(Object);
        expect(result).toHaveProperty('rows');
        expect(result.rows).toBe(10);
        expect(result).toHaveProperty('data');
        expect(result.data).toEqual(
            expect.arrayContaining([
                { id: 0 }, { id: 1 },
                { id: 2 }, { id: 3 },
                { id: 4 }, { id: 5 },
                { id: 6 }, { id: 7 },
                { id: 8 }, { id: 9 }
            ])
        );

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);
    });
});

describe('compressed: deflate', function () {
    const ctx: { clickhouseBuffer?: ClickhouseBuffer, directoryPath?: string } = {};
    const database = DEFAULT_DATABASE;
    const table = 'test_compressed_deflate';

    beforeAll(async function () {
        const mainDirectoryPath = await mkdtemp('buffer/tmp-');
        const directoryPath = await ClickhouseBuffer.prepareDirectoryPath(mainDirectoryPath, database, table, 0o777);

        const clickhouseBuffer = new ClickhouseBuffer({
            directoryPath,
            database,
            table,
            maxRowsInMemory: 10,
            conditions: {
                maxRows: 10
            },
            compressed: 'deflate',
            compressedFiles: "gzip"
        });

        await clickhouseBuffer.clickhouseClient.query(
            `CREATE TABLE IF NOT EXISTS ${database}.${table}
             (
                 id UInt16
             ) ENGINE = MergeTree
             (
             ) ORDER BY id`
        );
        await clickhouseBuffer.clickhouseClient.query(
            `TRUNCATE TABLE ${database}.${table}`
        );

        ctx.clickhouseBuffer = clickhouseBuffer;
        ctx.directoryPath = directoryPath;
    });

    afterAll(async function () {
        if (ctx.clickhouseBuffer) {
            await ctx.clickhouseBuffer.release();
        }
    });

    it('insert', async function () {
        for (let i = 0; i < 9; i++) {
            ctx.clickhouseBuffer.push([i]);
        }

        await setTimeout(10);

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(9);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);

        ctx.clickhouseBuffer.push([9]);

        await setTimeout(100);

        const result = await ctx.clickhouseBuffer.clickhouseClient.query(
            `SELECT *
             FROM ${database}.${table} FORMAT TSVWithNamesAndTypes`
        );
        expect(result).toBeInstanceOf(Object);
        expect(result).toHaveProperty('rows');
        expect(result.rows).toBe(10);
        expect(result).toHaveProperty('data');
        expect(result.data).toEqual(
            expect.arrayContaining([
                { id: 0 }, { id: 1 },
                { id: 2 }, { id: 3 },
                { id: 4 }, { id: 5 },
                { id: 6 }, { id: 7 },
                { id: 8 }, { id: 9 }
            ])
        );

        expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
        expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);
    });
});