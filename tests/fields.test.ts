import { ClickhouseBuffer, DEFAULT_DATABASE } from '../src';
import { setTimeout } from "timers/promises";
import { mkdtemp } from 'fs/promises';

describe('clickhouse-buffer', function () {

    describe('add field names to settings', function () {
        const ctx: { clickhouseBuffer?: ClickhouseBuffer, directoryPath?: string, database: string, table: string } = {
            database: DEFAULT_DATABASE,
            table: 'fieldSettings0'
        };

        beforeAll(async function () {
            const mainDirectoryPath = await mkdtemp('buffer/tmp-');
            const directoryPath = await ClickhouseBuffer.prepareDirectoryPath(mainDirectoryPath, ctx.database, ctx.table, 0o777);

            const clickhouseBuffer = new ClickhouseBuffer({
                directoryPath,
                database: ctx.database,
                table: ctx.table,
                maxRowsInMemory: 10,
                conditions: {
                    maxRows: 10
                },
                fields: [{
                    name: 'id',
                }]
            });

            await clickhouseBuffer.clickhouseClient.query(`
                DROP TABLE IF EXISTS ${ctx.database}.${ctx.table}
            `);
            await clickhouseBuffer.clickhouseClient.query(
                `CREATE TABLE ${ctx.database}.${ctx.table}
                 (
                     id        UInt16,
                     firstName String
                 ) ENGINE = MergeTree
                 (
                 ) ORDER BY id`
            );
            await clickhouseBuffer.clickhouseClient.query(
                `TRUNCATE TABLE ${ctx.database}.${ctx.table}`
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

            await setTimeout(500);

            const result = await ctx.clickhouseBuffer.clickhouseClient.query(
                `SELECT *
                 FROM "${ctx.database}"."${ctx.table}" FORMAT TSVWithNamesAndTypes`
            );
            expect(result).toBeInstanceOf(Object);
            expect(result).toHaveProperty('rows');
            expect(result.rows).toBe(10);
            expect(result).toHaveProperty('data');
            expect(result.data).toEqual(
                expect.arrayContaining([
                    { id: 0, firstName: '' }, { id: 1, firstName: '' },
                    { id: 2, firstName: '' }, { id: 3, firstName: '' },
                    { id: 4, firstName: '' }, { id: 5, firstName: '' },
                    { id: 6, firstName: '' }, { id: 7, firstName: '' },
                    { id: 8, firstName: '' }, { id: 9, firstName: '' }
                ])
            );

            expect(ctx.clickhouseBuffer.rowsInMemory()).toBe(0);
            expect(ctx.clickhouseBuffer.filesInMemory()).toBe(0);
            expect(ctx.clickhouseBuffer.rowsInFiles()).toBe(0);
        });
    });

});