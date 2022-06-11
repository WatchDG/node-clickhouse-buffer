import path from 'path';
import { mkdir, writeFile, rm, readdir } from 'fs/promises';
import { hrtime } from 'process';
import { ClickhouseClient, DEFAULT_DATABASE } from "@watchdg/clickhouse-client";

export { DEFAULT_DATABASE } from "@watchdg/clickhouse-client";

import { filesToStream } from "./files_to_stream";
import { Mutex } from "./mutex";

import type { ClickhouseClientOptions } from "@watchdg/clickhouse-client";

interface Conditions {
    maxTime?: number;
    maxRows?: number;
}

interface Options {
    clickhouseClient?: ClickhouseClientOptions;
    database?: string;
    table: string;
    maxRowsInMemory?: number;
    maxRowsPerFile?: number;
    maxFilesPerLoad?: number;
    directoryPath: string;
    fsMode?: number;
    conditions?: Conditions;
}

type columnType = string | number | Date | boolean;

export class ClickhouseBuffer {
    private readonly directoryPath: string;
    private readonly fsMode: number = 0o777;

    private readonly maxRowsPerFile: number = 1000;
    private readonly maxRowsInMemory: number = 1000;
    private readonly maxFilesPerLoad: number = 100;

    private rows: string[] = [];
    private readonly files: string[] = [];

    private readonly conditions: Conditions = { maxTime: 5000 };

    private readonly loadToDatabaseMutex: Mutex = new Mutex();

    private readonly maxTimeTimer?: NodeJS.Timer;
    private lastLoadDate: number = Date.now();

    private statRowsInFiles: number = 0;

    private readonly insertStatement: string;

    readonly clickhouseClient: ClickhouseClient;

    private static fmtRow(row: Array<columnType>): string {
        for (let i = 0, l = row.length; i < l; i++) {
            const columnValue = row[i];
            if (columnValue instanceof Date) {
                row[i] = columnValue.getTime() / 1000 | 0;
            } else if (typeof columnValue == 'boolean') {
                row[i] = Number(columnValue);
            }
        }
        return JSON.stringify(row);
    }

    private static calcBytes(rows: string[]): number {
        return rows.reduce(function (bytes, row) {
            return bytes + Buffer.byteLength(row);
        }, 0);
    }

    private static isConditionMet(self: ClickhouseBuffer): boolean {
        return (self.conditions?.maxTime && (Date.now() - self.lastLoadDate) >= self.conditions.maxTime) ||
            (self.conditions?.maxRows && self.statRowsInFiles >= self.conditions.maxRows);
    }

    private static maxTimeHandler(self: ClickhouseBuffer): void {
        setImmediate(ClickhouseBuffer.flushToFiles, self, self.rows, true);
        self.rows = [];
    }

    private static async flushToFiles(self: ClickhouseBuffer, rows: string[], checkConditions: boolean = false): Promise<void> {
        const rowsLength = rows.length;

        if (rowsLength > 0) {
            const files = [];
            const sortKeys = `${Date.now() / 1000 | 0}_${(hrtime.bigint() % 10_000_000_000n).toString(10)}`;
            const parts = Math.ceil(rowsLength / self.maxRowsPerFile);
            for (let part = 0; part < parts; part++) {
                const numRowsToFile = rows.length >= self.maxRowsPerFile ? self.maxRowsPerFile : rows.length;
                const rowsToFile = rows.splice(0, numRowsToFile);
                const numBytesToFile = ClickhouseBuffer.calcBytes(rowsToFile);
                const dataToFile = rowsToFile.join('\n') + '\n';
                const filename = `${sortKeys}_${part}_r${numRowsToFile}_b${numBytesToFile}`;
                await writeFile(path.join(self.directoryPath, filename), dataToFile, { mode: self.fsMode });
                files.push(filename);
            }
            self.files.push(...files);
            self.statRowsInFiles += rowsLength;
        }

        if (checkConditions && ClickhouseBuffer.isConditionMet(self)) {
            const numOfFiles = self.files.length >= self.maxFilesPerLoad ? self.maxFilesPerLoad : self.files.length;
            if (!(numOfFiles > 0)) {
                return;
            }
            const files = self.files.splice(0, numOfFiles);
            setImmediate(ClickhouseBuffer.loadToDatabase, self, files);
        }
    }

    private static async loadToDatabase(self: ClickhouseBuffer, files: string[]) {
        const rowsInFiles = ClickhouseBuffer.getRowsInFiles(files);
        const paths = files.map(function (filename) {
            return path.join(self.directoryPath, filename);
        });
        const stream = filesToStream(Array.from(paths));
        await self.clickhouseClient.query({
            query: self.insertStatement,
            data: stream
        });
        self.lastLoadDate = Date.now();
        self.statRowsInFiles -= rowsInFiles;
        for (const path of paths) {
            await rm(path, { force: true });
        }
    }

    static getRowsInFiles(files: string[]) {
        let rowsInFiles = 0;
        for (const file of files) {
            const parts = file.split('_').filter(function (part) {
                return /^r\d+$/.test(part);
            });
            const rowsInFile = parseInt(parts[0].slice(1));
            rowsInFiles += rowsInFile;
        }
        return rowsInFiles;
    }

    static async prepareDirectoryPath(mainDirectoryPath: string, database: string, table: string, fsMode: number): Promise<string> {
        const directoryPath = path.join(mainDirectoryPath, database, table);
        await mkdir(directoryPath, { recursive: true, mode: fsMode });
        return directoryPath;
    }

    constructor(options: Options) {
        if (options.maxRowsInMemory) {
            this.maxRowsInMemory = options.maxRowsInMemory;
        }
        if (options.maxRowsPerFile) {
            this.maxRowsPerFile = options.maxRowsPerFile;
        }
        if (options.maxFilesPerLoad) {
            this.maxFilesPerLoad = options.maxFilesPerLoad;
        }
        if (options.fsMode) {
            this.fsMode = options.fsMode;
        }
        if (options.conditions) {
            this.conditions = options.conditions;
        }

        this.directoryPath = options.directoryPath;

        this.clickhouseClient = new ClickhouseClient(options.clickhouseClient);

        this.insertStatement = `INSERT INTO "${options.database ?? DEFAULT_DATABASE}"."${options.table}" FORMAT JSONCompactEachRow`;

        if (this.conditions.maxTime) {
            this.maxTimeTimer = setInterval(ClickhouseBuffer.maxTimeHandler, this.conditions.maxTime, this).unref();
        }
    }

    push(row: Array<columnType>): void {
        const rowValue = ClickhouseBuffer.fmtRow(row);
        this.rows.push(rowValue);
        if (this.rows.length >= this.maxRowsInMemory) {
            setImmediate(ClickhouseBuffer.flushToFiles, this, this.rows, !!this.conditions.maxRows);
            this.rows = [];
        }
    }

    async loadFilesToDatabase(): Promise<void> {
        const files = await readdir(this.directoryPath);
        if (files.length > 0) {
            await ClickhouseBuffer.loadToDatabase(this, files);
        }
    }

    release() {
        this.clickhouseClient.close().finally();
        if (this.conditions.maxTime) {
            clearInterval(this.maxTimeTimer);
        }
        setImmediate(ClickhouseBuffer.flushToFiles, this, this.rows);
    }

    filesInMemory(): number {
        return this.files.length;
    }

    rowsInMemory(): number {
        return this.rows.length;
    }

    rowsInFiles(): number {
        return this.statRowsInFiles;
    }
}
