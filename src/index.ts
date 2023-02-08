import path from 'path';
import { mkdir, rm, readdir } from 'fs/promises';
import { ClickhouseClient, DEFAULT_DATABASE } from "@watchdg/clickhouse-client";

export { DEFAULT_DATABASE } from "@watchdg/clickhouse-client";
export { CompressionFormat } from './encoder_decoder';

import { filesToStream } from "./files_to_stream";
import { Mutex } from "./mutex";

import type { ClickhouseClientOptions } from "@watchdg/clickhouse-client";
import { CompressionFormat, getEncoder } from "./encoder_decoder";
import { rowsToFiles } from "./rows_to_files";

interface Conditions {
    maxTime?: number;
    maxRows?: number;
}

export interface FieldSettings {
    name: string;
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
    fields?: FieldSettings[];
    compressed?: CompressionFormat;
    compressedFiles?: CompressionFormat;
}

type columnType = string | number | Date | boolean;

function removeFiles(paths: string[]): Promise<void[]> {
    return Promise.all(paths.map(path => rm(path, { force: true })));
}

export class ClickhouseBuffer {
    private readonly directoryPath: string;
    private readonly fsMode: number = 0o777;
    private readonly database: string;
    private readonly table: string;
    private readonly fields?: FieldSettings[];
    private readonly compressed?: CompressionFormat;
    private readonly compressedFiles?: CompressionFormat;

    private readonly maxRowsPerFile: number = 1000;
    private readonly maxRowsInMemory: number = 1000;
    private readonly maxFilesPerLoad: number = 100;

    private rows: string[] = [];
    private files: string[] = [];

    private readonly conditions: Conditions = { maxTime: 5000 };

    private readonly loadToDatabaseMutex: Mutex = new Mutex();

    private readonly maxTimeTimer?: NodeJS.Timer;
    private lastLoadDate: number = Date.now();

    private statsRowsInFiles = 0;

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

    private static maxTimeHandler(self: ClickhouseBuffer): void {
        const rows = self.resetRows();
        if (rows.length > 0) {
            setImmediate(ClickhouseBuffer.flushToFiles, self, rows, true);
        }
    }

    private static isConditionMet(self: ClickhouseBuffer): boolean {
        return (self.conditions?.maxTime && (Date.now() - self.lastLoadDate) >= self.conditions.maxTime) ||
            (self.conditions?.maxRows && self.statsRowsInFiles >= self.conditions.maxRows);
    }

    private static async flushToFiles(self: ClickhouseBuffer, rows: string[], checkConditions = false): Promise<void> {

        const { files, stats } = await rowsToFiles(rows, {
            directory: self.directoryPath,
            fsMode: self.fsMode,
            maxRowsPerFile: self.maxRowsPerFile,
            compression: self.compressedFiles
        });

        self.files.push(...files);
        self.statsRowsInFiles += stats.rows;

        if (checkConditions && ClickhouseBuffer.isConditionMet(self)) {
            const files = self.resetFiles();
            if (files.length > 0) {
                setImmediate(ClickhouseBuffer.loadToDatabase, self, files);
            }
        }
    }

    private static async loadToDatabase(self: ClickhouseBuffer, files: string[]) {
        self.lastLoadDate = Date.now();

        await self.loadToDatabaseMutex.acquire()
            .then(async function () {
                const paths = files.map(function (filename) {
                    return path.join(self.directoryPath, filename);
                });

                let stream = filesToStream(Array.from(paths));

                const encoder = getEncoder(self.compressed);

                if (encoder) {
                    stream = stream.pipe(encoder);
                }

                await self.clickhouseClient.query({
                    query: self.insertStatement,
                    data: stream,
                    compressed: self.compressed
                });

                await removeFiles(paths);
            })
            .finally(function () {
                self.loadToDatabaseMutex.release();
            });
    }

    static getRowsInFiles(files: string[]) {
        let rowsInFiles = 0;
        for (const file of files) {
            const parts = file.split('.')[0].split('_').filter(function (part) {
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

        if (options.fields) {
            this.fields = options.fields;
        }
        if (options.compressed) {
            this.compressed = options.compressed;
        }
        if (options.compressedFiles) {
            this.compressedFiles = options.compressedFiles;
        }

        this.database = options.database ?? DEFAULT_DATABASE;
        this.table = options.table;

        const columns = this.fields ? ' (' + this.fields.map(function (fieldSettings) {
            return `"${fieldSettings.name}"`;
        }).join(',') + ')' : '';
        this.insertStatement = `INSERT INTO "${this.database}"."${this.table}"${columns} FORMAT JSONCompactEachRow`;

        if (this.conditions.maxTime) {
            this.maxTimeTimer = setInterval(ClickhouseBuffer.maxTimeHandler, this.conditions.maxTime, this).unref();
        }
    }

    private resetRows(): any[] {
        const rows = this.rows;
        this.rows = [];
        return rows;
    }

    private resetFiles(): string[] {
        const files = this.files;
        this.files = [];
        this.statsRowsInFiles = 0;
        return files;
    }

    push(row: Array<columnType>): void {
        const rowValue = ClickhouseBuffer.fmtRow(row);
        this.rows.push(rowValue);
        if (this.rows.length >= this.maxRowsInMemory) {
            const rows = this.resetRows();
            setImmediate(ClickhouseBuffer.flushToFiles, this, rows, !!this.conditions.maxRows);
        }
    }

    async loadFilesToDatabase(): Promise<void> {
        const files = await readdir(this.directoryPath);
        if (files.length > 0) {
            await ClickhouseBuffer.loadToDatabase(this, files);
        }
    }

    async release() {
        if (this.conditions.maxTime) {
            clearInterval(this.maxTimeTimer);
        }
        const rows = this.resetRows();
        await ClickhouseBuffer.flushToFiles(this, rows, false);
        await this.clickhouseClient.close();
    }

    filesInMemory(): number {
        return this.files.length;
    }

    rowsInMemory(): number {
        return this.rows.length;
    }

    rowsInFiles(): number {
        return this.statsRowsInFiles;
    }
}