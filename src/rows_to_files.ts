import { hrtime } from "process";
import { Readable } from "stream";
import { writeFile } from "fs/promises";
import path from "path";

import { CompressionFormat, getEncoder } from "./encoder_decoder";

function calculateBytes(rows: string[]): number {
    return rows.reduce(function (bytes, row) {
        return bytes + Buffer.byteLength(row);
    }, 0);
}

export interface RowsToFilesOptions {
    directory: string;
    fsMode: number;
    maxRowsPerFile: number;
    compression?: CompressionFormat;
}

const DefaultRowsToFilesOptions: RowsToFilesOptions = {
    directory: './clickhouse_buffer',
    fsMode: 0o777,
    maxRowsPerFile: 1000
};

export async function rowsToFiles(rows: string[], options: RowsToFilesOptions = DefaultRowsToFilesOptions) {
    const rowsLength = rows.length;

    const sortKey = `${Date.now() / 1000 | 0}_${(hrtime.bigint() % 10_000_000_000n).toString(10)}`;
    const parts = Math.ceil(rowsLength / options.maxRowsPerFile);

    const files = [];

    for (let part = 0; part < parts; part++) {
        const numRowsToFile = rows.length >= options.maxRowsPerFile ? options.maxRowsPerFile : rows.length;
        const rowsToFile = rows.splice(0, numRowsToFile);
        const numBytesToFile = calculateBytes(rowsToFile);

        let dataToFile: string | Readable = rowsToFile.join('\n') + '\n';
        let filename = `${sortKey}_p${part}_r${numRowsToFile}_b${numBytesToFile}`;

        const encoder = getEncoder(options.compression);

        if (encoder) {
            encoder.end(dataToFile);
            dataToFile = encoder;
            filename += `.${options.compression}`;
        }

        await writeFile(path.join(options.directory, filename), dataToFile, { mode: options.fsMode });
        files.push(filename);
    }

    return {
        files,
        stats: {
            rows: rowsLength
        }
    };
}