import type { ColumnType } from "./types";
import { rm } from "fs/promises";

export function fmtRowJSON(row: Array<ColumnType>): string {
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

export function removeFiles(paths: string[]): Promise<void[]> {
    return Promise.all(paths.map(path => rm(path, { force: true })));
}
