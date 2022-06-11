import { ClickhouseBuffer } from '../src';

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