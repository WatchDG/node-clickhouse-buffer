# clickhouse-buffer

## Install

```shell
yarn add clickhouse-buffer
# or
npm install clickhouse-buffer
```

## How to use

```typescript
import {ClickhouseBuffer, DEFAULT_DATABASE} from "clickhouse-buffer";

(async ()=>{
    const database = DEFAULT_DATABASE;
    const table = 'events';

    const clickhouseBuffer = new ClickhouseBuffer({
        directoryPath: await ClickhouseBuffer.prepareDirectoryPath('buffer', database, table, 0o777),
        database,
        table
    });

    await clickhouseBuffer.clickhouseClient.query(`CREATE TABLE IF NOT EXISTS ${database}.${table} (id UInt16) ENGINE = MergeTree() ORDER BY id`);

    await clickhouseBuffer.loadFilesToDatabase();

    for (let i = 0; i <= 9; i++) {
        clickhouseBuffer.push([i]);
    }

    setTimeout(() => {
        for (let i = 10; i <= 19; i++) {
            clickhouseBuffer.push([i]);
        }
    }, 5000)

    setTimeout(() => {
        clickhouseBuffer.release();
    }, 15000);
})();
```