import { DEFAULT_RETRIES_OPTIONS, withRetries } from "../lib/retries";

describe('retries', function () {
    it('with default options', async function () {
        let x = 0;
        const limit = 5;

        async function testFunction() {
            if (x < limit) {
                x++;
                throw x;
            }
            return x;
        }

        await expect(withRetries(DEFAULT_RETRIES_OPTIONS, testFunction, null)).rejects.toEqual(1);
    });

    it('with custom options', async function () {
        let x = 0;
        const limit = 5;

        async function testFunction() {
            if (x < limit) {
                x++;
                throw x;
            }
            return x;
        }

        await expect(withRetries({
            initTime: 100,
            maxAttempts: 2,
        }, testFunction, null))
            .rejects.toEqual(2);
    });
});