import statisticsReplication from '../../replication/StatisticsReplication';

const NOW = 1790000000;
const STEP = 300;

function makeStats(rows: number, scale = 1, vsizesLength = 39, start = NOW): any[] {
  const stats: any[] = [];
  for (let i = 0; i < rows; i++) {
    stats.push({
      added: String(start - i * STEP),
      count: 1000 * scale,
      vbytes_per_second: 1000,
      total_fee: 0,
      mempool_byte_weight: 0,
      min_fee: 0.1,
      vsizes: new Array(vsizesLength).fill(100 * scale),
    });
  }
  return stats;
}

function withDeviatingRows(stats: any[], deviatingRows: number): any[] {
  const copy: any[] = [];
  for (let i = 0; i < stats.length; i++) {
    if (i < deviatingRows) {
      copy.push({ ...stats[i], count: stats[i].count * 0.5, vsizes: stats[i].vsizes.map((vsize: number) => vsize * 0.5) });
    } else {
      copy.push(stats[i]);
    }
  }
  return copy;
}

function check(results: any[][]): { serverPicked: string, sanitizedResult: any[] } {
  const resultsPerServer = Object.fromEntries(results.map((result, index) => [`server-${index}`, result]));
  return (statisticsReplication as any).performSanityCheck(resultsPerServer, '/api/v1/statistics/1w');
}

describe('StatisticsReplication sanity check', () => {
  test('picks a healthy result over one that deviates', () => {
    const bad = makeStats(100, 0.5);
    const { serverPicked, sanitizedResult } = check([makeStats(100), bad, makeStats(100), makeStats(100)]);
    expect(sanitizedResult.length).toBe(100);
    expect(sanitizedResult).not.toBe(bad);
    expect(serverPicked).not.toBe('server-1');
  });

  test('returns the server name of the picked result', () => {
    const results = [makeStats(100, 0.5), makeStats(100), makeStats(100), makeStats(100)];
    const { serverPicked, sanitizedResult } = check(results);
    expect(serverPicked).toMatch(/^server-[1-3]$/);
    expect(sanitizedResult).toBe(results[Number(serverPicked.split('-')[1])]);
  });

  test('matches rows by slot when one result has an extra newest row', () => {
    const { sanitizedResult } = check([makeStats(101, 1, 39, NOW + STEP), makeStats(100), makeStats(100), makeStats(100)]);
    expect(sanitizedResult.length).toBeGreaterThan(0);
  });

  test('rejects a sparse result', () => {
    const sparse = makeStats(10);
    const { serverPicked, sanitizedResult } = check([sparse, makeStats(100), makeStats(100), makeStats(100)]);
    expect(sanitizedResult).not.toBe(sparse);
    expect(serverPicked).not.toBe('server-0');
  });

  test('rejects a result with the old 38 band vsizes', () => {
    const old = makeStats(100, 1, 38);
    const { serverPicked, sanitizedResult } = check([old, makeStats(100), makeStats(100), makeStats(100)]);
    expect(sanitizedResult).not.toBe(old);
    expect(serverPicked).not.toBe('server-0');
  });

  test('returns an empty result when no results agree', () => {
    const { sanitizedResult } = check([makeStats(100, 1), makeStats(100, 2), makeStats(100, 4), makeStats(100, 8)]);
    expect(sanitizedResult).toEqual([]);
  });

  test('returns an empty result on a 2 vs 2 split', () => {
    const { serverPicked, sanitizedResult } = check([makeStats(100, 1), makeStats(100, 1), makeStats(100, 2), makeStats(100, 2)]);
    expect(sanitizedResult).toEqual([]);
    expect(serverPicked).toBe('');
  });

  test('returns an empty result when only one pair agrees', () => {
    const { sanitizedResult } = check([makeStats(100, 1), makeStats(100, 1), makeStats(100, 2), makeStats(100, 4)]);
    expect(sanitizedResult).toEqual([]);
  });

  test('counts a row deviating in both count and vsizes only once', () => {
    const base = makeStats(100);
    const { sanitizedResult } = check([withDeviatingRows(base, 6), base, makeStats(100, 4)]);
    expect(sanitizedResult.length).toBe(100);
  });

  test('rejects a pair with more than 10% deviating rows', () => {
    const base = makeStats(100);
    const { sanitizedResult } = check([withDeviatingRows(base, 11), base, makeStats(100, 4)]);
    expect(sanitizedResult).toEqual([]);
  });
});
