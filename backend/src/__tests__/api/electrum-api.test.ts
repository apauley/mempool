import Client from '@mempool/electrum-client';
import ElectrumApi from '../../api/bitcoin/electrum-api';
import { IElectrumApi } from '../../api/bitcoin/electrum-api.interface';
import { IEsploraApi } from '../../api/bitcoin/esplora-api.interface';

jest.mock('@mempool/electrum-client', () => jest.fn());
jest.mock('../../api/blocks', () => ({}));
jest.mock('../../api/transaction-utils', () => ({}));
jest.mock('../../indexer', () => ({}));
jest.mock('../../api/memory-cache', () => ({
  get: jest.fn(),
  set: jest.fn(),
}));

describe.each(['address', 'scripthash'])('Electrum %s transaction pagination', (kind) => {
  let api: ElectrumApi;
  let history: IElectrumApi.ScriptHashHistory[];

  beforeEach(() => {
    history = [
      { tx_hash: 'oldest', height: 100 },
      { tx_hash: 'newest', height: 102 },
      { tx_hash: 'middle', height: 101 },
    ];
    (Client as jest.Mock).mockImplementation(() => ({
      initElectrum: jest.fn().mockResolvedValue(undefined),
      blockchainScripthash_getHistory: jest.fn().mockImplementation(async () => [...history]),
    }));
    api = new ElectrumApi({
      validateAddress: jest.fn().mockResolvedValue({ isvalid: true, scriptPubKey: '00' }),
    });
    jest.spyOn(api, '$getRawTransaction').mockImplementation(async (txid): Promise<IEsploraApi.Transaction> => ({
      txid,
      version: 2,
      locktime: 0,
      size: 100,
      weight: 400,
      fee: 0,
      vin: [],
      vout: [],
      status: { confirmed: true },
    }));
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  async function fetchTxids(cursor = ''): Promise<string[]> {
    const transactions = kind === 'address'
      ? await api.$getAddressTransactions('address', cursor)
      : await api.$getScriptHashTransactions('scripthash', cursor);
    return transactions.map((transaction) => transaction.txid);
  }

  test('excludes a cursor at index zero', async () => {
    expect(await fetchTxids('newest')).toEqual(['middle', 'oldest']);
  });

  test('returns an empty page after the only transaction', async () => {
    history = [{ tx_hash: 'only', height: 100 }];
    expect(await fetchTxids('only')).toEqual([]);
  });

  test('excludes a cursor at a positive index', async () => {
    expect(await fetchTxids('middle')).toEqual(['oldest']);
  });

  test('returns the first page without a cursor', async () => {
    expect(await fetchTxids()).toEqual(['newest', 'middle', 'oldest']);
  });

  test('returns the first page for an unknown cursor', async () => {
    expect(await fetchTxids('unknown')).toEqual(['newest', 'middle', 'oldest']);
  });
});
