'use client';

import { formatVolume, formatPrice, parseOutcomes, parseOutcomePrices } from '@/lib/validations';
import { formatTimeEST } from '@/lib/utils';

interface Market {
  ID?: string | null;
  QUESTION?: string | null;
  CATEGORY?: string | null;
  OUTCOME_PRICES?: string | null;
  OUTCOMES?: string | null;
  VOLUME_NUM?: number | null;
  VOLUME_24HR?: number | null;
  LIQUIDITY_NUM?: number | null;
  SPREAD?: number | null;
  ACTIVE?: boolean | null;
  CLOSED?: boolean | null;
  END_DATE?: string | null;
  IMAGE?: string | null;
  INGESTED_AT?: string | null;
}

function PriceBar({ label, price }: { label: string; price: number }) {
  const pct = Math.min(Math.max(price * 100, 0), 100);
  const isYes = label.toLowerCase().includes('yes');

  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-8 text-right font-medium text-gray-300">{label}</span>
      <div className="flex-1 h-5 bg-gray-800 rounded overflow-hidden">
        <div
          className={`h-full rounded transition-all duration-500 ${
            isYes ? 'bg-emerald-500' : 'bg-rose-500'
          }`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="w-14 text-right font-mono text-gray-200">{formatPrice(price)}</span>
    </div>
  );
}

function CategoryBadge({ category }: { category: string | null | undefined }) {
  const colors: Record<string, string> = {
    'Politics': 'bg-blue-900/50 text-blue-300 border-blue-700',
    'Sports': 'bg-green-900/50 text-green-300 border-green-700',
    'Crypto': 'bg-yellow-900/50 text-yellow-300 border-yellow-700',
    'Science': 'bg-purple-900/50 text-purple-300 border-purple-700',
    'Pop Culture': 'bg-pink-900/50 text-pink-300 border-pink-700',
    'Business': 'bg-cyan-900/50 text-cyan-300 border-cyan-700',
  };

  const cat = category || 'Other';
  const colorClass = colors[cat] || 'bg-gray-800/50 text-gray-300 border-gray-600';

  return (
    <span className={`px-2 py-0.5 text-[10px] font-medium rounded border ${colorClass}`}>
      {cat}
    </span>
  );
}

export function MarketCard({ market }: { market: Market }) {
  const outcomes = parseOutcomes(market.OUTCOMES);
  const prices = parseOutcomePrices(market.OUTCOME_PRICES);

  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg p-4 hover:border-cyan-600/50 transition-colors">
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-medium text-gray-100 leading-tight line-clamp-2">
            {market.QUESTION || 'Untitled Market'}
          </h3>
          <div className="flex items-center gap-2 mt-1.5">
            <CategoryBadge category={market.CATEGORY} />
            {market.END_DATE && (
              <span className="text-[10px] text-gray-500">
                Ends {new Date(market.END_DATE).toLocaleDateString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', year: 'numeric' })} {formatTimeEST(market.END_DATE)}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Price bars */}
      <div className="space-y-1.5 mb-3">
        {outcomes.slice(0, 4).map((outcome, i) => (
          <PriceBar key={i} label={outcome} price={prices[i] || 0} />
        ))}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-2 pt-2 border-t border-gray-800">
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Volume</div>
          <div className="text-xs font-mono text-gray-200">{formatVolume(market.VOLUME_NUM)}</div>
        </div>
        <div>
          <div className="text-[10px] text-gray-500 uppercase">24h Vol</div>
          <div className="text-xs font-mono text-gray-200">{formatVolume(market.VOLUME_24HR)}</div>
        </div>
        <div>
          <div className="text-[10px] text-gray-500 uppercase">Liquidity</div>
          <div className="text-xs font-mono text-gray-200">{formatVolume(market.LIQUIDITY_NUM)}</div>
        </div>
      </div>
    </div>
  );
}

export function MarketTable({ markets, onExport }: { markets: Market[]; onExport?: () => void }) {
  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <h2 className="text-sm font-medium text-gray-200">Markets ({markets.length})</h2>
        {onExport && (
          <button
            onClick={onExport}
            className="px-3 py-1 text-xs bg-gray-800 text-gray-300 rounded hover:bg-gray-700 transition-colors"
          >
            Export CSV
          </button>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 uppercase">
              <th className="text-left px-4 py-2 font-medium">Market</th>
              <th className="text-left px-3 py-2 font-medium">Category</th>
              <th className="text-right px-3 py-2 font-medium">Yes</th>
              <th className="text-right px-3 py-2 font-medium">Volume</th>
              <th className="text-right px-3 py-2 font-medium">24h Vol</th>
              <th className="text-right px-3 py-2 font-medium">Liquidity</th>
            </tr>
          </thead>
          <tbody>
            {markets.map((m, i) => {
              const prices = parseOutcomePrices(m.OUTCOME_PRICES);
              return (
                <tr key={m.ID || i} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                  <td className="px-4 py-2.5 max-w-xs truncate text-gray-200">
                    {m.QUESTION || 'N/A'}
                  </td>
                  <td className="px-3 py-2.5">
                    <CategoryBadge category={m.CATEGORY} />
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-emerald-400">
                    {formatPrice(prices[0])}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-gray-300">
                    {formatVolume(m.VOLUME_NUM)}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-gray-300">
                    {formatVolume(m.VOLUME_24HR)}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-gray-300">
                    {formatVolume(m.LIQUIDITY_NUM)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
